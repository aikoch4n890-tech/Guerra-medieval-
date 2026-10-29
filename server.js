// =====================================================================
//  Guerra dos Reinos - servidor da live
//  Lê comentários, curtidas e presentes da sua live do TikTok
//  e manda tudo para o jogo em tempo real.
//
//  Variáveis (opcionais) no Render:
//    TIKTOK_USER   = seu @ do TikTok (sem o @). Se não colocar, o jogo pergunta.
//    SIGN_API_KEY  = chave grátis da eulerstream.com (ajuda se a conexão falhar muito)
// =====================================================================
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const GAME = path.join(__dirname, 'index.html');

// ---------------------------------------------------------------------
//  WebSocket mínimo (servidor -> jogo), sem dependências extras
// ---------------------------------------------------------------------
function wsAccept(req, socket) {
  const key = req.headers['sec-websocket-key'];
  if (!key) return socket.destroy();
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  return socket;
}
function wsFrame(str, opcode = 1) {
  const data = Buffer.from(str);
  const len = data.length;
  let head;
  if (len < 126) head = Buffer.from([0x80 | opcode, len]);
  else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | opcode; head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[0] = 0x80 | opcode; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
  return Buffer.concat([head, data]);
}

// ---------------------------------------------------------------------
//  Salas: uma conexão com o TikTok por @, compartilhada por todos os jogos abertos
// ---------------------------------------------------------------------
const rooms = new Map();

function cleanUser(u) {
  return String(u || '').trim().replace(/^https?:\/\/(www\.)?tiktok\.com\//i, '').replace(/^@/, '').replace(/\/live.*$/i, '').replace(/[^\w.]/g, '').slice(0, 40);
}

function getRoom(user) {
  let room = rooms.get(user);
  if (!room) {
    room = { user, clients: new Set(), conn: null, status: 'connecting', detail: '', retry: null, closing: false, giftLogged: false };
    rooms.set(user, room);
    connectTikTok(room);
  }
  return room;
}

function send(room, obj) {
  const frame = wsFrame(JSON.stringify(obj));
  for (const s of room.clients) { try { s.write(frame); } catch (e) {} }
}
function setStatus(room, status, detail) {
  room.status = status; room.detail = detail || '';
  console.log(`[@${room.user}] ${status}${detail ? ' - ' + detail : ''}`);
  send(room, { type: 'status', status, detail: room.detail, user: room.user });
}

// --- leitura defensiva dos campos (mudam entre versões da biblioteca) ---
const nameOf = d => (d.user && (d.user.nickname || d.user.uniqueId)) || d.nickname || d.uniqueId || 'anônimo';
const idOf   = d => String((d.user && (d.user.uniqueId || d.user.userId)) || d.uniqueId || d.userId || nameOf(d));
function coinsOf(d) {
  const v = d.diamondCount ?? (d.giftDetails && d.giftDetails.diamondCount) ?? (d.gift && (d.gift.diamondCount ?? d.gift.diamond_count))
    ?? (d.extendedGiftInfo && d.extendedGiftInfo.diamond_count) ?? (d.giftInfo && d.giftInfo.diamondCount);
  return Number(v) || 1;
}
function giftTypeOf(d) {
  return d.giftType ?? (d.giftDetails && d.giftDetails.giftType) ?? (d.gift && (d.gift.type ?? d.gift.giftType)) ?? (d.extendedGiftInfo && d.extendedGiftInfo.type);
}

async function connectTikTok(room) {
  if (room.closing) return;
  let lib;
  try { lib = await import('tiktok-live-connector'); }
  catch (e) { setStatus(room, 'error', 'Biblioteca tiktok-live-connector não instalada'); return; }
  const Conn = lib.TikTokLiveConnection || lib.WebcastPushConnection || (lib.default && (lib.default.TikTokLiveConnection || lib.default.WebcastPushConnection));
  const opts = { enableExtendedGiftInfo: true, processInitialData: false };
  if (process.env.SIGN_API_KEY) {
    opts.signApiKey = process.env.SIGN_API_KEY;
    if (lib.SignConfig) lib.SignConfig.apiKey = process.env.SIGN_API_KEY;
  }
  const conn = new Conn(room.user, opts);
  room.conn = conn;
  setStatus(room, 'connecting');

  // comentários -> escolher time
  conn.on('chat', d => send(room, { type: 'comment', user: nameOf(d), id: idOf(d), text: d.comment || '' }));

  // curtidas
  conn.on('like', d => send(room, { type: 'like', user: nameOf(d), id: idOf(d), count: Number(d.likeCount ?? d.count ?? 1) || 1 }));

  // presentes (sequências de combo só contam quando terminam)
  conn.on('gift', d => {
    if (!room.giftLogged) { room.giftLogged = true; console.log('[debug] primeiro presente:', JSON.stringify(d).slice(0, 800)); }
    const streakable = Number(giftTypeOf(d)) === 1;
    if (streakable && !d.repeatEnd) return;
    const count = Math.max(1, Number(d.repeatCount || 1));
    send(room, { type: 'gift', user: nameOf(d), id: idOf(d), coins: coinsOf(d), count,
      gift: (d.giftDetails && d.giftDetails.giftName) || (d.gift && d.gift.name) || (d.extendedGiftInfo && d.extendedGiftInfo.name) || '' });
  });

  conn.on('streamEnd', () => { setStatus(room, 'offline', 'A live terminou'); scheduleRetry(room, 30); });
  conn.on('disconnected', () => { if (room.status === 'live') { setStatus(room, 'reconnecting'); scheduleRetry(room, 5); } });
  conn.on('error', e => console.log(`[@${room.user}] erro:`, e && (e.message || e)));

  try {
    const state = await conn.connect();
    setStatus(room, 'live', 'Sala ' + (state && state.roomId || ''));
  } catch (e) {
    const msg = String((e && (e.message || e.exception || e)) || 'erro');
    const offline = /offline|not.*live|UserOffline|isn't online|LIVE has ended/i.test(msg + (e && e.name));
    setStatus(room, offline ? 'offline' : 'error', offline ? 'Você ainda não está ao vivo' : msg.slice(0, 160));
    scheduleRetry(room, offline ? 20 : 30);
  }
}

function scheduleRetry(room, sec) {
  clearTimeout(room.retry);
  if (room.closing) return;
  room.retry = setTimeout(() => {
    try { room.conn && room.conn.disconnect && room.conn.disconnect(); } catch (e) {}
    connectTikTok(room);
  }, sec * 1000);
}

function dropClient(room, socket) {
  room.clients.delete(socket);
  if (room.clients.size === 0) {
    // ninguém mais com o jogo aberto: desliga depois de 1 minuto
    setTimeout(() => {
      if (room.clients.size) return;
      room.closing = true; clearTimeout(room.retry);
      try { room.conn && room.conn.disconnect && room.conn.disconnect(); } catch (e) {}
      rooms.delete(room.user);
      console.log(`[@${room.user}] desligado (jogo fechado)`);
    }, 60000);
  }
}

// ---------------------------------------------------------------------
//  HTTP: serve o jogo
// ---------------------------------------------------------------------
const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname === '/health') { res.writeHead(200); return res.end('ok'); }
  if (url.pathname === '/' || url.pathname === '/index.html') {
    fs.readFile(GAME, (err, buf) => {
      if (err) { res.writeHead(500); return res.end('index.html não encontrado'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(buf);
    });
    return;
  }
  res.writeHead(404); res.end('não encontrado');
});

server.on('upgrade', (req, socket) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname !== '/ws') return socket.destroy();
  const user = cleanUser(process.env.TIKTOK_USER || url.searchParams.get('user'));
  if (!wsAccept(req, socket)) return;
  if (!user) { socket.write(wsFrame(JSON.stringify({ type: 'status', status: 'nouser' }))); return; }
  const room = getRoom(user);
  room.closing = false;
  room.clients.add(socket);
  socket.write(wsFrame(JSON.stringify({ type: 'status', status: room.status, detail: room.detail, user })));
  const ping = setInterval(() => { try { socket.write(wsFrame('', 9)); } catch (e) {} }, 25000);
  socket.on('data', buf => { if ((buf[0] & 0x0f) === 8) socket.end(); });
  const bye = () => { clearInterval(ping); dropClient(room, socket); };
  socket.on('close', bye); socket.on('error', bye);
});

server.listen(PORT, () => console.log('Guerra dos Reinos rodando na porta ' + PORT));
