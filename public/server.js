// F1 World Tour - online server: serves the game, runs 2-6 player lobbies over WebSocket,
// and keeps the shared best-lap ghost for each track. Deploy on Render (see README.md).
const http = require('http'), fs = require('fs'), path = require('path');
let WebSocketServer;
try { ({ WebSocketServer } = require('ws')); }
catch (e) {
  console.error('ERROR: the "ws" package is not installed. package.json must sit in the SAME folder as server.js,');
  console.error('and the build command must be "npm install". See README.md.');
  process.exit(1);
}

const PORT = process.env.PORT || 3000;
const PUB = path.join(__dirname, 'public');
const DATA = path.join(__dirname, 'ghosts.json');
const MAX_PLAYERS = 6;
const TRACKS = ['monza', 'spa', 'monaco', 'silverstone', 'interlagos', 'bahrain', 'singapore', 'austin', 'abudhabi', 'redbull'];
const COMPOUNDS = ['S', 'M', 'H', 'I', 'W'];
const MIME = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript', '.json': 'application/json', '.png': 'image/png', '.ico': 'image/x-icon' };

let best = {};
try { best = JSON.parse(fs.readFileSync(DATA, 'utf8')); } catch (e) { best = {}; }

const num = (v, lo, hi) => { v = Number(v); return Number.isFinite(v) ? Math.max(lo, Math.min(hi, v)) : 0; };
const hex = (v, d) => (/^#[0-9a-f]{6}$/i.test(String(v)) ? String(v) : d);

// ---------- HTTP: static game + shared ghost API ----------
const srv = http.createServer((req, res) => {
  const u = new URL(req.url, 'http://localhost');
  if (u.pathname === '/health') { res.end('ok'); return; }
  if (u.pathname.startsWith('/api/best/')) {
    const tr = u.pathname.slice(10);
    if (!TRACKS.includes(tr)) { res.statusCode = 404; res.end('no such track'); return; }
    if (req.method === 'GET') { res.setHeader('content-type', 'application/json'); res.end(JSON.stringify(best[tr] || null)); return; }
    if (req.method === 'PUT') {
      let body = '';
      req.on('data', c => { body += c; if (body.length > 300000) req.destroy(); });
      req.on('end', () => {
        try {
          const j = JSON.parse(body);
          const ok = j && Number.isFinite(j.t) && j.t > 300 && j.t < 20000 && Array.isArray(j.d) && j.d.length === j.t;
          if (ok && (!best[tr] || j.t < best[tr].t)) {
            best[tr] = { n: String(j.n || '?').slice(0, 12), t: j.t, d: j.d.map(p => [num(p[0], 0, 3e6) | 0, +num(p[1], -3, 3).toFixed(2)]) };
            fs.writeFile(DATA, JSON.stringify(best), () => {});
          }
          res.end('ok');
        } catch (e) { res.statusCode = 400; res.end('bad request'); }
      });
      return;
    }
    res.statusCode = 405; res.end('method not allowed'); return;
  }
  let p = decodeURIComponent(u.pathname);
  let f = path.join(PUB, p === '/' ? 'index.html' : p);
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) f = path.join(PUB, 'index.html');
  res.setHeader('content-type', MIME[path.extname(f)] || 'application/octet-stream');
  const st = fs.createReadStream(f);
  st.on('error', () => {
    res.statusCode = 500;
    res.setHeader('content-type', 'text/plain');
    res.end('The game file is missing. The server needs public/index.html (a folder named "public" containing a file named index.html). See README.md.');
  });
  st.pipe(res);
});

// ---------- WebSocket: lobbies and live race relay ----------
const wss = new WebSocketServer({ server: srv, maxPayload: 8192 });
const rooms = new Map();
let nextId = 1;

const send = (ws, m) => { if (ws.readyState === 1) ws.send(JSON.stringify(m)); };
const bc = (r, m, except) => r.ps.forEach(p => { if (p.ws !== except) send(p.ws, m); });
const roomInfo = r => ({
  t: 'room', code: r.code, host: r.host, cfg: r.cfg, started: r.started,
  ps: r.ps.map(p => ({ id: p.id, n: p.n, c: p.c, sec: p.sec, num: p.num, pat: p.pat }))
});

function leave(me) {
  const r = me.room;
  if (!r) return;
  r.ps = r.ps.filter(p => p !== me);
  me.room = null;
  if (!r.ps.length) { rooms.delete(r.code); return; }
  if (r.host === me.id) r.host = r.ps[0].id;
  bc(r, { t: 'gone', id: me.id });
  bc(r, roomInfo(r));
}

wss.on('connection', ws => {
  const me = { id: nextId++, ws, n: 'DRIVER', c: '#00f2ff', sec: '#ffffff', num: 1, pat: 0, room: null, tick: Date.now(), count: 0 };
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    const now = Date.now();
    if (now - me.tick > 1000) { me.tick = now; me.count = 0; }
    if (++me.count > 60) return;                       // max 60 messages per second per player
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;

    if (m.t === 'hello') {
      me.n = String(m.n || 'DRIVER').slice(0, 12);
      me.c = hex(m.c, me.c); me.sec = hex(m.sec, me.sec);
      me.num = Math.max(1, Math.min(99, m.num | 0 || 1)); me.pat = (m.pat | 0) & 3;
      send(ws, { t: 'hi', id: me.id });
      return;
    }
    if (m.t === 'create' || m.t === 'join') {
      leave(me);
      let r;
      if (m.t === 'create') {
        let code;
        do { code = Array.from({ length: 4 }, () => 'ABCDEFGHJKLMNPQRSTUVWXYZ'[Math.random() * 24 | 0]).join(''); } while (rooms.has(code));
        r = { code, host: me.id, ps: [], cfg: { track: 'monza', laps: 3, wx: 1 }, started: false };
        rooms.set(code, r);
      } else {
        r = rooms.get(String(m.code || '').toUpperCase());
        if (!r) { send(ws, { t: 'err', m: 'ROOM NOT FOUND' }); return; }
        if (r.ps.length >= MAX_PLAYERS) { send(ws, { t: 'err', m: 'ROOM IS FULL (6 PLAYERS MAX)' }); return; }
        if (r.started) { send(ws, { t: 'err', m: 'THAT RACE HAS ALREADY STARTED' }); return; }
      }
      r.ps.push(me); me.room = r;
      bc(r, roomInfo(r));
      return;
    }
    const r = me.room;
    if (!r) return;
    if (m.t === 'cfg' && r.host === me.id && !r.started) {
      r.cfg = {
        track: TRACKS.includes(m.track) ? m.track : 'monza',
        laps: [1, 3, 5, 10].includes(m.laps | 0) ? m.laps | 0 : 3,
        wx: m.wx === 2 ? 2 : 1
      };
      bc(r, roomInfo(r));
      return;
    }
    if (m.t === 'start' && r.host === me.id && !r.started) {
      r.started = true;
      bc(r, { t: 'start', cfg: r.cfg, order: r.ps.map(p => p.id), seed: Math.random() });
      return;
    }
    if (m.t === 's' && r.started) {
      bc(r, {
        t: 's', id: me.id, pos: num(m.pos, 0, 3e6) | 0, x: +num(m.x, -3, 3).toFixed(2), sp: num(m.sp, 0, 500) | 0,
        lap: num(m.lap, 0, 50) | 0, cr: m.cr ? 1 : 0, fin: num(m.fin, 0, 1e7) | 0,
        comp: COMPOUNDS.includes(m.comp) ? m.comp : 'M', wing: num(m.wing, 0, 100) | 0
      }, ws);
      return;
    }
    if (m.t === 'end' && r.host === me.id) { r.started = false; bc(r, roomInfo(r)); return; }
    if (m.t === 'leave') leave(me);
  });
  ws.on('close', () => leave(me));
});

setInterval(() => wss.clients.forEach(ws => {
  if (ws.isAlive === false) return ws.terminate();
  ws.isAlive = false; ws.ping();
}), 30000);

if (require.main === module) {
  if (!fs.existsSync(path.join(PUB, 'index.html'))) {
    console.error('WARNING: ' + path.join(PUB, 'index.html') + ' was not found. Put the game at public/index.html (folder "public", file "index.html").');
  }
  srv.listen(PORT, () => console.log('F1 World Tour online server listening on port ' + PORT));
}
module.exports = { srv, wss, rooms };
