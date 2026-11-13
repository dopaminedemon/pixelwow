'use strict';
/*
 * Whisperwood multiplayer server  -  no dependencies, Node 18+.
 *   node server.js
 * Env:  PORT (3000)  DATA_DIR (./data)  MAX_PLAYERS (40)  INVITE_CODE (optional, required to register)
 *
 * What the server owns:  accounts + login, saved characters, the monsters (positions, HP, AI, respawn),
 * monster claiming (first hit), party XP/kill credit, shared resource nodes, world clock, chat, parties.
 * What it still trusts the client for:  the player's own stats, damage numbers (sanity-capped), inventory, gold.
 */
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto'), zlib = require('zlib');
const PROTO = 17;                       // must match NEED_SV in game.html
const PORT = +process.env.PORT || 3000;
const DATA = process.env.DATA_DIR || path.join(__dirname, 'data');
const MAXP = +process.env.MAX_PLAYERS || 40;
const ADMIN_CODE = String(process.env.ADMIN_CODE || '1');   // typed in the game console as: admin <code>  (set ADMIN_CODE to something secret on a public server)
const INVITE = process.env.INVITE_CODE || '';
const WORLD = JSON.parse(fs.readFileSync(path.join(__dirname, 'world.json'), 'utf8'));
if ((WORLD.proto | 0) !== PROTO) console.error('\n!!! world.json is OUT OF DATE (found version ' + (WORLD.proto | 0) + ', need ' + PROTO + '). Elite enemies and the Ironcrest area will be missing. Replace world.json with the one from the new zip and restart.\n');
const HTML_PATH = path.join(__dirname, 'game.html');

// ---------------------------------------------------------------- persistence
fs.mkdirSync(DATA, { recursive: true });
const ACC_FILE = path.join(DATA, 'accounts.json');
let accounts = { users: {} };
try { accounts = JSON.parse(fs.readFileSync(ACC_FILE, 'utf8')); if (!accounts.users) accounts.users = {}; } catch (e) { }
const nameIndex = {}; // character name (lower) -> username (lower)
for (const u in accounts.users) (accounts.users[u].chars || []).forEach(c => { if (c) nameIndex[c.name.toLowerCase()] = u });
let dirty = false;
for (const u in accounts.users) (accounts.users[u].chars || []).forEach(c => {
  if (c && c.cls === 'assassin') { c.cls = 'soulweaver'; c.known = [0]; c.sp = Math.floor((c.lvl | 0) / 3); dirty = true; console.log('converted Assassin ' + c.name + ' to Soulweaver') }
  if (c && c.cls === 'mime') { c.cls = 'akatsuki'; c.known = [0]; c.sp = Math.floor((c.lvl | 0) / 3); dirty = true; console.log('converted Mime ' + c.name + ' to Akatsuki') }
});
function flush() {
  if (!dirty) return;
  dirty = false;
  const tmp = ACC_FILE + '.tmp';
  try {
    fs.writeFileSync(tmp, JSON.stringify(accounts));
    fs.renameSync(tmp, ACC_FILE);
    // rolling backup once an hour
    const b = path.join(DATA, 'backup-' + new Date().toISOString().slice(0, 13) + '.json');
    if (!fs.existsSync(b)) { fs.copyFileSync(ACC_FILE, b); pruneBackups() }
  } catch (e) { console.error('save failed', e.message); dirty = true }
}
function pruneBackups() {
  try {
    const f = fs.readdirSync(DATA).filter(x => x.startsWith('backup-')).sort();
    while (f.length > 48) fs.unlinkSync(path.join(DATA, f.shift()));
  } catch (e) { }
}
setInterval(flush, 8000);
function shutdown() { console.log('saving...'); dirty = true; flush(); process.exit(0) }
process.on('SIGINT', shutdown); process.on('SIGTERM', shutdown);

// ---------------------------------------------------------------- minimal websocket
const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
class Conn {
  constructor(sock) {
    this.sock = sock; this.buf = Buffer.alloc(0); this.open = true; this.frag = null; this.onmsg = null; this.onclose = null;
    this.bucket = 300; this.bt = Date.now();
    sock.setNoDelay(true);
    sock.on('data', d => this.data(d));
    sock.on('close', () => this.end());
    sock.on('error', () => this.end());
  }
  data(d) {
    this.buf = Buffer.concat([this.buf, d]);
    if (this.buf.length > 3e6) return this.close();
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const fin = b[0] & 128, op = b[0] & 15, masked = b[1] & 128; let len = b[1] & 127, o = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); o = 4 }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); o = 10 }
      if (len > 1.5e6) return this.close();
      if (!masked) return this.close();
      if (b.length < o + 4 + len) return;
      const mk = b.slice(o, o + 4), pl = Buffer.from(b.slice(o + 4, o + 4 + len));
      for (let i = 0; i < len; i++) pl[i] ^= mk[i & 3];
      this.buf = b.slice(o + 4 + len);
      if (op === 8) return this.close();
      if (op === 9) { this.frame(10, pl); continue }
      if (op === 10) continue;
      if (op === 1 || op === 0) {
        if (op === 1) this.frag = [];
        if (!this.frag) return this.close();
        this.frag.push(pl);
        if (fin) { const s = Buffer.concat(this.frag).toString('utf8'); this.frag = null; this.message(s) }
      }
    }
  }
  message(s) {
    const now = Date.now(); this.bucket = Math.min(300, this.bucket + (now - this.bt) * .25); this.bt = now;
    if (--this.bucket < 0) return; // flood protection
    let m; try { m = JSON.parse(s) } catch (e) { return }
    if (m && typeof m === 'object' && this.onmsg) { try { this.onmsg(m) } catch (e) { console.error('handler error', e) } }
  }
  frame(op, pl) {
    if (!this.open) return;
    const n = pl.length; let h;
    if (n < 126) h = Buffer.from([128 | op, n]);
    else if (n < 65536) { h = Buffer.alloc(4); h[0] = 128 | op; h[1] = 126; h.writeUInt16BE(n, 2) }
    else { h = Buffer.alloc(10); h[0] = 128 | op; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2) }
    this.sock.write(Buffer.concat([h, pl]));
  }
  send(o) { if (this.open && this.sock.writableLength < 400000) this.frame(1, Buffer.from(typeof o === 'string' ? o : JSON.stringify(o))) }
  close() { if (this.open) { try { this.frame(8, Buffer.alloc(0)); this.sock.end() } catch (e) { } } this.end() }
  end() { if (!this.open) return; this.open = false; try { this.sock.destroy() } catch (e) { } if (this.onclose) this.onclose() }
}

// ---------------------------------------------------------------- rules shared with the client
const RACE_CLASSES = {
  human: ['warrior', 'mage', 'hunter', 'paladin', 'necro', 'soulweaver', 'gunner', 'alchemist'],
  gnome: ['warrior', 'mage', 'druid', 'gunner', 'alchemist'], troll: ['warrior', 'paladin', 'druid'],
  tauren: ['warrior', 'mage', 'druid', 'necro'], elf: ['mage', 'hunter', 'soulweaver', 'vampire'],
  tiefling: ['mage', 'soulweaver', 'warrior', 'akatsuki', 'alchemist'], dragonborn: ['mage', 'akatsuki', 'vampire', 'paladin']
};
const BOUNDS = { x0: -907, x1: 4300, z0: -1850, z1: 720 };       // the main island, the sea, Jadeheart Isle and Emberreach Isle
const num = (v, d = 0) => (typeof v === 'number' && isFinite(v)) ? v : d;
function validChar(c) {
  return c && typeof c === 'object' && typeof c.name === 'string' && RACE_CLASSES[c.race] && RACE_CLASSES[c.race].includes(c.cls) &&
    Number.isFinite(c.lvl) && c.lvl >= 1 && c.lvl <= 40 && Array.isArray(c.inv) && c.inv.length <= 200 && c.eq && typeof c.eq === 'object' && Array.isArray(c.pos);
}

// ---------------------------------------------------------------- world: monsters
function rng32(a) { return function () { a |= 0; a = a + 0x6D2B79F5 | 0; let t = Math.imul(a ^ a >>> 15, 1 | a); t = t + Math.imul(t ^ t >>> 7, 61 | t) ^ t; return ((t ^ t >>> 14) >>> 0) / 4294967296 } }
const rnd = rng32(WORLD.seed), R = (a, b) => a + rnd() * (b - a);
const mobs = []; let nextMob = 1;
function inZ(z, k) {
  const ar = z.rs.map(r => (r[1] - r[0]) * (r[3] - r[2])); let t = rnd() * ar.reduce((p, c) => p + c, 0), i = 0;
  while (i < ar.length - 1 && t > ar[i]) { t -= ar[i]; i++ }
  const r = z.rs[i]; return [(r[0] + r[1]) / 2 + (rnd() - .5) * (r[1] - r[0]) * k, (r[2] + r[3]) / 2 + (rnd() - .5) * (r[3] - r[2]) * k];
}
const HC = [];
const campNear = (zi, x, z) => (WORLD.camps || []).some(c => c.zi === zi && Math.hypot(x - c.x, z - c.z) < 40);
function hCenter(zi, md) {
  const z = WORLD.zones[zi]; let p;
  for (let t = 0; t < 80; t++) {
    p = inZ(z, .8);
    if (zi === 0 && Math.hypot(p[0], p[1]) < 34) continue;
    if (zi === 10 && p[1] < -378 && p[0] > 284 && p[0] < 479) continue;
    if (zi === 5 && WORLD.village && Math.hypot(p[0] - WORLD.village.x, p[1] - WORLD.village.z) < WORLD.village.r) continue;
    if (campNear(zi, p[0], p[1])) continue;
    if (HC.every(h => Math.hypot(h[0] - p[0], h[1] - p[1]) > md)) break;
  }
  for (let t = 0; t < 500 && ((zi === 0 && Math.hypot(p[0], p[1]) < 34) || (zi === 10 && p[1] < -378 && p[0] > 284 && p[0] < 479) || (zi === 5 && WORLD.village && Math.hypot(p[0] - WORLD.village.x, p[1] - WORLD.village.z) < WORLD.village.r) || campNear(zi, p[0], p[1])); t++) p = inZ(z, .8);
  HC.push(p); return p;
}
function spawn(type, x, z, lvl, win, zone, el) {
  const t = WORLD.mobs[type];
  mobs.push({
    id: nextMob++, type, lvl, t, max: t.flat ? t.hp : t.hp * (1 + .2 * lvl) * 1.2, dmg: t.flat ? t.dmg : t.dmg * (1 + .1 * lvl) * 1.1, hx: x, hz: z, x, z, hp: t.flat ? t.hp : t.hp * (1 + .2 * lvl) * 1.2,
    win: win || null, active: win ? 0 : 1, zone: zone || '', el: el ? 1 : 0,
    st: 0 /*0 idle 1 chase 2 back*/, cd: 0, stun: 0, wt: 0, wp: null, atk: 0, dead: win ? 1 : 0, dt: 0, resp: t.resp || (win ? 1e9 : el ? 120 : (type === 'boss' ? 60 : 25)),
    owner: 0, party: 0, target: 0, lastHit: 0, near: false
  });
}
function horde(zi, type, n, l0, l1) { const [cx, cz] = hCenter(zi, 48); for (let i = 0; i < n; i++) { const a = R(0, 6.28), d = R(1.5, 7); spawn(type, cx + Math.cos(a) * d, cz + Math.sin(a) * d, Math.round(R(l0, l1))) } }
function lone(zi, type, l0, l1) { const [x, z] = hCenter(zi, 34); spawn(type, x, z, Math.round(R(l0, l1))) }
for (const zi of Object.keys(WORLD.plan).map(Number).sort((a, b) => a - b))
  WORLD.plan[zi].forEach(([ty, sz, nh, nl, a, b]) => { for (let i = 0; i < nh * 2; i++) horde(zi, ty, sz, a, b); for (let i = 0; i < nl * 2; i++) lone(zi, ty, a, b) });
(WORLD.exclude || []).forEach(r => { for (let i = mobs.length - 1; i >= 0; i--) { const m = mobs[i]; if (m.x >= r[0] && m.x <= r[1] && m.z >= r[2] && m.z <= r[3]) mobs.splice(i, 1) } });   // Frostmere and its docks stay free of wandering monsters
(WORLD.extra || []).forEach(a => spawn(a[0], a[1], a[2], a[3]));                                                               // the jungle monsters of Jadeheart Isle
WORLD.specials.forEach(([t, x, z, l]) => spawn(t, x, z, l));
(WORLD.bosses || []).forEach(b => spawn(b.type, b.x, b.z, b.lvl, [b.from, b.to], b.zone));
(WORLD.elites || []).forEach(e => spawn('el' + e.i, e.x, e.z, e.lvl, null, '', 1));
(WORLD.camps || []).forEach(c => c.g.forEach(a => spawn(a[0], a[1], a[2], a[3])));
const mobById = new Map(mobs.map(m => [m.id, m]));
console.log('world ready:', mobs.length, 'monsters');

// ---------------------------------------------------------------- players / parties / nodes
const players = new Map(); // id -> player in world
const sessions = new Map(); // token -> username(lower)
const online = new Map();   // username(lower) -> conn (one login per account)
const byName = new Map();   // character name lower -> player
const parties = new Map(); let nextPid = 1, nextParty = 1;
const drops = new Map(); let nextDrop = 1;
const deadNodes = new Map(); // node index -> respawn time (ms)
let dayT = +process.env.START_TIME || 60; const CYC = WORLD.dayLen + WORLD.nightLen;
const hourNow = () => { const p = dayT % CYC; return (p < WORLD.dayLen ? 6 + p / WORLD.dayLen * 12 : 18 + (p - WORLD.dayLen) / WORLD.nightLen * 12) % 24 };
const dist = (ax, az, bx, bz) => Math.hypot(ax - bx, az - bz);
const sendAll = (o, except) => { const s = JSON.stringify(o); players.forEach(p => { if (p !== except) p.c.send(s) }) };

function partyOf(p) { return p.party ? parties.get(p.party) : null }
function pushParty(pt) {
  const members = pt.members.map(id => players.get(id)).filter(Boolean);
  const msg = JSON.stringify({ t: 'party', id: pt.id, leader: pt.leader, m: members.map(p => [p.id, p.name, p.cls, p.lvl]) });
  members.forEach(p => p.c.send(msg));
}
function leaveParty(p, why) {
  const pt = partyOf(p); if (!pt) return;
  pt.members = pt.members.filter(id => id !== p.id); p.party = 0;
  p.c.send({ t: 'party', id: 0, leader: 0, m: [], why });
  if (pt.members.length < 2) {
    pt.members.forEach(id => { const q = players.get(id); if (q) { q.party = 0; q.c.send({ t: 'party', id: 0, leader: 0, m: [], why: 'The party was disbanded.' }) } });
    parties.delete(pt.id);
  } else { if (pt.leader === p.id) pt.leader = pt.members[0]; pushParty(pt) }
}
function sys(p, m) { p.c.send({ t: 'sys', m }) }

// ---------------------------------------------------------------- mobs: simulation
function releaseClaim(m) { m.owner = 0; m.party = 0; m.target = 0 }
function canFight(p, m) { return !m.owner || m.owner === p.id || (p.party && m.party === p.party) }
function aggroCheck(m) {
  for (const p of players.values()) {
    if (p.dead || p.stealth) continue;
    const d = dist(p.x, p.z, m.x, m.z);
    if ((d < m.t.ag && (m.lvl >= p.lvl + 5 || m.t.al)) || (p.race === 'tiefling' && d < 30)) return p;
  }
  return null;
}
function step(m, tx, tz, sp, dt) { const dx = tx - m.x, dz = tz - m.z, l = Math.hypot(dx, dz) || 1; m.x += dx / l * sp * dt; m.z += dz / l * sp * dt }
// ---- Boss attack engine (Shae Phu, the ice dragon, the mech): telegraphed attacks. The same code runs on the server and in the offline game. ----
const SK_SETS={
 shae:{lance:{dur:1.9,len:52,w:6.5,dmg:650,nm:'Magma Lance'},comet:{dur:2.3,r:6.5,dmg:700,nm:'Comet Rain'},stomp:{dur:1.7,r:16,dmg:600,nm:'Cyclops Stomp'},charge:{dur:1.5,len:44,w:7.5,dmg:680,nm:'Lava Charge'}},
 ice:{lance:{dur:1.9,len:50,w:6.5,dmg:600,nm:'Frost Breath'},comet:{dur:2.3,r:6.5,dmg:650,nm:'Icicle Storm'},stomp:{dur:1.7,r:15,dmg:560,nm:'Glacial Slam'},charge:{dur:1.5,len:42,w:7.5,dmg:620,nm:'Wing Rush'}},
 mech:{lance:{dur:1.9,len:46,w:6,dmg:450,nm:'Laser Beam'},comet:{dur:2.3,r:6,dmg:490,nm:'Missile Barrage'},stomp:{dur:1.7,r:14,dmg:420,nm:'Shockwave Stomp'},charge:{dur:1.5,len:38,w:7,dmg:460,nm:'Piston Charge'}},
 crate:{lance:{dur:1.9,len:44,w:6.5,dmg:560,nm:'Chain Hook'},comet:{dur:2.3,r:6.5,dmg:610,nm:'Pack Bombs'},stomp:{dur:1.7,r:14,dmg:520,nm:'Pallet Slam'},charge:{dur:1.5,len:38,w:7.5,dmg:580,nm:'Forklift Rush'}},
 miner:{lance:{dur:1.7,len:46,w:8,dmg:470,nm:'Rolling Dynamite'},comet:{dur:1.8,r:5,dmg:500,n:[7,10],nm:'Dynamite Barrage'},stomp:{dur:1.6,r:15,dmg:440,nm:'Pickaxe Quake'},charge:{dur:1.4,len:40,w:8,dmg:490,nm:'Minecart Rush'}},
 santa:{ph:.5,cd1:6.5,cd2:4.2,a1:['sleigh','coal','hoho','cane','bombs','ycross'],a2:['sleigh','coal','hoho','cane','bombs','ycross'],sleigh:{sh:'charge',dur:1.5,dmg:700,nm:'Sleigh Charge',len:40,w:9},coal:{sh:'comet',dur:2.0,dmg:650,nm:'Coal Barrage',r:5.5,n:[4,6]},hoho:{sh:'stomp',dur:1.7,dmg:640,nm:'Ho Ho Slam',r:15},cane:{sh:'lance',dur:1.5,dmg:680,nm:'Candy Cane Lance',len:60,w:5.5},bombs:{sh:'comet',dur:2.2,dmg:700,nm:'Present Bombs',r:7.5,n:[3,5]},ycross:{sh:'cross',dur:1.8,dmg:690,nm:'Yuletide Cross',len:34,w:6}},
 rwiz:{ph:.5,cd1:6.5,cd2:4.2,a1:['antler','bolt','aurora','ring','wand','star'],a2:['antler','bolt','aurora','ring','wand','star'],antler:{sh:'charge',dur:1.4,dmg:710,nm:'Antler Charge',len:38,w:8},bolt:{sh:'comet',dur:1.9,dmg:640,nm:'Frost Bolt Volley',r:4.8,n:[5,7]},aurora:{sh:'lance',dur:1.6,dmg:690,nm:'Aurora Beam',len:66,w:6},ring:{sh:'stomp',dur:1.9,dmg:650,nm:'Blizzard Ring',r:18},wand:{sh:'cone',dur:1.5,dmg:670,nm:'Wand Sweep',r:26,ang:1.2},star:{sh:'cross',dur:1.8,dmg:690,nm:'Star Cross',len:36,w:5.5}},
 snowb:{ph:.5,cd1:6.5,cd2:4.2,a1:['snowball','belly','carrot','roll','sweep','bliz'],a2:['snowball','belly','carrot','roll','sweep','bliz'],snowball:{sh:'comet',dur:1.9,dmg:650,nm:'Snowball Barrage',r:5,n:[5,8]},belly:{sh:'stomp',dur:1.6,dmg:660,nm:'Belly Slam',r:16},carrot:{sh:'lance',dur:1.5,dmg:690,nm:'Carrot Lance',len:58,w:6.5},roll:{sh:'charge',dur:1.4,dmg:720,nm:'Rolling Charge',len:36,w:10},sweep:{sh:'cone',dur:1.5,dmg:680,nm:'Frostbite Sweep',r:28,ang:1.25},bliz:{sh:'cross',dur:1.8,dmg:690,nm:'Blizzard Cross',len:38,w:6}},
 kramp:{ph:.5,cd1:6.5,cd2:4.2,a1:['chain','hell','goat','dark','claw','curse'],a2:['chain','hell','goat','dark','claw','curse'],chain:{sh:'lance',dur:1.4,dmg:700,nm:'Chain Lash',len:64,w:5},hell:{sh:'comet',dur:2.0,dmg:660,nm:'Hellfire Rain',r:5.5,n:[4,6]},goat:{sh:'stomp',dur:1.5,dmg:650,nm:'Goat Stomp',r:14},dark:{sh:'charge',dur:1.4,dmg:720,nm:'Dark Charge',len:42,w:8},claw:{sh:'cone',dur:1.4,dmg:680,nm:'Claw Sweep',r:25,ang:1.15},curse:{sh:'cross',dur:1.7,dmg:690,nm:'Curse Cross',len:36,w:5.5}},
 glad:{ph:.4,cd1:5,cd2:2.6,a1:['thrust','volley','slam','charge','sweep','cross'],a2:['lunge','barrage','pyre','nova'],
  thrust:{sh:'lance',dur:1.4,len:64,w:5,dmg:700,nm:'Spear Thrust'},volley:{sh:'comet',dur:1.9,r:5.2,n:[5,5],dmg:650,nm:'Javelin Volley'},slam:{sh:'stomp',dur:1.5,r:13,dmg:640,nm:'Shield Slam'},charge:{sh:'charge',dur:1.4,len:36,w:8,dmg:720,nm:'Gladiator Charge'},sweep:{sh:'cone',dur:1.5,r:26,ang:1.15,dmg:680,nm:'Crescent Sweep'},cross:{sh:'cross',dur:1.8,len:36,w:5.5,dmg:690,nm:'Spear Cross'},
  lunge:{sh:'charge',dur:.9,len:38,w:10,dmg:1400,nm:'Inferno Lunge'},barrage:{sh:'comet',dur:1.0,r:4.6,n:[9,9],dmg:1300,nm:'Flame Javelin Barrage'},pyre:{sh:'cross',dur:1.1,len:42,w:7.5,dmg:1400,nm:'Pyre Cross'},nova:{sh:'stomp',dur:1.0,r:18,dmg:1350,nm:'Blazing Nova'}}};
const SHAE_SK=SK_SETS.shae;
function shaeInShape(k,x,z,p){const pr=.8;if(k.kind=='comet'||k.kind=='stomp'){for(const c of k.pts)if(Math.hypot(p.x-c[0],p.z-c[1])<k.r+pr)return true;return false}
 const dx=Math.sin(k.a),dz=Math.cos(k.a),rx=p.x-k.x0,rz=p.z-k.z0,t=rx*dx+rz*dz,sg=rx*dz-rz*dx,s=Math.abs(sg);
 if(k.kind=='cone'){const d=Math.hypot(rx,rz);if(d>k.r+pr)return false;if(d<2.5)return true;return Math.acos(Math.max(-1,Math.min(1,t/d)))<=k.ang}
 if(k.kind=='cross')return(Math.abs(t)<=k.len&&s<k.w/2+pr)||(Math.abs(sg)<=k.len&&Math.abs(t)<k.w/2+pr);
 return t>=-1&&t<=k.len&&s<k.w/2+pr}
// S: persistent state, B: {x,z,hp,max,id,t:{set}}, env: {players(), hit(id,dmg), tele(msg), rnd(), atk()}, tg: current target {x,z} or null. Returns true while the boss is busy casting.
function shaeSim(S,B,dt,env,tg){const set=(B.t&&B.t.set)||B.set||'shae',SKS=SK_SETS[set]||SK_SETS.shae,ph2=SKS.ph?B.hp<=B.max*SKS.ph:B.hp<B.max*.5;
 if(S.sk){const k=S.sk;k.t+=dt;
  if(k.kind=='charge'&&k.t>=k.dur){const sp=k.len/.45;if(!k.dashed){k.dashed=1;k.dl=0;env.atk()}const step=Math.min(sp*dt,k.len-k.dl);B.x+=Math.sin(k.a)*step;B.z+=Math.cos(k.a)*step;k.dl+=step;if(k.dl>=k.len-1e-6){shaeResolve(S,B,env,k);S.sk=null;S.cd=ph2?(SKS.cd2||4.2):(SKS.cd1||6.8)}return true}
  if(k.kind!='charge'&&k.t>=k.dur){shaeResolve(S,B,env,k);S.sk=null;S.cd=ph2?(SKS.cd2||4.2):(SKS.cd1||6.8);return false}
  return true}
 S.cd-=dt;if(S.cd>0||!tg||Math.hypot(tg.x-B.x,tg.z-B.z)>75)return false;
 const kinds=(SKS.a1?(ph2?SKS.a2:SKS.a1):['lance','comet','stomp','charge']).filter(x=>x!=S.last);const kind=kinds[Math.floor(env.rnd()*kinds.length)];S.last=kind;const d=SKS[kind],sh=d.sh||kind,pl=env.players();
 const k={kind:sh,id:kind,ang:d.ang||0,t:0,dur:d.dur,x0:B.x,z0:B.z,a:Math.atan2(tg.x-B.x,tg.z-B.z),dmg:d.dmg,len:d.len||0,w:d.w||0,r:d.r||0,pts:[]};
 if(sh=='comet'){const want=d.n?(ph2?d.n[1]:d.n[0]):(ph2?5:3);pl.slice(0,want).forEach(p=>k.pts.push([p.x,p.z]));while(k.pts.length<want){const a=env.rnd()*6.283,r=3+env.rnd()*12;k.pts.push([tg.x+Math.cos(a)*r,tg.z+Math.sin(a)*r])}}
 if(sh=='stomp')k.pts=[[B.x,B.z]];
 S.sk=k;env.tele({k:sh,id:kind,x:k.x0,z:k.z0,a:k.a,len:k.len,w:k.w,r:k.r,ang:k.ang,pts:k.pts,dur:k.dur,th:set,nm:d.nm});return true}
function shaeResolve(S,B,env,k){env.atk();env.players().forEach(p=>{if(shaeInShape(k,k.x0,k.z0,p))env.hit(p.id,k.dmg)})}


function shaeEnv(m) {
  return {
    rnd: rnd,
    players: () => { const L = []; for (const q of players.values()) if (!q.dead && !q.stealth && dist(q.x, q.z, m.x, m.z) < 90) L.push({ id: q.id, x: q.x, z: q.z }); return L },
    hit: (id, d) => { const q = players.get(id); if (q) q.c.send({ t: 'mhit', id: m.id, d, u: 1 }) },
    tele: o => { const s = JSON.stringify(Object.assign({ t: 'tele', id: m.id }, o)); players.forEach(q => { if (dist(q.x, q.z, m.x, m.z) < 240) q.c.send(s) }) },
    atk: () => { m.atk = .6 }
  }
}

// ---- the Frostbound Reach (winter raid): two of the four bosses are picked every 20 minutes ----
const WINT = { ms: 1200000, cur: -1, slots: [[3317, 134], [3452, -28]], types: ['wsanta', 'wwiz', 'wsnowb', 'wkramp'] };
function winterCycle() {
  const cyc = Math.floor(Date.now() / WINT.ms); if (cyc === WINT.cur) return; WINT.cur = cyc;
  const r = rng32((cyc * 2654435761) >>> 0), idx = [0, 1, 2, 3];
  for (let i = 3; i > 0; i--) { const j = Math.floor(r() * (i + 1)); const t = idx[i]; idx[i] = idx[j]; idx[j] = t }
  const pick = [idx[0], idx[1]], names = [];
  WINT.types.forEach((ty, k) => { const m = mobs.find(q => q.type === ty); if (!m) return; const sl = pick.indexOf(k);
    if (sl >= 0) { m.hx = WINT.slots[sl][0]; m.hz = WINT.slots[sl][1]; m.active = 1; m.dead = 0; m.dt = 0; m.hp = m.max; m.x = m.hx; m.z = m.hz; m.st = 0; m.stun = 0; m.S = null; releaseClaim(m); names.push(m.t.nm) }
    else { m.active = 0; m.dead = 1; m.hp = 0; m.st = 0; m.stun = 0; m.S = null; releaseClaim(m) } });
  sendAll({ t: 'sys', m: '❄ New run in the Frostbound Reach: ' + names.join(' and ') + ' wait for challengers.' });
}
function simMob(m, dt) {
  if (m.win && !m.active) return;
  if (m.dead) {
    m.dt += dt;
    if (m.dt > m.resp) { m.S = null; m.dead = 0; m.hp = m.max; m.x = m.hx; m.z = m.hz; m.st = 0; m.stun = 0; releaseClaim(m) }
    return;
  }
  // only simulate monsters that have a player within ~220 units (cheap)
  let near = false;
  for (const p of players.values()) if (Math.abs(p.x - m.x) < 220 && Math.abs(p.z - m.z) < 220) { near = true; break }
  m.near = near; if (!near && m.st === 0) return;
  if (m.t.imm) { m.stun = 0; m.slowT = 0; m.weakT = 0 }
  if (m.slowT > 0) m.slowT -= dt;
  if (m.weakT > 0) m.weakT -= dt;
  const sf = m.slowT > 0 ? 1 - (m.slowF || .5) : 1;
  m.cd -= dt * (m.slowT > 0 && m.slowA ? sf : 1); m.atk = Math.max(0, m.atk - dt);
  if (m.stun > 0) { m.stun -= dt; return }
  if (m.mcT > 0) {
    m.mcT -= dt;
    let t = mobById.get(m.mcTarget);
    if (!t || t.dead) { t = null; let bd = 40; for (const q of mobs) { if (q === m || q.dead || (q.win && !q.active)) continue; const d = dist(q.x, q.z, m.x, m.z); if (d < bd) { bd = d; t = q } } if (t) m.mcTarget = t.id }
    if (!t || m.mcT <= 0) { m.mcT = 0; m.st = 2; return }
    if (dist(t.x, t.z, m.x, m.z) > m.t.r + t.t.r + 1.4) step(m, t.x, t.z, m.t.sp * sf, dt);
    else if (m.cd <= 0) {
      m.cd = m.t.ai; m.atk = .3; t.hp -= m.dmg;
      if (t.hp <= 0) { const k = players.get(m.mcBy); if (k) killMob(t, k); else { t.dead = 1; t.dt = 0; t.hp = 0; t.st = 0; t.stun = 0; releaseClaim(t) } }
    }
    return;
  }
  if (m.st === 0) {
    const p = aggroCheck(m); if (p && canFight(p, m)) { m.st = 1; m.target = p.id }
    m.wt -= dt; if (m.wt <= 0) { m.wt = R(3, 7); m.wp = [m.hx + R(-8, 8), m.hz + R(-8, 8)] }
    if (m.wp && Math.hypot(m.wp[0] - m.x, m.wp[1] - m.z) > 1) step(m, m.wp[0], m.wp[1], m.t.sp * .35 * sf, dt);
  }
  if (m.st === 1) {
    let p = players.get(m.target);
    if (m.lastHit && Date.now() - m.lastHit > 30000) { m.st = 2; m.lastHit = 0; releaseClaim(m); return }
    if (!p || p.dead || p.stealth || dist(m.x, m.z, m.hx, m.hz) > (m.t.leash || 75)) {
      // try the owner or a party member before giving up
      p = null;
      if (m.owner) { const o = players.get(m.owner); if (o && !o.dead && !o.stealth) p = o }
      if (!p && m.party) { const pt = parties.get(m.party); if (pt) for (const id of pt.members) { const q = players.get(id); if (q && !q.dead && !q.stealth && dist(q.x, q.z, m.x, m.z) < 60) { p = q; break } } }
      if (!p || dist(m.x, m.z, m.hx, m.hz) > (m.t.leash || 75)) { m.st = 2; releaseClaim(m); return }
      m.target = p.id;
    }
    if (m.t.sk) { const S = m.S || (m.S = { cd: 5, last: '', sk: null }); if (shaeSim(S, m, dt, shaeEnv(m), { x: p.x, z: p.z })) return }
    const dd = dist(p.x, p.z, m.x, m.z);
    if (dd > m.t.r + 1.7) step(m, p.x, p.z, m.t.sp * sf, dt);
    else if (m.cd <= 0) { m.cd = m.t.ai; m.atk = .3; if (m.owner) m.lastHit = Date.now(); p.c.send({ t: 'mhit', id: m.id, d: Math.round(m.dmg * (m.weakT > 0 ? 1 - (m.weakF || .25) : 1) * (m.t.set === 'glad' && m.hp <= m.max * .4 ? 2 : 1) * 10) / 10 }) }
  }
  if (m.st === 2) {
    m.S = null;
    step(m, m.hx, m.hz, m.t.sp * sf, dt); m.hp = Math.min(m.max, m.hp + m.max * .1 * dt);
    if (dist(m.x, m.z, m.hx, m.hz) < 2) { m.st = 0; m.hp = m.max }
  }
}
function killMob(m, killer) {
  m.dead = 1; m.dt = 0; m.hp = 0; m.st = 0; m.stun = 0;
  const ids = new Set([killer.id]);
  if (m.owner) ids.add(m.owner);
  const pt = parties.get(m.party) || partyOf(killer); if (pt) pt.members.forEach(id => ids.add(id));
  const el = [...ids].map(id => players.get(id)).filter(p => p && dist(p.x, p.z, m.x, m.z) < 160);
  const n = el.length || 1, share = (1 + .1 * (n - 1)) / n;
  el.forEach(p => p.c.send({ t: 'kill', id: m.id, sh: Math.round(share * 1000) / 1000, n }));
  releaseClaim(m);
}
function onHit(p, msg, isStun) {
  const m = mobById.get(msg.id | 0); if (!m || m.dead) return;
  if (dist(p.x, p.z, m.x, m.z) > 60) return;                         // too far away to have hit it
  if (!canFight(p, m)) { const o = players.get(m.owner); p.c.send({ t: 'deny', id: m.id, by: o ? o.name : 'another player' }); return }
  if (!m.owner) { m.owner = p.id; m.party = p.party; p.c.send({ t: 'sys', m: 'You claimed ' + m.t.nm + '.' }) }
  m.lastHit = Date.now(); if (m.st !== 1) m.st = 1; m.target = p.id;
  if (m.t.imm && isStun) return;                                       // bosses such as Shae Phu ignore stuns, slows, pulls and weakening
  if (isStun === 2) { m.slowT = Math.max(m.slowT || 0, Math.min(8, num(msg.s))); m.slowF = Math.max(.1, Math.min(.8, num(msg.f, .5))); m.slowA = msg.a ? 1 : 0; return }
  if (isStun === 4) { m.weakT = Math.max(m.weakT || 0, Math.min(4, num(msg.s))); m.weakF = Math.max(.05, Math.min(.5, num(msg.f, .25))); return }
  if (isStun === 3) {                                                       // pulled toward a point (Grasp of the Abyss, Abyssal Pit)
    const now = Date.now(); if (now - (m.pullAt || 0) < 120) return; m.pullAt = now;
    const tx = num(msg.x, m.x), tz = num(msg.z, m.z); if (dist(tx, tz, p.x, p.z) > 40) return;
    const k = Math.max(0, Math.min(40, num(msg.k, 3))), dx = tx - m.x, dz = tz - m.z, l = Math.hypot(dx, dz);
    if (l > .01) { const q = Math.min(k, l); m.x += dx / l * q; m.z += dz / l * q }
    return
  }
  if (isStun) { m.stun = Math.max(m.stun, Math.min(6, num(msg.s))); return }
  const maxHit = 120 + p.lvl * 55, now = Date.now();
  p.budget = Math.min(maxHit * 8, (p.budget || 0) + (now - (p.bt || now)) / 1000 * maxHit * 6); p.bt = now;
  const d = Math.max(0, Math.min(maxHit, num(msg.d)));
  if (p.budget < d) return; p.budget -= d;
  m.hp -= d;
  if (m.hp <= 0) killMob(m, p);
}

// ---------------------------------------------------------------- game loop
let last = Date.now(), tick = 0;
setInterval(() => {
  const now = Date.now(), dt = Math.min(.15, (now - last) / 1000); last = now; tick++;
  dayT += dt;
  for (const m of mobs) simMob(m, dt);
  deadNodes.forEach((t, i) => { if (now >= t) { deadNodes.delete(i); sendAll({ t: 'node', i, a: 1 }) } });
  if (tick % 20 === 0) {
    winterCycle();
    const h = hourNow();
    for (const m of mobs) {
      if (!m.win || m.t.wint) continue; const act = h >= m.win[0] && h < m.win[1];
      if (act && !m.active) { m.active = 1; m.dead = 0; m.dt = 0; m.hp = m.max; m.x = m.hx; m.z = m.hz; m.st = 0; m.stun = 0; releaseClaim(m); sendAll({ t: 'sys', m: '⚠ ' + m.t.nm + ' has appeared in ' + (m.zone || 'the wilds') + '! It is marked on your map.' }) }
      else if (!act && m.active) { m.active = 0; m.dead = 1; m.hp = 0; m.st = 0; m.stun = 0; releaseClaim(m) }
    }
    const nowT = Date.now(); drops.forEach((d, id) => { if (nowT - d.t > 300000) { drops.delete(id); sendAll({ t: 'dropdel', id }) } });
  }
  if (tick % 2 === 0) snapshots();
}, 50);

function snapshots() {
  const tm = Math.round(dayT * 10) / 10, bs = mobs.filter(m => m.win).map(m => [m.id, m.active ? 1 : 0, m.active && m.dead ? 1 : 0]),
    es = mobs.filter(m => m.el || m.t.nb).map(m => [m.id, m.dead ? 1 : 0, m.dead ? Math.max(0, Math.round(m.resp - m.dt)) : 0]);
  for (const p of players.values()) {
    const pl = [], mb = [], pm = [];
    for (const q of players.values()) {
      if (q === p) continue;
      if (Math.abs(q.x - p.x) < 320 && Math.abs(q.z - p.z) < 320 || (p.party && q.party === p.party)) pl.push([q.id, +q.x.toFixed(1), +q.z.toFixed(1), +q.r.toFixed(2), q.f, Math.round(q.hp), Math.round(q.mh), q.fm | 0, q.mn || []]);
    }
    for (const m of mobs) {
      if (Math.abs(m.x - p.x) > 190 || Math.abs(m.z - p.z) > 190) continue;
      mb.push([m.id, +m.x.toFixed(1), +m.z.toFixed(1), Math.round(m.hp), (m.dead ? 1 : 0) | (m.stun > 0 ? 2 : 0) | (m.atk > 0 ? 4 : 0) | (m.st === 1 ? 8 : 0), m.owner]);
    }
    const pt = partyOf(p); if (pt) pt.members.forEach(id => { const q = players.get(id); if (q && q !== p) pm.push([q.id, Math.round(q.hp), Math.round(q.mh), q.lvl]) });
    p.c.send({ t: 'snap', tm, pl, mb, pm, bs, es });
  }
}

// ---------------------------------------------------------------- accounts
const hashPw = (pw, salt, cb) => crypto.scrypt(pw, salt, 32, { N: 16384 }, (e, k) => cb(e, k && k.toString('hex')));
const loginTries = new Map();
function tooMany(ip) { const now = Date.now(), a = (loginTries.get(ip) || []).filter(t => now - t < 60000); a.push(now); loginTries.set(ip, a); return a.length > 12 }
const cleanUser = u => typeof u === 'string' && /^[A-Za-z0-9_]{3,16}$/.test(u);
const cleanName = n => typeof n === 'string' && /^[A-Za-z][A-Za-z0-9 _-]{1,13}$/.test(n.trim());
const stripCtl = s => String(s).replace(/[\u0000-\u001f\u007f<>]/g, '').slice(0, 200);

function authed(c, uname, user) {
  const old = online.get(uname); if (old && old !== c) { old.send({ t: 'kicked', m: 'You logged in from another place.' }); old.close() }
  c.user = uname; online.set(uname, c);
  const token = crypto.randomBytes(24).toString('hex'); sessions.set(token, { u: uname, t: Date.now() });
  user.lastLogin = Date.now(); dirty = true;
  c.send({ t: 'authok', token, user: user.name, chars: user.chars, online: players.size, max: MAXP });
}

function handle(c, m) {
  if (m.t === 'ping') return c.send({ t: 'pong', n: m.n });
  if (m.t === 'auth') {
    if (c.authBusy) return;
    if (tooMany(c.ip)) return c.send({ t: 'autherr', m: 'Too many attempts. Wait a minute.' });
    if (m.mode === 'token') {
      const s = sessions.get(String(m.token)); if (!s || Date.now() - s.t > 7 * 864e5) return c.send({ t: 'autherr', m: 'Session expired. Please log in.', tok: 1 });
      const u = accounts.users[s.u]; if (!u) return c.send({ t: 'autherr', m: 'Unknown account.', tok: 1 });
      sessions.delete(String(m.token)); return authed(c, s.u, u);
    }
    if (!cleanUser(m.user)) return c.send({ t: 'autherr', m: 'Username: 3-16 letters, numbers or _.' });
    if (typeof m.pass !== 'string' || m.pass.length < 6 || m.pass.length > 64) return c.send({ t: 'autherr', m: 'Password must be 6-64 characters.' });
    const uname = m.user.toLowerCase();
    c.authBusy = true;
    if (m.mode === 'register') {
      if (INVITE && m.invite !== INVITE) { c.authBusy = false; return c.send({ t: 'autherr', m: 'Wrong invite code.' }) }
      if (accounts.users[uname]) { c.authBusy = false; return c.send({ t: 'autherr', m: 'That username is taken.' }) }
      const salt = crypto.randomBytes(16).toString('hex');
      hashPw(m.pass, salt, (e, h) => {
        c.authBusy = false; if (e || !c.open) return;
        if (accounts.users[uname]) return c.send({ t: 'autherr', m: 'That username is taken.' });
        const user = { name: m.user, salt, hash: h, created: Date.now(), chars: [null, null, null] };
        accounts.users[uname] = user; dirty = true; console.log('new account', m.user); authed(c, uname, user);
      });
    } else {
      const user = accounts.users[uname];
      if (!user) { c.authBusy = false; return c.send({ t: 'autherr', m: 'Wrong username or password.' }) }
      hashPw(m.pass, user.salt, (e, h) => {
        c.authBusy = false; if (e || !c.open) return;
        if (h.length !== user.hash.length || !crypto.timingSafeEqual(Buffer.from(h), Buffer.from(user.hash))) return c.send({ t: 'autherr', m: 'Wrong username or password.' });
        authed(c, uname, user);
      });
    }
    return;
  }
  if (!c.user) return;
  const user = accounts.users[c.user];
  if (!user) return;
  const p = c.player;

  switch (m.t) {
    case 'saveChar': {
      const slot = m.slot | 0; if (slot < 0 || slot > 2 || !validChar(m.data)) return c.send({ t: 'saveerr', slot, m: 'Invalid character data.' });
      const d = m.data, old = user.chars[slot];
      if (JSON.stringify(d).length > 600000) return;
      if (!old) {
        const nm = d.name.trim();
        if (!cleanName(nm)) return c.send({ t: 'saveerr', slot, m: 'Names: 2-14 letters, numbers, space, - or _.' });
        if (nameIndex[nm.toLowerCase()]) return c.send({ t: 'saveerr', slot, m: 'That character name is taken.' });
        d.name = nm; nameIndex[nm.toLowerCase()] = c.user;
      } else {
        d.name = old.name; d.race = old.race;                                  // name and race can't change
        const nc2 = (RACE_CLASSES[old.race] || []).includes(d.cls) ? d.cls : old.cls;   // a valid class change (Veyra) is kept
        if (nc2 !== old.cls && c.player && c.player.slot === slot) { c.player.cls = nc2; sendAll({ t: 'pcls', id: c.player.id, cls: nc2 }, c.player) }
        d.cls = nc2;
        if (d.lvl > old.lvl + 2 && !c.adm) d.lvl = old.lvl;                    // no sudden level jumps (unless the player is in admin mode)
      }
      user.chars[slot] = d; dirty = true; if (c.player && c.player.slot === slot) { c.player.lvl = d.lvl }
      if (!old) c.send({ t: 'chars', chars: user.chars });
      return;
    }
    case 'delChar': {
      const slot = m.slot | 0, ch = user.chars[slot]; if (!ch) return;
      if (p && p.slot === slot) return;
      delete nameIndex[ch.name.toLowerCase()]; user.chars[slot] = null; dirty = true; return c.send({ t: 'chars', chars: user.chars });
    }
    case 'chcls': {
      const slot = m.slot | 0, ch = user.chars[slot]; if (!ch) return;
      const nc = String(m.cls); if (!(RACE_CLASSES[ch.race] || []).includes(nc)) return;
      ch.cls = nc; dirty = true; if (p && p.slot === slot) { p.cls = nc; sendAll({ t: 'pcls', id: p.id, cls: nc }, p) }
      c.send({ t: 'clsok', slot, cls: nc }); return;
    }
    case 'play': {
      if (p) return; const slot = m.slot | 0, ch = user.chars[slot]; if (!ch) return;
      if (players.size >= MAXP) return c.send({ t: 'full', m: 'The world is full (' + MAXP + ' players). Try again soon.' });
      const pl = {
        id: nextPid++, c, slot, user: c.user, name: ch.name, race: ch.race, cls: ch.cls, lvl: ch.lvl, x: num(ch.pos[0]), z: num(ch.pos[1]),
        r: 0, f: 0, hp: 100, mh: 100, dead: 0, stealth: 0, party: 0, invites: {}, chat: [], budget: 800, bt: Date.now()
      };
      c.player = pl; players.set(pl.id, pl); byName.set(pl.name.toLowerCase(), pl);
      c.send({
        t: 'welcome', id: pl.id, tm: dayT, now: Date.now(), max: MAXP, sv: PROTO, wv: WORLD.proto | 0,
        mobs: mobs.map(q => [q.id, q.type, q.lvl, Math.round(q.hx), Math.round(q.hz), Math.round(q.x), Math.round(q.z), Math.round(q.hp), q.dead, q.owner, q.win ? (q.active ? 1 : 0) : 1]),
        nodes: [...deadNodes.keys()],
        drops: [...drops.values()].map(d => ({ id: d.id, item: d.item, x: d.x, z: d.z, by: d.by })),
        players: [...players.values()].filter(q => q !== pl).map(q => [q.id, q.name, q.race, q.cls, q.lvl, q.x, q.z])
      });
      sendAll({ t: 'pj', p: [pl.id, pl.name, pl.race, pl.cls, pl.lvl, pl.x, pl.z] }, pl);
      sendAll({ t: 'sys', m: pl.name + ' entered the world.' }, pl);
      console.log(pl.name, 'joined (' + players.size + ' online)');
      return;
    }
  }
  if (!p) return;
  switch (m.t) {
    case 'pos': {
      const x = num(m.x, p.x), z = num(m.z, p.z);
      if (x < BOUNDS.x0 || x > BOUNDS.x1 || z < BOUNDS.z0 || z > BOUNDS.z1) return;
      p.x = x; p.z = z; p.r = num(m.r); p.f = m.f | 0; p.hp = num(m.hp, p.hp); p.mh = Math.max(1, num(m.mh, p.mh)); p.lvl = Math.max(1, Math.min(40, m.l | 0 || p.lvl));
      p.dead = (p.f & 2) ? 1 : 0; p.stealth = (p.f & 4) ? 1 : 0;
      p.fm = Math.max(0, Math.min(9, m.fm | 0));
      p.mn = Array.isArray(m.mn) ? m.mn.slice(0, 8).filter(a => Array.isArray(a) && a.length >= 3).map(a => [num(a[0]), num(a[1]), a[2] | 0, a[3] | 0]) : []; return;
    }
    case 'hit': return onHit(p, m, false);
    case 'stun': return onHit(p, m, true);
    case 'slow': return onHit(p, m, 2);
    case 'weak': return onHit(p, m, 4);
    case 'pull': return onHit(p, m, 3);
    case 'node': {
      const i = m.i | 0; if (i < 0 || i > 20000 || deadNodes.has(i)) return;
      deadNodes.set(i, Date.now() + WORLD.nodeRespawn * 1000); sendAll({ t: 'node', i, a: 0 }, p); return;
    }
    case 'chat': {
      const text = stripCtl(m.m || ''); if (!text.trim()) return;
      const now = Date.now(); p.chat = p.chat.filter(t => now - t < 10000); if (p.chat.length >= 8) return sys(p, 'Slow down!'); p.chat.push(now);
      const ch = m.ch, out = { t: 'chat', ch, from: p.name, m: text };
      if (ch === 'g') sendAll(out);
      else if (ch === 'p') { const pt = partyOf(p); if (!pt) return sys(p, 'You are not in a party.'); pt.members.forEach(id => { const q = players.get(id); if (q) q.c.send(out) }) }
      else if (ch === 'w') {
        const q = byName.get(String(m.to || '').toLowerCase()); if (!q) return sys(p, 'No player named "' + stripCtl(m.to || '') + '" is online.');
        q.c.send({ ...out, to: q.name }); p.c.send({ ...out, to: q.name, me: 1 });
      } else { players.forEach(q => { if (dist(q.x, q.z, p.x, p.z) < 120) q.c.send(out) }) }
      return;
    }
    case 'inv': {
      const q = byName.get(String(m.name || '').toLowerCase());
      if (!q || q === p) return sys(p, 'Player not found.');
      if (q.party) return sys(p, q.name + ' is already in a party.');
      const pt = partyOf(p);
      if (pt && pt.leader !== p.id) return sys(p, 'Only the party leader can invite.');
      if (pt && pt.members.length >= 5) return sys(p, 'Your party is full (5).');
      q.invites[p.id] = Date.now(); q.c.send({ t: 'invite', from: p.name, fid: p.id }); sys(p, 'Invited ' + q.name + '.'); return;
    }
    case 'invr': {
      const f = players.get(m.fid | 0), at = p.invites[m.fid | 0]; delete p.invites[m.fid | 0];
      if (!f || !at || Date.now() - at > 60000) return sys(p, 'That invitation expired.');
      if (!m.ok) return sys(f, p.name + ' declined your invitation.');
      if (p.party) return sys(p, 'You are already in a party.');
      let pt = partyOf(f);
      if (!pt) { pt = { id: nextParty++, leader: f.id, members: [f.id] }; parties.set(pt.id, pt); f.party = pt.id }
      if (pt.members.length >= 5) return sys(p, 'That party is full.');
      pt.members.push(p.id); p.party = pt.id; pushParty(pt);
      pt.members.forEach(id => { const q = players.get(id); if (q) sys(q, p.name + ' joined the party.') }); return;
    }
    case 'leave': return leaveParty(p, 'You left the party.');
    case 'kick': {
      const pt = partyOf(p); if (!pt || pt.leader !== p.id) return sys(p, 'Only the leader can do that.');
      const q = byName.get(String(m.name || '').toLowerCase()); if (!q || q.party !== p.party || q === p) return;
      leaveParty(q, 'You were removed from the party.'); return;
    }
    case 'lead': {
      const pt = partyOf(p); if (!pt || pt.leader !== p.id) return sys(p, 'Only the leader can do that.');
      const q = byName.get(String(m.name || '').toLowerCase()); if (!q || q.party !== p.party) return;
      pt.leader = q.id; pushParty(pt); return;
    }
    case 'drop': {
      const it = m.item, x = num(m.x, p.x), z = num(m.z, p.z);
      if (!it || typeof it !== 'object' || typeof it.name !== 'string' || it.name.length > 60 || !it.st || typeof it.st !== 'object') return;
      if (JSON.stringify(it).length > 4000 || dist(x, z, p.x, p.z) > 12) return;
      if (drops.size >= 300) return sys(p, 'There are too many items on the ground.');
      let mine = 0; drops.forEach(d => { if (d.o === p.id) mine++ }); if (mine >= 12) return sys(p, 'You have dropped too many items. Wait for them to be picked up.');
      const id = nextDrop++, rec = { id, item: it, x, z, by: p.name, t: Date.now(), o: p.id };
      drops.set(id, rec); sendAll({ t: 'dropadd', id, item: it, x, z, by: p.name }); return;
    }
    case 'pick': {
      const id = m.id | 0, d = drops.get(id); if (!d || dist(p.x, p.z, d.x, d.z) > 9) return;
      drops.delete(id); p.c.send({ t: 'pickok', item: d.item }); sendAll({ t: 'dropdel', id }); return;
    }
    case 'portal': {
      // a mage's Town Portal: tell everyone nearby so they can see it and step through (clients time it out after 15s)
      const now = Date.now(); if (now - (p.portalT || 0) < 8000) return sys(p, 'Your portal spell is not ready yet.');
      const x = num(m.x, p.x), z = num(m.z, p.z); if (dist(x, z, p.x, p.z) > 12) return;
      let zi = m.zi | 0; if (zi < 0 || zi > 10) zi = 2;
      p.portalT = now; players.forEach(q => { if (q !== p && dist(q.x, q.z, x, z) < 450) q.c.send({ t: 'portal', x, z, ttl: 15, from: p.name, zi }) }); return;
    }
    case 'aura': {
      // a paladin's aura: relay it to party members standing nearby
      const now = Date.now(); if (now - (p.auraT || 0) < 3000) return; p.auraT = now;
      const k = m.k | 0, d = Math.min(60, num(m.d)); if (k < 1 || k > 4) return;
      const pt = partyOf(p); if (!pt) return;
      pt.members.forEach(id => { const q = players.get(id); if (q && q !== p && dist(q.x, q.z, p.x, p.z) < 120) q.c.send({ t: 'aura', k, d, from: p.name }) }); return;
    }
    case 'mc': {
      // a mage's Mind Control: the target and the nearest creature fight each other
      const now = Date.now(); if (now - (p.mcAt || 0) < 3000) return; p.mcAt = now;
      const a = mobById.get(m.a | 0); if (!a || a.dead || (a.win && !a.active) || dist(p.x, p.z, a.x, a.z) > 60) return;
      let b = mobById.get(m.b | 0);
      if (!b || b.dead || b === a || (b.win && !b.active) || dist(a.x, a.z, b.x, b.z) > 45) { b = null; let bd = 40; for (const q of mobs) { if (q === a || q.dead || (q.win && !q.active)) continue; const d = dist(q.x, q.z, a.x, a.z); if (d < bd) { bd = d; b = q } } }
      if (!b) return;
      const sec = Math.max(1, Math.min(8, num(m.s) || 8));
      [[a, b], [b, a]].forEach(([x, y]) => { x.mcT = sec; x.mcTarget = y.id; x.mcBy = p.id });
      return;
    }
    case 'adm': {
      // admin mode for this connection only: lets level jumps be saved. Everyone in chat is told.
      const now = Date.now(); if (now - (c.admAt || 0) < 1500) return; c.admAt = now;
      const code = String(m.code == null ? '' : m.code).slice(0, 64);
      if (code === '0') { c.adm = false; return c.send({ t: 'adm', ok: 0, off: 1 }) }
      if (code === ADMIN_CODE) { c.adm = true; console.log('[admin] ' + (p ? p.name : c.user) + ' turned on admin mode'); if (p) sendAll({ t: 'sys', m: '⚠ ' + p.name + ' turned on admin mode.' }); return c.send({ t: 'adm', ok: 1 }) }
      return c.send({ t: 'adm', ok: 0, m: 'Wrong code.' });
    }
    case 'ping': return c.send({ t: 'pong', n: m.n });
  }
}

function dropPlayer(c) {
  const p = c.player; if (!p) return;
  leaveParty(p, '');
  players.delete(p.id); if (byName.get(p.name.toLowerCase()) === p) byName.delete(p.name.toLowerCase());
  mobs.forEach(m => { if (m.owner === p.id) { m.owner = 0; if (!m.party) { m.st = 2 } } if (m.target === p.id) m.target = 0 });
  sendAll({ t: 'pq', id: p.id }); sendAll({ t: 'sys', m: p.name + ' left the world.' });
  console.log(p.name, 'left (' + players.size + ' online)'); c.player = null;
}

// ---------------------------------------------------------------- http
let htmlGz = null, htmlTime = 0;
function getHtml() {
  const st = fs.statSync(HTML_PATH);
  if (!htmlGz || st.mtimeMs !== htmlTime) { htmlGz = zlib.gzipSync(fs.readFileSync(HTML_PATH)); htmlTime = st.mtimeMs }
  return htmlGz;
}
const server = http.createServer((req, res) => {
  const u = req.url.split('?')[0];
  if (u === '/' || u === '/index.html' || u === '/game.html') {
    try {
      const ae = String(req.headers['accept-encoding'] || ''); const gz = getHtml();
      const h = { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' };
      if (ae.includes('gzip')) { h['Content-Encoding'] = 'gzip'; res.writeHead(200, h); res.end(gz) } else { res.writeHead(200, h); res.end(zlib.gunzipSync(gz)) }
    } catch (e) { res.writeHead(500); res.end('game.html is missing next to server.js') }
  } else if (u === '/health') { res.writeHead(200, { 'Content-Type': 'application/json' }); res.end(JSON.stringify({ ok: 1, proto: PROTO, players: players.size, max: MAXP, mobs: mobs.length, up: Math.round(process.uptime()) })) }
  else { res.writeHead(404); res.end('Not found') }
});
server.on('upgrade', (req, sock) => {
  if (req.url.split('?')[0] !== '/ws') return sock.destroy();
  const key = req.headers['sec-websocket-key']; if (!key) return sock.destroy();
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' +
    crypto.createHash('sha1').update(key + GUID).digest('base64') + '\r\n\r\n');
  const c = new Conn(sock);
  c.ip = String(req.headers['x-forwarded-for'] || sock.remoteAddress || '').split(',')[0].trim();
  c.onmsg = m => handle(c, m);
  c.onclose = () => { dropPlayer(c); if (c.user && online.get(c.user) === c) online.delete(c.user) };
});
server.listen(PORT, () => {
  console.log('\nWhisperwood server (protocol ' + PROTO + ') running.  Open  http://localhost:' + PORT + '  in your browser.');
  console.log('Players max: ' + MAXP + (INVITE ? '  (invite code required)' : '') + '   data: ' + DATA + '\n');
});
