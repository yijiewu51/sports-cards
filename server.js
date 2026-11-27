const express = require('express');
const Database = require('better-sqlite3');
const path = require('path');
const { SEED_DATA } = require('./seed');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ---------- Database ----------
const db = new Database('./cards.db');
db.pragma('journal_mode = WAL');

db.exec(`CREATE TABLE IF NOT EXISTS cards (
  id TEXT PRIMARY KEY,
  player TEXT NOT NULL DEFAULT '',
  series TEXT DEFAULT '',
  year TEXT DEFAULT '',
  rc INTEGER DEFAULT 0,
  sign INTEGER DEFAULT 0,
  mat INTEGER DEFAULT 0,
  number TEXT DEFAULT '',
  buyer TEXT DEFAULT '',
  source TEXT DEFAULT '',
  cost_cny REAL DEFAULT 0,
  sold_price REAL,
  status TEXT DEFAULT '持有',
  note TEXT DEFAULT '',
  settled INTEGER DEFAULT 0,
  created_at TEXT DEFAULT (datetime('now'))
)`);

// Add settled column if missing (for existing databases)
try { db.exec('ALTER TABLE cards ADD COLUMN settled INTEGER DEFAULT 0'); } catch(e) {}

// Seed if empty
const count = db.prepare('SELECT COUNT(*) as c FROM cards').get().c;
if (count === 0) {
  const insert = db.prepare(`INSERT INTO cards (id,player,series,year,rc,sign,mat,number,buyer,source,cost_cny,sold_price,status,note) VALUES (@id,@player,@series,@year,@rc,@sign,@mat,@number,@buyer,@source,@cost_cny,@sold_price,@status,@note)`);
  const tx = db.transaction((rows) => { for (const r of rows) insert.run(r); });
  tx(SEED_DATA.map(c => ({
    ...c,
    rc: c.rc ? 1 : 0,
    sign: c.sign ? 1 : 0,
    mat: c.mat ? 1 : 0,
  })));
  console.log(`Seeded ${SEED_DATA.length} cards`);
}

// ---------- SSE for real-time sync ----------
let clients = [];

function broadcast() {
  clients.forEach(c => c.res.write('data: update\n\n'));
}

app.get('/api/events', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.write('data: connected\n\n');
  const client = { id: Date.now(), res };
  clients.push(client);
  req.on('close', () => { clients = clients.filter(c => c.id !== client.id); });
});

// ---------- API ----------
function rowToCard(r) {
  return { ...r, rc: !!r.rc, sign: !!r.sign, mat: !!r.mat, settled: !!r.settled };
}

app.get('/api/cards', (req, res) => {
  const rows = db.prepare('SELECT * FROM cards ORDER BY id').all();
  res.json(rows.map(rowToCard));
});

app.post('/api/cards', (req, res) => {
  const c = req.body;
  db.prepare(`INSERT INTO cards (id,player,series,year,rc,sign,mat,number,buyer,source,cost_cny,sold_price,status,note) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
    .run(c.id, c.player||'', c.series||'', c.year||'', c.rc?1:0, c.sign?1:0, c.mat?1:0, c.number||'', c.buyer||'', c.source||'', c.cost_cny||0, c.sold_price??null, c.status||'持有', c.note||'');
  broadcast();
  res.json({ ok: true });
});

app.put('/api/cards/:id', (req, res) => {
  const fields = req.body;
  const sets = [];
  const vals = [];
  for (const [k, v] of Object.entries(fields)) {
    if (k === 'id' || k === 'created_at') continue;
    sets.push(`${k} = ?`);
    if (k === 'rc' || k === 'sign' || k === 'mat' || k === 'settled') vals.push(v ? 1 : 0);
    else vals.push(v);
  }
  if (sets.length === 0) return res.json({ ok: true });
  vals.push(req.params.id);
  db.prepare(`UPDATE cards SET ${sets.join(', ')} WHERE id = ?`).run(...vals);
  broadcast();
  res.json({ ok: true });
});

app.delete('/api/cards/:id', (req, res) => {
  db.prepare('DELETE FROM cards WHERE id = ?').run(req.params.id);
  broadcast();
  res.json({ ok: true });
});

// ---------- Start ----------
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
