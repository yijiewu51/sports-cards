const express = require('express');
const { createClient } = require('@libsql/client');
const path = require('path');
const { SEED_DATA } = require('./seed');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ---------- Database ----------
const db = createClient({
  url: process.env.TURSO_URL || 'file:cards.db',
  authToken: process.env.TURSO_AUTH_TOKEN,
});

async function initDB() {
  await db.execute(`CREATE TABLE IF NOT EXISTS cards (
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

  try { await db.execute('ALTER TABLE cards ADD COLUMN settled INTEGER DEFAULT 0'); } catch(e) {}

  const result = await db.execute('SELECT COUNT(*) as c FROM cards');
  const count = result.rows[0].c;
  if (count === 0) {
    const stmts = SEED_DATA.map(c => ({
      sql: 'INSERT INTO cards (id,player,series,year,rc,sign,mat,number,buyer,source,cost_cny,sold_price,status,note) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
      args: [c.id, c.player||'', c.series||'', c.year||'', c.rc?1:0, c.sign?1:0, c.mat?1:0, c.number||'', c.buyer||'', c.source||'', c.cost_cny||0, c.sold_price??null, c.status||'持有', c.note||'']
    }));
    await db.batch(stmts);
    console.log(`Seeded ${SEED_DATA.length} cards`);
  }
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

app.get('/api/cards', async (req, res) => {
  const result = await db.execute('SELECT * FROM cards ORDER BY id');
  res.json(result.rows.map(rowToCard));
});

app.post('/api/cards', async (req, res) => {
  const c = req.body;
  await db.execute({
    sql: 'INSERT INTO cards (id,player,series,year,rc,sign,mat,number,buyer,source,cost_cny,sold_price,status,note) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
    args: [c.id, c.player||'', c.series||'', c.year||'', c.rc?1:0, c.sign?1:0, c.mat?1:0, c.number||'', c.buyer||'', c.source||'', c.cost_cny||0, c.sold_price??null, c.status||'持有', c.note||'']
  });
  broadcast();
  res.json({ ok: true });
});

app.put('/api/cards/:id', async (req, res) => {
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
  await db.execute({ sql: `UPDATE cards SET ${sets.join(', ')} WHERE id = ?`, args: vals });
  broadcast();
  res.json({ ok: true });
});

app.delete('/api/cards/:id', async (req, res) => {
  await db.execute({ sql: 'DELETE FROM cards WHERE id = ?', args: [req.params.id] });
  broadcast();
  res.json({ ok: true });
});

// ---------- Start ----------
const PORT = process.env.PORT || 3000;
initDB().then(() => {
  app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
}).catch(err => {
  console.error('Failed to init DB:', err);
  process.exit(1);
});
