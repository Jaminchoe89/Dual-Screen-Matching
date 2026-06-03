const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

app.use(express.static(path.join(__dirname, 'public')));

app.get('/logos.svg', (req, res) => {
  const p = path.join(__dirname, 'public', 'logos.svg');
  if (fs.existsSync(p)) return res.sendFile(p);
  res.sendFile(path.join(__dirname, 'Logos.svg'));
});

app.get('/play',    (req, res) => res.sendFile(path.join(__dirname, 'public', 'player.html')));
app.get('/display', (req, res) => res.sendFile(path.join(__dirname, 'public', 'display.html')));

// ── Database ──────────────────────────────────────────────────────────────────

let pool = null;

if (process.env.DATABASE_URL) {
  const { Pool } = require('pg');
  pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false },
  });
  console.log('PostgreSQL enabled');
} else {
  console.log('No DATABASE_URL — using in-memory leaderboard');
}

async function initDB() {
  if (!pool) return;
  await pool.query(`
    CREATE TABLE IF NOT EXISTS scores (
      id        SERIAL PRIMARY KEY,
      name      TEXT    NOT NULL,
      time_ms   INTEGER NOT NULL,
      time_str  TEXT    NOT NULL,
      played_at TIMESTAMP DEFAULT NOW()
    )
  `);
}

async function dbLoadLeaderboard() {
  if (!pool) return null;
  const { rows } = await pool.query(`
    SELECT name, time_ms AS time, time_str AS "timeStr"
    FROM scores
    ORDER BY time_ms ASC
    LIMIT 3
  `);
  return rows;
}

async function dbInsertScore(name, timeMs, timeStr) {
  if (!pool) return null;
  await pool.query(
    `INSERT INTO scores (name, time_ms, time_str) VALUES ($1, $2, $3)`,
    [name, timeMs, timeStr]
  );
  const { rows } = await pool.query(`
    SELECT name, time_ms AS time, time_str AS "timeStr"
    FROM scores
    ORDER BY time_ms ASC
    LIMIT 3
  `);
  return rows;
}

// ── Constants ─────────────────────────────────────────────────────────────────

const GAME_TIME_LIMIT = 90000; // 1 min 30 sec

// ── Game state ────────────────────────────────────────────────────────────────

let timeoutHandle = null;

let state = {
  status: 'idle',   // 'idle' | 'playing' | 'finished' | 'timeout'
  cards: [],
  flippedCards: [],
  startTime: null,
  pendingScore: null,
  qualifies: false,
  leaderboard: [],
};

function shuffle(arr) {
  const a = [...arr];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function createCards() {
  const pairs = shuffle([0,1,2,3,4,5,6,7,0,1,2,3,4,5,6,7]);
  return pairs.map((logoIndex, id) => ({ id, logoIndex, flipped: false, matched: false }));
}

function fmtTime(ms) {
  const s = Math.floor(ms / 1000);
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`;
}

function qualifiesForLeaderboard(elapsed) {
  if (state.leaderboard.length < 3) return true;
  return elapsed < state.leaderboard[2].time;
}

// In-memory fallback: insert + return new top 3
function memInsertScore(name, timeMs, timeStr) {
  return [...state.leaderboard, { name, time: timeMs, timeStr }]
    .sort((a, b) => a.time - b.time)
    .slice(0, 3);
}

// ── Socket handlers ───────────────────────────────────────────────────────────

io.on('connection', (socket) => {
  socket.emit('state:sync', state);

  socket.on('game:start', () => {
    clearTimeout(timeoutHandle);
    state = {
      ...state,
      status: 'playing',
      cards: createCards(),
      flippedCards: [],
      startTime: Date.now(),
      pendingScore: null,
      qualifies: false,
    };
    io.emit('state:sync', state);

    timeoutHandle = setTimeout(() => {
      if (state.status !== 'playing') return;
      state = { ...state, status: 'timeout' };
      io.emit('state:sync', state);
    }, GAME_TIME_LIMIT);
  });

  socket.on('card:flip', ({ cardId }) => {
    if (state.status !== 'playing') return;
    if (state.flippedCards.length >= 2) return;

    const card = state.cards[cardId];
    if (!card || card.flipped || card.matched) return;

    card.flipped = true;
    state.flippedCards = [...state.flippedCards, cardId];
    io.emit('state:sync', state);

    if (state.flippedCards.length === 2) {
      const [id1, id2] = state.flippedCards;
      const c1 = state.cards[id1];
      const c2 = state.cards[id2];

      if (c1.logoIndex === c2.logoIndex) {
        setTimeout(() => {
          c1.matched = true;
          c2.matched = true;
          state.flippedCards = [];

          if (state.cards.every(c => c.matched)) {
            clearTimeout(timeoutHandle);
            const elapsed = Date.now() - state.startTime;
            state.status = 'finished';
            state.pendingScore = elapsed;
            state.qualifies = qualifiesForLeaderboard(elapsed);
          }

          io.emit('state:sync', state);
        }, 400);
      } else {
        setTimeout(() => {
          c1.flipped = false;
          c2.flipped = false;
          state.flippedCards = [];
          io.emit('state:sync', state);
        }, 700);
      }
    }
  });

  socket.on('game:submit_name', async ({ name }) => {
    if (state.status !== 'finished' || !state.qualifies || state.pendingScore === null) return;
    const trimmed = String(name).trim().slice(0, 24);
    if (!trimmed) return;

    let leaderboard;
    try {
      leaderboard = await dbInsertScore(trimmed, state.pendingScore, fmtTime(state.pendingScore));
    } catch (err) {
      console.error('DB insert failed, using in-memory fallback:', err.message);
    }

    // Fall back to in-memory if DB unavailable
    if (!leaderboard) {
      leaderboard = memInsertScore(trimmed, state.pendingScore, fmtTime(state.pendingScore));
    }

    state = {
      ...state,
      status: 'idle',
      cards: [],
      flippedCards: [],
      startTime: null,
      leaderboard,
      pendingScore: null,
      qualifies: false,
    };
    io.emit('state:sync', state);
  });

  socket.on('game:reset', () => {
    clearTimeout(timeoutHandle);
    state = { ...state, status: 'idle', cards: [], flippedCards: [], startTime: null, pendingScore: null, qualifies: false };
    io.emit('state:sync', state);
  });
});

// ── Start ─────────────────────────────────────────────────────────────────────

const PORT = process.env.PORT || 3000;

async function start() {
  try {
    await initDB();
    const saved = await dbLoadLeaderboard();
    if (saved) {
      state.leaderboard = saved;
      console.log(`Leaderboard restored: ${saved.length} entries`);
    }
  } catch (err) {
    console.error('DB init failed (starting with empty leaderboard):', err.message);
  }

  server.listen(PORT, () => {
    console.log(`\n  COMPASS Card Match running`);
    console.log(`  Player:  http://localhost:${PORT}/play`);
    console.log(`  Display: http://localhost:${PORT}/display\n`);
  });
}

start();
