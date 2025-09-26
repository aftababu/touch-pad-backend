import express from 'express';
import http from 'http';
import path from 'path';
import fs from 'fs';
import cors from 'cors';
import { Server } from 'socket.io';
import { createClient } from 'redis';
import { createAdapter } from '@socket.io/redis-adapter';

// ---------- Config ----------
const PORT = process.env.PORT || 4000;

// Use your Redis Cloud credentials by default; can be overridden via env
const REDIS_HOST = process.env.REDIS_HOST || 'redis-16267.c330.asia-south1-1.gce.redns.redis-cloud.com';
const REDIS_PORT = Number(process.env.REDIS_PORT || 16267);
const REDIS_USERNAME = process.env.REDIS_USERNAME || 'default';
const REDIS_PASSWORD = process.env.REDIS_PASSWORD || 'iKCs1LSpS6W98BpVKyuNcRm2NtHCVc7m';
// Redis Cloud typically requires TLS; set REDIS_TLS=false to disable
const REDIS_TLS = (process.env.REDIS_TLS ?? 'true').toLowerCase() !== 'false';

const MAX_STROKES_PER_ROOM = parseInt(process.env.MAX_STROKES_PER_ROOM || '5000', 10);
const ROOM_TTL_SECONDS = parseInt(process.env.ROOM_TTL_SECONDS || '86400', 10); // 24h

// ---------- Express ----------
const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));

const server = http.createServer(app);

// ---------- Socket.IO ----------
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket'],
  perMessageDeflate: { threshold: 1024 },
  connectionStateRecovery: {}
});

// ---------- Redis (Adapter + Storage) ----------
const pubClient = createClient({
  username: REDIS_USERNAME,
  password: REDIS_PASSWORD,
  socket: {
    host: REDIS_HOST,
    port: REDIS_PORT,
    tls: REDIS_TLS ? {} : undefined  
  }
});

const subClient = pubClient.duplicate();

pubClient.on('error', (err) => console.error('Redis Pub Client Error:', err));
subClient.on('error', (err) => console.error('Redis Sub Client Error:', err));

function roomKey(roomId) {
  return `wb:${roomId}:strokes`;
}

async function roomPushStroke(roomId, stroke) {
  const key = roomKey(roomId);
  const payload = JSON.stringify(quantizeStroke(stroke));
  await pubClient
    .multi()
    .rPush(key, payload)
    .lTrim(key, -MAX_STROKES_PER_ROOM, -1)
    .expire(key, ROOM_TTL_SECONDS)
    .exec();
}

async function roomGetStrokes(roomId) {
  const key = roomKey(roomId);
  const arr = await pubClient.lRange(key, 0, -1);
  return arr.map((s) => {
    try { return JSON.parse(s); } catch { return null; }
  }).filter(Boolean);
}

async function roomPopStroke(roomId) {
  const key = roomKey(roomId);
  await pubClient.rPop(key);
}

async function roomClear(roomId) {
  const key = roomKey(roomId);
  await pubClient.del(key);
}

// Reduce payload size without visible quality loss
function quantizeStroke(stroke) {
  if (stroke?.points && Array.isArray(stroke.points)) {
    return {
      ...stroke,
      points: stroke.points.map((p) => ({
        x: Math.round(p.x * 10) / 10, // 0.1 px precision
        y: Math.round(p.y * 10) / 10,
        t: Math.round(p.t || 0)
      }))
    };
  }
  return stroke;
}

async function start() {
  // Connect Redis and attach Socket.IO adapter
  await Promise.all([pubClient.connect(), subClient.connect()]);
  io.adapter(createAdapter(pubClient, subClient));
  console.log(`Connected to Redis at ${REDIS_HOST}:${REDIS_PORT} (tls=${REDIS_TLS})`);

  io.on('connection', (socket) => {
    socket.on('room:join', async ({ roomId, role }) => {
      if (!roomId || typeof roomId !== 'string') return;
      socket.join(roomId);
      const strokes = await roomGetStrokes(roomId);
      socket.emit('room:init', { strokes });
      socket.to(roomId).emit('room:user:joined', { userId: socket.id, role: role || 'receiver' });
    });

    // stroke types: 'draw', 'erase', 'text'
    socket.on('stroke:commit', async ({ roomId, stroke }) => {
      if (!roomId || !stroke) return;

      if (stroke.type === 'text') {
        const txt = (stroke.text || '').toString();
        if (!txt || txt.length > 2000) return;
        const x = Number(stroke.x), y = Number(stroke.y);
        if (!Number.isFinite(x) || !Number.isFinite(y)) return;
        const payload = {
          type: 'text',
          x, y,
          text: txt,
          color: stroke.color || '#111',
          size: Math.max(8, Math.min(120, Number(stroke.size) || 24)),
          font: stroke.font || 'sans-serif',
          ts: Date.now()
        };
        await roomPushStroke(roomId, payload);
        socket.to(roomId).emit('stroke:commit', { stroke: payload });
        return;
      }

      if (!Array.isArray(stroke.points) || stroke.points.length < 2 || stroke.points.length > 8000) return;
      const size = Math.max(1, Math.min(64, Number(stroke.size) || 4));
      const mode = stroke.type === 'erase' ? 'erase' : 'draw';
      const payload = {
        type: mode,
        color: stroke.color || '#111',
        size,
        points: stroke.points,
        ts: Date.now()
      };
      await roomPushStroke(roomId, payload);
      socket.to(roomId).emit('stroke:commit', { stroke: payload });
    });

    socket.on('stroke:undo', async ({ roomId }) => {
      if (!roomId) return;
      await roomPopStroke(roomId);
      io.to(roomId).emit('room:undo');
    });

    socket.on('room:clear', async ({ roomId }) => {
      if (!roomId) return;
      await roomClear(roomId);
      io.to(roomId).emit('room:cleared');
    });
  });

  // Serve client if built
  const __dirname = path.resolve();
  const clientDist = path.join(__dirname, 'client', 'dist');
  if (fs.existsSync(clientDist)) {
    app.use(express.static(clientDist));
    app.get('*', (_req, res) => {
      res.sendFile(path.join(clientDist, 'index.html'));
    });
  }

  app.get('/health', (_req, res) => res.json({ ok: true }));

  server.listen(PORT, () => {
    console.log(`Server listening on http://localhost:${PORT}`);
  });
}

start().catch((err) => {
  console.error('Failed to start server:', err);
  process.exit(1);
});

// Graceful shutdown
process.on('SIGINT', async () => {
  try { await pubClient.quit(); } catch {}
  try { await subClient.quit(); } catch {}
  process.exit(0);
});