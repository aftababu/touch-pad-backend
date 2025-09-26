import express from 'express';
import http from 'http';
import path from 'path';
import { Server } from 'socket.io';
import cors from 'cors';
import fs from 'fs';

const app = express();
app.use(cors());
app.use(express.json({ limit: '2mb' }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

// In-memory room store. No database.
const rooms = new Map();
const MAX_STROKES_PER_ROOM = 5000;
const MAX_ROOMS = 2000;

function getOrCreateRoom(roomId) {
  if (!rooms.has(roomId)) {
    if (rooms.size >= MAX_ROOMS) {
      const oldestKey = rooms.keys().next().value;
      rooms.delete(oldestKey);
    }
    rooms.set(roomId, { strokes: [], updatedAt: Date.now() });
  }
  return rooms.get(roomId);
}

io.on('connection', (socket) => {
  socket.on('room:join', ({ roomId, role }) => {
    if (!roomId || typeof roomId !== 'string') return;
    socket.join(roomId);
    const room = getOrCreateRoom(roomId);
    room.updatedAt = Date.now();
    socket.emit('room:init', { strokes: room.strokes });
    socket.to(roomId).emit('room:user:joined', { userId: socket.id, role: role || 'receiver' });
  });

  // stroke types: 'draw' (default), 'erase', 'text'
  socket.on('stroke:commit', ({ roomId, stroke }) => {
    if (!roomId || !stroke) return;
    const room = getOrCreateRoom(roomId);

    if (stroke.type === 'text') {
      const txt = (stroke.text || '').toString();
      if (!txt || txt.length > 1000) return;
      const x = Number(stroke.x), y = Number(stroke.y);
      if (!Number.isFinite(x) || !Number.isFinite(y)) return;
      const color = stroke.color || '#111';
      const size = Math.max(8, Math.min(96, Number(stroke.size) || 24));
      const payload = { type: 'text', x, y, text: txt, color, size, font: stroke.font || 'sans-serif', ts: Date.now() };
      room.strokes.push(payload);
      if (room.strokes.length > MAX_STROKES_PER_ROOM) {
        room.strokes.splice(0, room.strokes.length - MAX_STROKES_PER_ROOM);
      }
      room.updatedAt = Date.now();
      socket.to(roomId).emit('stroke:commit', { stroke: payload });
      return;
    }

    // draw/erase
    if (!Array.isArray(stroke.points) || stroke.points.length < 2 || stroke.points.length > 5000) return;
    const size = Math.max(1, Math.min(64, Number(stroke.size) || 4));
    const mode = stroke.type === 'erase' ? 'erase' : 'draw';
    const color = stroke.color || '#111';
    const payload = { type: mode, color, size, points: stroke.points, ts: Date.now() };
    room.strokes.push(payload);
    if (room.strokes.length > MAX_STROKES_PER_ROOM) {
      room.strokes.splice(0, room.strokes.length - MAX_STROKES_PER_ROOM);
    }
    room.updatedAt = Date.now();
    socket.to(roomId).emit('stroke:commit', { stroke: payload });
  });

  socket.on('stroke:undo', ({ roomId }) => {
    if (!roomId) return;
    const room = getOrCreateRoom(roomId);
    if (room.strokes.length === 0) return;
    room.strokes.pop();
    room.updatedAt = Date.now();
    io.to(roomId).emit('room:undo');
  });

  socket.on('room:clear', ({ roomId }) => {
    if (!roomId) return;
    const room = getOrCreateRoom(roomId);
    room.strokes = [];
    room.updatedAt = Date.now();
    io.to(roomId).emit('room:cleared');
  });

  socket.on('disconnect', () => {});
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

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => {
  console.log(`Server listening on http://localhost:${PORT}`);
});