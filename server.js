
const http = require('http');
const { Server } = require('socket.io');

const server = http.createServer();

const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

const MAX_PLAYERS = 2;
const rooms = new Map();
const randomQueue = [];

function makeCode() {
  let code;

  do {
    code = Math.random().toString(36).slice(2, 7).toUpperCase();
  } while (rooms.has(code));

  return code;
}

function queueRemove(id) {
  const index = randomQueue.indexOf(id);
  if (index !== -1) randomQueue.splice(index, 1);
}

function emitPlayers(code) {
  const room = rooms.get(code);
  if (room) {
    io.to(code).emit('players', [...room.players]);
  }
}

// RANDOM MATCH
async function makeRandomMatch(firstId, secondId) {
  const first = io.sockets.sockets.get(firstId);
  const second = io.sockets.sockets.get(secondId);

  if (!first || !second || firstId === secondId) return false;
  if (first.data.roomCode || second.data.roomCode) return false;

  const code = makeCode();

  const room = {
    host: firstId,
    players: new Set([firstId, secondId]),
    mode: 'random'
  };

  rooms.set(code, room);

  first.data.roomCode = code;
  second.data.roomCode = code;

  await Promise.all([
    first.join(code),
    second.join(code)
  ]);

  emitPlayers(code);

  first.emit('randomMatchFound', {
    roomCode: code,
    players: 2
  });

  second.emit('randomMatchFound', {
    roomCode: code,
    players: 2
  });

  setTimeout(() => {
    const currentRoom = rooms.get(code);

    if (currentRoom && currentRoom.players.size === 2) {
      io.to(code).emit('matchStart', {
        roomCode: code
      });
    }
  }, 900);

  console.log('Random match created:', code);

  return true;
}

async function matchQueue() {
  while (randomQueue.length >= 2) {
    const firstId = randomQueue.shift();
    const secondId = randomQueue.shift();

    const success = await makeRandomMatch(firstId, secondId);

    if (!success) {
      const first = io.sockets.sockets.get(firstId);
      const second = io.sockets.sockets.get(secondId);

      if (first && !first.data.roomCode) {
        randomQueue.unshift(firstId);
      }

      if (second && !second.data.roomCode) {
        randomQueue.push(secondId);
      }

      if (randomQueue.length < 2) break;
    }
  }
}

io.on('connection', (socket) => {
  console.log('Player connected:', socket.id);

  // CREATE ROOM
  socket.on('createRoom', async () => {
    if (socket.data.roomCode) {
      return socket.emit('roomError', 'Already in a room');
    }

    queueRemove(socket.id);

    const code = makeCode();

    rooms.set(code, {
      host: socket.id,
      players: new Set([socket.id]),
      mode: 'private'
    });

    socket.data.roomCode = code;

    await socket.join(code);

    socket.emit('roomCreated', code);
    emitPlayers(code);

    console.log('Room created:', code);
  });

  // JOIN ROOM
  socket.on('joinRoom', async (roomCode) => {
    const code = String(roomCode || '')
      .trim()
      .toUpperCase();

    const room = rooms.get(code);

    if (!room) {
      return socket.emit('roomError', 'Room not found');
    }

    if (socket.data.roomCode) {
      return socket.emit('roomError', 'Already in a room');
    }

    if (room.players.size >= MAX_PLAYERS) {
      return socket.emit('roomError', 'Room is full');
    }

    queueRemove(socket.id);

    room.players.add(socket.id);
    socket.data.roomCode = code;

    await socket.join(code);

    socket.emit('joinedRoom', code);
    emitPlayers(code);

    console.log('Player joined:', code);
  });

  // RANDOM MATCH
  socket.on('randomMatch', () => {
    if (socket.data.roomCode) {
      return socket.emit('roomError', 'Leave your current room first');
    }

    if (randomQueue.includes(socket.id)) {
      return socket.emit('randomMatchWaiting', {
        message: 'Searching for another player...'
      });
    }

    randomQueue.push(socket.id);

    socket.emit('randomMatchWaiting', {
      message: 'Searching for another player...'
    });

    console.log('Random queue:', randomQueue.length);

    matchQueue();
  });

  // CANCEL RANDOM MATCH
  socket.on('cancelRandomMatch', () => {
    queueRemove(socket.id);

    socket.emit('randomMatchCancelled');
  });

  // GAME EVENT RELAY
  socket.on('gameEvent', (data) => {
    if (!data || typeof data !== 'object') return;

    const code = String(data.roomCode || '')
      .trim()
      .toUpperCase();

    const room = rooms.get(code);

    if (!room || !room.players.has(socket.id)) return;

    io.to(code).emit('gameEvent', {
      ...data,
      roomCode: code,
      from: socket.id
    });
  });

  // DISCONNECT
  socket.on('disconnect', () => {
    queueRemove(socket.id);

    const code = socket.data.roomCode;

    if (code) {
      const room = rooms.get(code);

      if (room) {
        room.players.delete(socket.id);

        if (room.players.size === 0) {
          rooms.delete(code);
        } else {
          io.to(code).emit('players', [...room.players]);

          io.to(code).emit('playerLeft', {
            socketId: socket.id
          });

          rooms.delete(code);

          for (const memberId of room.players) {
            const member = io.sockets.sockets.get(memberId);

            if (member) {
              member.data.roomCode = null;
              member.leave(code);

              member.emit(
                'roomError',
                'Other player disconnected. Room closed.'
              );
            }
          }
        }
      }
    }

    console.log('Player disconnected:', socket.id);
  });
});

server.listen(
  process.env.PORT || 3000,
  '0.0.0.0',
  () => {
    console.log('Multiplayer server running');
  }
);
