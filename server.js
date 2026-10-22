const http = require("http");
const { Server } = require("socket.io");

const server = http.createServer();
const io = new Server(server, {
  cors: { origin: "*" }
});

const rooms = new Map();

io.on("connection", (socket) => {
  console.log("Player connected:", socket.id);

  socket.on("createRoom", () => {
    const code = Math.random().toString(36).slice(2, 7).toUpperCase();
    rooms.set(code, new Set([socket.id]));
    socket.join(code);
    socket.emit("roomCreated", code);
  });

  socket.on("joinRoom", (roomCode) => {
    const code = String(roomCode || "").trim().toUpperCase();
    const players = rooms.get(code);

    if (!players) {
      socket.emit("roomError", "Room not found");
      return;
    }

    players.add(socket.id);
    socket.join(code);
    io.to(code).emit("players", [...players]);
  });

  socket.on("disconnect", () => {
    for (const [code, players] of rooms) {
      players.delete(socket.id);
      if (players.size === 0) rooms.delete(code);
      else io.to(code).emit("players", [...players]);
    }
  });
});

server.listen(process.env.PORT || 3000, "0.0.0.0", () => {
  console.log("Multiplayer server running");
});
