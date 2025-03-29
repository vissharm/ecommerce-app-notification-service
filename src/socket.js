const socketIo = require('socket.io');

let io;

const initSocket = (server) => {
  io = socketIo(server, {
    transports: ['websocket'],  // WebSocket only, no polling
    allowUpgrades: false,  // Prevent transport upgrades
    cors: {
      origin: "http://localhost:3000",
      methods: ["GET", "POST"]
    }
  });
  
  io.on('connection', (socket) => {
    console.log('A client connected');

    // Emit an event to the client
    socket.emit('message', 'Connected to WebSocket server');

    socket.on('disconnect', () => {
      console.log('A client disconnected');
    });
  });

  return io;
};

const getIo = () => {
  if (!io) {
    throw new Error('Socket.io not initialized');
  }
  return io;
};

module.exports = { initSocket, getIo };
