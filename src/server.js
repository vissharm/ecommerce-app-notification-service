const express = require('express');
const mongoose = require('mongoose');
const dotenv = require('dotenv');
const notificationRoutes = require('./routes/notificationRoutes');
const { KafkaClient, Consumer, Producer } = require('kafka-node');
const http = require('http');
const { initSocket } = require('./socket'); // Import initSocket function
const cors = require('cors'); // Import CORS middleware

dotenv.config();

const app = express();
const server = http.createServer(app);

// CORS configuration
app.use(cors({
  origin: "http://localhost:3000",
  methods: ["GET", "POST"],
  credentials: true
}));

app.use(express.json());
app.use('/api/notifications', notificationRoutes);

// Initialize Socket.IO
initSocket(server);

mongoose.connect(process.env.MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true })
  .then(() => console.log('MongoDB connected'))
  .catch(err => console.log(err));


const kafkaClient = new KafkaClient({ kafkaHost: process.env.KAFKA_BROKER });
const producer = new Producer(kafkaClient);

producer.on('ready', () => {
  console.log('Kafka Producer is connected and ready.');
  producer.createTopics(['order-created', 'user-registered'], false, (err, data) => {
    if (err) console.error('Error creating Kafka topics:', err);
    else console.log('Kafka topics created:', data);
  });
});

producer.on('error', (err) => {
  console.error('Kafka Producer error:', err);
});

server.listen(process.env.PORT, () => {
  console.log(`Notification service running on port ${process.env.PORT}`);
});
