const express = require('express');
const Notification = require('../models/Notification');
const router = express.Router();
const kafka = require('kafka-node');
const mongoose = require('mongoose');
const { getIo } = require('../socket');
const auth = require('../../../shared/middleware/auth');

// Create a separate connection to the order-service database
const orderServiceConnection = mongoose.createConnection(process.env.ORDER_SERVICE_MONGO_URI || 'mongodb://127.0.0.1:27017/order-service', {
  useNewUrlParser: true,
  useUnifiedTopology: true
});

// Define Order Schema on the order-service connection
const Order = orderServiceConnection.model('Order', new mongoose.Schema({
  userId: String,
  productId: String,
  quantity: Number,
  status: String,
  orderDate: { type: Date, default: Date.now },
  lastUpdated: { type: Date, default: Date.now }
}));

// Kafka configuration
const client = new kafka.KafkaClient({ 
  kafkaHost: 'localhost:9092',
  connectTimeout: 3000,
  requestTimeout: 30000
});
const consumerGroup = new kafka.ConsumerGroup(
  {
      kafkaHost: 'localhost:9092',
      groupId: 'notification-service-group',
      autoCommit: true,
      autoCommitIntervalMs: 5000,
      // Start reading from the beginning if no offset is found
      fromOffset: 'earliest',
      // Handle offset out of range by reading from earliest
      outOfRangeOffset: 'earliest'
  },
  ['order-created']
);

// Add consumer group event listeners
consumerGroup.on('ready', () => {
  console.log('Consumer group is ready');
});

consumerGroup.on('message', async (message) => {
  try {
    console.log('Raw Kafka message received:', message);
    const orderData = JSON.parse(message.value);
    const currentDate = new Date();
    
    // Create and save notification
    const notification = new Notification({
      userId: orderData.userId,
      message: `New order created for product: ${orderData.productId}`,
      read: false,
      createdAt: currentDate,
      orderId: orderData._id,
      orderDate: new Date(orderData.orderDate)
    });
    
    const savedNotification = await notification.save();

    // Update order status
    const updatedOrder = await Order.findByIdAndUpdate(
      orderData._id,
      {
        status: 'Completed',
        lastUpdated: currentDate
      },
      { new: true }
    );

    if (!updatedOrder) {
      throw new Error('Order not found');
    }

    const notificationMessage = {
      orderId: orderData._id,
      notificationId: savedNotification._id,
      status: 'Completed',
      lastUpdated: currentDate.toISOString(),
      productId: orderData.productId
    };

    // Get the Socket.IO instance
    const io = getIo();
    
    // Emit notification event
    io.emit('notification', {
      message: JSON.stringify(notificationMessage)
    });

    // Emit order status update event
    io.emit('orderStatusUpdate', {
      orderId: orderData._id,
      status: 'Completed',
      lastUpdated: currentDate.toISOString()
    });

  } catch (err) {
    console.error('Error processing Kafka message:', err);
  }
});

consumerGroup.on('error', (err) => {
  console.error('Consumer Group error:', err);
});

consumerGroup.on('offsetOutOfRange', (err) => {
  console.error('Offset out of range, resetting to earliest:', err);
});

router.post('/notify', async (req, res) => {
  const { userId, message } = req.body;
  const notification = new Notification({ userId, message });
  await notification.save();
  res.status(201).send(notification);
});

router.get('/notifications', auth(), async (req, res) => {
  try {
    const notifications = await Notification.find({ userId: req.user.id })
      .sort({ createdAt: -1 }); // Sort by newest first
    res.status(200).send(notifications);
  } catch (err) {
    console.error('Error fetching notifications:', err);
    res.status(500).json({ message: 'Internal server error' });
  }
});

router.put('/markAsRead/:notificationId', auth(), async (req, res) => {
  try {
    const notification = await Notification.findOne({
      _id: req.params.notificationId,
      userId: req.user.id
    });
    
    if (!notification) {
      return res.status(404).json({ message: 'Notification not found' });
    }
    
    notification.read = true;
    await notification.save();
    
    res.status(200).json(notification);
  } catch (err) {
    console.error('Error marking notification as read:', err);
    res.status(500).json({ message: 'Internal server error' });
  }
});

router.delete('/delete/:notificationId', auth(), async (req, res) => {
  try {
    const notification = await Notification.findOne({
      _id: req.params.notificationId,
      userId: req.user.id
    });
    
    if (!notification) {
      return res.status(404).json({ message: 'Notification not found' });
    }
    
    await Notification.deleteOne({ _id: req.params.notificationId });
    
    // Trigger refresh for real-time updates
    const io = getIo();
    io.emit('notificationDeleted', { notificationId: req.params.notificationId });
    
    res.status(200).json({ message: 'Notification deleted successfully' });
  } catch (err) {
    console.error('Error deleting notification:', err);
    res.status(500).json({ message: 'Internal server error' });
  }
});

module.exports = router;
