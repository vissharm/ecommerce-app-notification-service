const mongoose = require('mongoose');

const NotificationSchema = new mongoose.Schema({
  userId: String,
  message: String,
  read: { type: Boolean, default: false },
  createdAt: { type: Date, default: Date.now },
  orderId: String,
  orderDate: Date
});

module.exports = mongoose.model('Notification', NotificationSchema);