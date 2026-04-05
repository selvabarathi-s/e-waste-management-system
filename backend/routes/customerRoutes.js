const express = require('express');
const router = express.Router();
const db = require('../models/db');
const logger = require('../utils/logger');

// Get all customer devices
router.get('/customer/devices', async (req, res, next) => {
  try {
    const devices = await db.query('SELECT * FROM customer_devices ORDER BY created_at DESC');
    res.json(devices);
  } catch (error) {
    next(error);
  }
});

// Add a device
router.post('/customer/devices', async (req, res, next) => {
  try {
    const { username, device_type, age_years, condition_status, ai_suggestion } = req.body;
    await db.query(
      'INSERT INTO customer_devices (username, device_type, age_years, condition_status, ai_suggestion) VALUES (?, ?, ?, ?, ?)',
      [username, device_type, age_years, condition_status, ai_suggestion]
    );
    res.status(201).json({ message: 'Device added successfully' });
  } catch (error) {
    next(error);
  }
});

// Update a device
router.put('/customer/devices/:id', async (req, res, next) => {
  try {
    const { id } = req.params;
    const { device_type, age_years, condition_status, ai_suggestion } = req.body;
    await db.query(
      'UPDATE customer_devices SET device_type = ?, age_years = ?, condition_status = ?, ai_suggestion = ? WHERE id = ?',
      [device_type, age_years, condition_status, ai_suggestion, id]
    );
    res.json({ message: 'Device updated successfully' });
  } catch (error) {
    next(error);
  }
});

// Delete a device
router.delete('/customer/devices/:id', async (req, res, next) => {
  try {
    const { id } = req.params;
    await db.query('DELETE FROM customer_devices WHERE id = ?', [id]);
    res.json({ message: 'Device deleted successfully' });
  } catch (error) {
    next(error);
  }
});

module.exports = router;
