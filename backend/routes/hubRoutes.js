const express = require('express');
const router = express.Router();
const db = require('../models/db');

// Get all hub inventory
router.get('/hub/inventory', async (req, res, next) => {
  try {
    const items = await db.query('SELECT * FROM hub_inventory ORDER BY created_at DESC');
    res.json(items);
  } catch (error) {
    next(error);
  }
});

// Add hub inventory
router.post('/hub/inventory', async (req, res, next) => {
  try {
    const { batch_id, source, category, weight_kg, ai_classification, destination } = req.body;
    await db.query(
      'INSERT INTO hub_inventory (batch_id, source, category, weight_kg, ai_classification, destination) VALUES (?, ?, ?, ?, ?, ?)',
      [batch_id, source, category, weight_kg, ai_classification, destination]
    );
    res.status(201).json({ message: 'Inventory added successfully' });
  } catch (error) {
    next(error);
  }
});

// Update hub inventory
router.put('/hub/inventory/:id', async (req, res, next) => {
  try {
    const { id } = req.params;
    const { status, destination } = req.body;
    await db.query(
      'UPDATE hub_inventory SET status = ?, destination = ? WHERE id = ?',
      [status, destination, id]
    );
    res.json({ message: 'Inventory updated successfully' });
  } catch (error) {
    next(error);
  }
});

// Delete hub inventory
router.delete('/hub/inventory/:id', async (req, res, next) => {
  try {
    const { id } = req.params;
    await db.query('DELETE FROM hub_inventory WHERE id = ?', [id]);
    res.json({ message: 'Inventory deleted successfully' });
  } catch (error) {
    next(error);
  }
});

module.exports = router;
