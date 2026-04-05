const express = require('express');
const router = express.Router();
const db = require('../models/db');

// Get all service jobs
router.get('/service/jobs', async (req, res, next) => {
  try {
    const jobs = await db.query('SELECT * FROM service_jobs ORDER BY created_at DESC');
    res.json(jobs);
  } catch (error) {
    next(error);
  }
});

// Add service job
router.post('/service/jobs', async (req, res, next) => {
  try {
    const { job_type, device_or_material, issue_or_details, weight_kg } = req.body;
    await db.query(
      'INSERT INTO service_jobs (job_type, device_or_material, issue_or_details, weight_kg) VALUES (?, ?, ?, ?)',
      [job_type, device_or_material, issue_or_details, weight_kg]
    );
    res.status(201).json({ message: 'Job added successfully' });
  } catch (error) {
    next(error);
  }
});

// Update service job status
router.put('/service/jobs/:id', async (req, res, next) => {
  try {
    const { id } = req.params;
    const { status } = req.body;
    await db.query(
      'UPDATE service_jobs SET status = ? WHERE id = ?',
      [status, id]
    );
    res.json({ message: 'Job updated successfully' });
  } catch (error) {
    next(error);
  }
});

// Delete service job
router.delete('/service/jobs/:id', async (req, res, next) => {
  try {
    const { id } = req.params;
    await db.query('DELETE FROM service_jobs WHERE id = ?', [id]);
    res.json({ message: 'Job deleted successfully' });
  } catch (error) {
    next(error);
  }
});

module.exports = router;
