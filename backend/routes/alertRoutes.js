const express = require('express');
const { query } = require('../models/db');

const router = express.Router();

router.get('/alerts', async (req, res, next) => {
  try {
    const { acknowledged, region } = req.query;
    let sql = 'SELECT * FROM alerts WHERE 1=1';
    const params = [];
    if (acknowledged !== undefined) {
      sql += ' AND acknowledged = ?';
      params.push(acknowledged === 'true' ? 1 : 0);
    }
    if (region) {
      sql += ' AND region LIKE ?';
      params.push(`%${region}%`);
    }
    sql += ' ORDER BY created_at DESC';
    const rows = await query(sql, params);
    res.json(rows);
  } catch (e) {
    next(e);
  }
});

router.post('/alerts/:id/acknowledge', async (req, res, next) => {
  try {
    await query('UPDATE alerts SET acknowledged = 1, acknowledged_at = NOW() WHERE id = ?', [req.params.id]);
    res.json({ acknowledged: true, id: req.params.id });
  } catch (e) {
    next(e);
  }
});

router.get('/alerts/summary', async (req, res, next) => {
  try {
    const rows = await query(`
      SELECT severity, COUNT(*) AS count,
             SUM(CASE WHEN acknowledged = 0 THEN 1 ELSE 0 END) AS unacknowledged
      FROM alerts
      GROUP BY severity
    `);
    const total = await query('SELECT COUNT(*) AS total FROM alerts WHERE acknowledged = 0');
    res.json({ breakdown: rows, total_unacknowledged: total[0]?.total || 0 });
  } catch (e) {
    next(e);
  }
});

module.exports = router;
