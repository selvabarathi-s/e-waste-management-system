const express = require('express');
const multer = require('multer');
const axios = require('axios');
const FormData = require('form-data');
const { query } = require('../models/db');

const router = express.Router();
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

const CV_URL = process.env.CV_URL || 'http://localhost:5002';

async function storeClassification(filename, result) {
  try {
    await query(
      `INSERT INTO cv_classifications
       (filename, predicted_class, confidence, is_ewaste, device_category, estimated_weight_kg, all_probabilities)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        filename,
        result.predicted_class || 'unknown',
        result.confidence || 0,
        result.is_ewaste !== undefined ? result.is_ewaste : true,
        result.device_category || null,
        result.estimated_weight_kg || null,
        result.all_probabilities ? JSON.stringify(result.all_probabilities) : null,
      ]
    );
  } catch (e) {
    console.error('Failed to store classification:', e.message);
  }
}

router.post('/classify', upload.single('image'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: { code: 'VALIDATION_ERROR', message: 'No image provided' } });

    const formData = new FormData();
    formData.append('image', req.file.buffer, { filename: req.file.originalname, contentType: req.file.mimetype });

    const response = await axios.post(`${CV_URL}/classify`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      timeout: 30000,
    });

    await storeClassification(req.file.originalname, response.data);
    res.json(response.data);
  } catch (e) {
    if (e.code === 'ECONNREFUSED') {
      return res.status(503).json({ error: { code: 'CV_UNAVAILABLE', message: 'CV service not running. Start it: cd cv-service && python app.py' } });
    }
    res.status(500).json({ error: { code: 'CLASSIFICATION_ERROR', message: e.message } });
  }
});

router.post('/classify/batch', upload.array('images', 20), async (req, res) => {
  try {
    if (!req.files || req.files.length === 0) return res.status(400).json({ error: { code: 'VALIDATION_ERROR', message: 'No images provided' } });

    const formData = new FormData();
    req.files.forEach((f) => formData.append('images', f.buffer, { filename: f.originalname, contentType: f.file.mimetype }));

    const response = await axios.post(`${CV_URL}/classify/batch`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
      timeout: 60000,
    });

    if (response.data.results) {
      for (const r of response.data.results) {
        const fileMatch = req.files.find((f) => f.originalname === r.filename);
        if (fileMatch) {
          await storeClassification(r.filename, r);
        }
      }
    }
    res.json(response.data);
  } catch (e) {
    if (e.code === 'ECONNREFUSED') {
      return res.status(503).json({ error: { code: 'CV_UNAVAILABLE', message: 'CV service not running.' } });
    }
    res.status(500).json({ error: { code: 'CLASSIFICATION_ERROR', message: e.message } });
  }
});

router.get('/cv/health', async (req, res) => {
  try {
    const response = await axios.get(`${CV_URL}/health`, { timeout: 5000 });
    res.json(response.data);
  } catch (e) {
    res.status(200).json({ ok: false, error: e.message, cvUrl: CV_URL });
  }
});

router.get('/cv/history', async (req, res) => {
  try {
    const limit = Math.min(200, Math.max(1, parseInt(req.query.limit, 10) || 50));
    const page = Math.max(1, parseInt(req.query.page, 10) || 1);
    const offset = (page - 1) * limit;
    const cls = req.query.class || null;
    const isEwaste = req.query.is_ewaste;

    let sql = 'SELECT * FROM cv_classifications WHERE 1=1';
    const params = [];

    if (cls) {
      sql += ' AND predicted_class = ?';
      params.push(cls);
    }
    if (isEwaste !== undefined) {
      sql += ' AND is_ewaste = ?';
      params.push(isEwaste === 'true' ? 1 : 0);
    }

    const countSql = `SELECT COUNT(*) AS total FROM (${sql}) AS t`;
    const [countRows] = await require('mysql2/promise').createPool({
      host: process.env.DB_HOST || 'localhost',
      user: process.env.DB_USER || 'root',
      password: process.env.DB_PASSWORD || '1234',
      database: process.env.DB_NAME || 'ewaste_schema',
    }).execute(countSql, params);

    sql += ' ORDER BY created_at DESC LIMIT ? OFFSET ?';
    const rows = await query(sql, [...params, limit, offset]);

    const parsed = rows.map((r) => ({
      ...r,
      all_probabilities: typeof r.all_probabilities === 'string' ? JSON.parse(r.all_probabilities) : r.all_probabilities,
    }));

    res.json({
      classifications: parsed,
      pagination: {
        page,
        limit,
        total: countRows[0]?.total || rows.length,
        hasMore: rows.length === limit,
      },
    });
  } catch (e) {
    res.status(500).json({ error: { code: 'HISTORY_ERROR', message: e.message } });
  }
});

router.get('/cv/stats', async (req, res) => {
  try {
    const [totalRows, classDist, ewasteCount] = await Promise.all([
      query('SELECT COUNT(*) AS total FROM cv_classifications'),
      query('SELECT predicted_class, COUNT(*) AS count FROM cv_classifications GROUP BY predicted_class ORDER BY count DESC'),
      query('SELECT SUM(CASE WHEN is_ewaste = 1 THEN 1 ELSE 0 END) AS ewaste, SUM(CASE WHEN is_ewaste = 0 THEN 1 ELSE 0 END) AS non_ewaste FROM cv_classifications'),
    ]);

    res.json({
      total: totalRows[0]?.total || 0,
      class_distribution: classDist,
      ewaste_detected: ewasteCount[0]?.ewaste || 0,
      non_ewaste: ewasteCount[0]?.non_ewaste || 0,
    });
  } catch (e) {
    res.status(500).json({ error: { code: 'STATS_ERROR', message: e.message } });
  }
});

module.exports = router;
