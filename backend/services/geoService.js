const axios = require('axios');
const config = require('../config/config');
const dataService = require('./dataService');
const logger = require('../utils/logger');
const { AppError } = require('../middleware/errorHandler');

const http = axios.create({
  baseURL: config.flaskUrl,
  timeout: config.mlRequestTimeoutMs,
});

async function getHeatmapPayload() {
  const rows = await dataService.getHeatmapPoints();
  return rows.map((r) => ({
    lat: parseFloat(r.lat),
    lng: parseFloat(r.lng),
    weight: Math.min(Math.max(parseFloat(r.weight) || 1, 0.1), 500),
  }));
}

async function getClustersGeoJson(method = 'dbscan') {
  const points = await dataService.getClusterInputPoints();
  if (points.length === 0) {
    return { type: 'FeatureCollection', features: [] };
  }

  try {
    const res = await http.post('/spatial/clusters', { points, method });
    return res.data.geojson || { type: 'FeatureCollection', features: [] };
  } catch (err) {
    logger.warn({ err: err.message }, 'cluster ML call failed');
    throw new AppError('Clustering service unavailable', 503, 'ML_UNAVAILABLE', {
      detail: err.response?.data || err.message,
    });
  }
}

module.exports = {
  getHeatmapPayload,
  getClustersGeoJson,
};
