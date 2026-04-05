const geoService = require('../services/geoService');
const logger = require('../utils/logger');

exports.getHeatmap = async (req, res, next) => {
  try {
    const points = await geoService.getHeatmapPayload();
    res.status(200).json({ points });
  } catch (e) {
    next(e);
  }
};

exports.getClusters = async (req, res, next) => {
  try {
    const method = req.query.method === 'kmeans' ? 'kmeans' : 'dbscan';
    const geojson = await geoService.getClustersGeoJson(method);
    res.status(200).json(geojson);
  } catch (e) {
    if (e.statusCode === 503) {
      logger.warn('Returning empty clusters (ML unavailable)');
      return res.status(200).json({
        type: 'FeatureCollection',
        features: [],
        warning: 'Clustering service unavailable. Start ml-service and train models.',
      });
    }
    next(e);
  }
};
