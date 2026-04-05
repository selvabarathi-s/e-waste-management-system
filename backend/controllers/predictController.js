const axios = require('axios');
const config = require('../config/config');
const predictionService = require('../services/predictionService');
exports.predictEwaste = async (req, res, next) => {
  try {
    const result = await predictionService.runTabularForecast(req.body);
    res.status(200).json(result);
  } catch (e) {
    next(e);
  }
};

exports.predictTimeseries = async (req, res, next) => {
  try {
    const result = await predictionService.runTimeseriesForecast(req.body);
    res.status(200).json(result);
  } catch (e) {
    next(e);
  }
};

exports.getPredictions = async (req, res, next) => {
  try {
    const filters = {
      region: req.query.region,
      forecast_year: req.query.forecast_year,
      device_category: req.query.device_category,
      model_type: req.query.model_type,
      page: req.query.page,
      limit: req.query.limit,
    };
    const result = await predictionService.listPredictions(filters);
    res.status(200).json(result);
  } catch (e) {
    next(e);
  }
};

exports.getMlHealth = async (req, res, next) => {
  try {
    const r = await axios.get(`${config.flaskUrl}/health`, { timeout: 5000 });
    res.status(200).json(r.data);
  } catch (e) {
    res.status(503).json({
      ok: false,
      error: e.message,
      flaskUrl: config.flaskUrl,
    });
  }
};
