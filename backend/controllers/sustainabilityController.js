const sustainabilityService = require('../services/sustainabilityService');

exports.getSustainabilityScores = async (req, res, next) => {
  try {
    const scores = await sustainabilityService.getAllScores();
    res.status(200).json(scores);
  } catch (e) {
    next(e);
  }
};

exports.getRegionScore = async (req, res, next) => {
  try {
    const scores = await sustainabilityService.getRegionScore(req.params.region);
    if (!scores || scores.length === 0) {
      return res.status(404).json({ error: { code: 'NOT_FOUND', message: 'No scores found for this region' } });
    }
    res.status(200).json(scores);
  } catch (e) {
    next(e);
  }
};

exports.recalculateScores = async (req, res, next) => {
  try {
    const results = await sustainabilityService.calculateScoresForAllRegions();
    const alerts = await sustainabilityService.generateAlerts();
    res.status(200).json({ scores: results, alerts_generated: alerts.length, alerts });
  } catch (e) {
    next(e);
  }
};

exports.getTrendAnalysis = async (req, res, next) => {
  try {
    const region = req.query.region || null;
    const trends = await sustainabilityService.getTrendAnalysis(region);
    res.status(200).json(trends);
  } catch (e) {
    next(e);
  }
};
