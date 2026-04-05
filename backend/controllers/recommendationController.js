const recommendationService = require('../services/recommendationService');

exports.getRecommendations = async (req, res, next) => {
  try {
    const result = await recommendationService.getRecommendations();
    res.status(200).json(result);
  } catch (e) {
    next(e);
  }
};

exports.getRecyclingCenterLocations = async (req, res, next) => {
  try {
    const result = await recommendationService.getRecyclingCenterLocations();
    res.status(200).json(result);
  } catch (e) {
    next(e);
  }
};

exports.getBinPlacements = async (req, res, next) => {
  try {
    const result = await recommendationService.getBinPlacements();
    res.status(200).json(result);
  } catch (e) {
    next(e);
  }
};

exports.getRankedRecommendations = async (req, res, next) => {
  try {
    const limit = parseInt(req.query.limit, 10) || 20;
    const result = await recommendationService.getRankedRecommendations(limit);
    res.status(200).json(result);
  } catch (e) {
    next(e);
  }
};
