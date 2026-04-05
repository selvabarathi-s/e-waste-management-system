const express = require('express');
const recommendationController = require('../controllers/recommendationController');

const router = express.Router();

router.get('/recommendations', recommendationController.getRecommendations);
router.get('/recommendations/recycling-centers', recommendationController.getRecyclingCenterLocations);
router.get('/recommendations/bin-placements', recommendationController.getBinPlacements);
router.get('/recommendations/ranked', recommendationController.getRankedRecommendations);

module.exports = router;
