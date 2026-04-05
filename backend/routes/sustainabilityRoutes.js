const express = require('express');
const sustainabilityController = require('../controllers/sustainabilityController');

const router = express.Router();

router.get('/scores', sustainabilityController.getSustainabilityScores);
router.get('/scores/:region', sustainabilityController.getRegionScore);
router.post('/scores/recalculate', sustainabilityController.recalculateScores);
router.get('/scores/trends', sustainabilityController.getTrendAnalysis);

module.exports = router;
