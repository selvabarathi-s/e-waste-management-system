const express = require('express');
const router = express.Router();
const geoController = require('../controllers/geoController');

router.get('/map/heatmap', geoController.getHeatmap);
router.get('/map/clusters', geoController.getClusters);

module.exports = router;
