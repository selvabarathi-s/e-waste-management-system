const express = require('express');
const simulationController = require('../controllers/simulationController');

const router = express.Router();

router.post('/simulate', simulationController.runSimulation);
router.get('/scenarios', simulationController.getScenarios);
router.delete('/scenarios/:id', simulationController.deleteScenario);
router.post('/scenarios/compare', simulationController.compareScenarios);

module.exports = router;
