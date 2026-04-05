const express = require('express');
const { query: queryVal, validationResult } = require('express-validator');
const predictController = require('../controllers/predictController');

const router = express.Router();

const validateGetPredictions = [
  queryVal('region').optional().isString().isLength({ max: 255 }),
  queryVal('forecast_year').optional().isInt({ min: 1900, max: 2100 }),
  queryVal('device_category').optional().isString().isLength({ max: 128 }),
  queryVal('model_type').optional().isIn(['tabular', 'timeseries']),
];

function handleQueryValidation(req, res, next) {
  const errors = validationResult(req);
  if (!errors.isEmpty()) {
    return res.status(400).json({
      error: {
        code: 'VALIDATION_ERROR',
        message: 'Invalid query parameters',
        details: errors.array(),
      },
    });
  }
  next();
}

router.post('/predict', predictController.predictEwaste);
router.post('/predict/timeseries', predictController.predictTimeseries);
router.get('/predictions', validateGetPredictions, handleQueryValidation, predictController.getPredictions);
router.get('/ml/health', predictController.getMlHealth);

module.exports = router;
