const express = require('express');
const { query: queryVal, validationResult } = require('express-validator');
const multer = require('multer');
const dataController = require('../controllers/dataController');

const router = express.Router();

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    const allowed = [
      'text/csv',
      'application/vnd.ms-excel',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    ];
    if (
      allowed.includes(file.mimetype) ||
      file.originalname.endsWith('.csv') ||
      file.originalname.endsWith('.xlsx') ||
      file.originalname.endsWith('.xls')
    ) {
      cb(null, true);
    } else {
      cb(new Error('Only CSV and Excel files are allowed'));
    }
  },
});

const validateGetData = [
  queryVal('year').optional().isInt({ min: 1900, max: 2100 }).withMessage('Invalid year'),
  queryVal('region').optional().isString().isLength({ max: 255 }),
  queryVal('device_category').optional().isString().isLength({ max: 128 }),
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

router.post('/upload', dataController.upload, dataController.uploadData);
router.post('/upload/validate', upload.single('file'), dataController.validateUpload);
router.get('/data', validateGetData, handleQueryValidation, dataController.getData);
router.get('/map-data', dataController.getMapData);

module.exports = router;
