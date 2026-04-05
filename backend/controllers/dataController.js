const multer = require('multer');
const dataService = require('../services/dataService');
const { AppError } = require('../middleware/errorHandler');

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

exports.upload = upload.single('file');

exports.uploadData = async (req, res, next) => {
  try {
    if (!req.file) {
      throw new AppError('No file uploaded. Please upload a CSV or Excel file.', 400, 'VALIDATION_ERROR');
    }

    let rows = [];
    if (req.file.mimetype === 'text/csv' || req.file.originalname.endsWith('.csv')) {
      rows = await dataService.parseCSV(req.file.buffer);
    } else {
      rows = dataService.parseExcel(req.file.buffer);
    }

    if (!rows || rows.length === 0) {
      throw new AppError('No valid data found in file.', 400, 'VALIDATION_ERROR');
    }

    const { inserted } = await dataService.uploadFromRows(rows);
    if (inserted === 0) {
      throw new AppError(
        'No rows inserted. Check required columns: region, year, sales_import_tonnes, population_millions, disposal_amount_tonnes (optional: device_category).',
        400,
        'VALIDATION_ERROR'
      );
    }

    res.status(201).json({ message: `Data uploaded successfully. ${inserted} records inserted.` });
  } catch (e) {
    next(e);
  }
};

exports.getData = async (req, res, next) => {
  try {
    const filters = {
      year: req.query.year,
      region: req.query.region,
      device_category: req.query.device_category,
      page: req.query.page,
      limit: req.query.limit,
    };
    const result = await dataService.getEwasteData(filters);
    res.status(200).json(result);
  } catch (e) {
    next(e);
  }
};

exports.getMapData = async (req, res, next) => {
  try {
    const rows = await dataService.getLocations();
    res.status(200).json(rows);
  } catch (e) {
    next(e);
  }
};

exports.validateUpload = async (req, res, next) => {
  try {
    if (!req.file) {
      throw new AppError('No file uploaded. Please upload a CSV or Excel file.', 400, 'VALIDATION_ERROR');
    }

    let rows = [];
    if (req.file.mimetype === 'text/csv' || req.file.originalname.endsWith('.csv')) {
      rows = await dataService.parseCSV(req.file.buffer);
    } else {
      rows = dataService.parseExcel(req.file.buffer);
    }

    if (!rows || rows.length === 0) {
      throw new AppError('No valid data found in file.', 400, 'VALIDATION_ERROR');
    }

    const validation = await dataService.validateData(rows);
    res.status(200).json(validation);
  } catch (e) {
    next(e);
  }
};
