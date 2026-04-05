const logger = require('../utils/logger');

function errorHandler(err, req, res, next) {
  if (err.name === 'MulterError') {
    return res.status(400).json({
      error: { code: err.code || 'UPLOAD_ERROR', message: err.message },
    });
  }
  if (err.message === 'Only CSV and Excel files are allowed') {
    return res.status(400).json({
      error: { code: 'INVALID_FILE_TYPE', message: err.message },
    });
  }

  const status = err.statusCode || err.status || 500;
  const code = err.code || (status === 400 ? 'BAD_REQUEST' : 'INTERNAL_ERROR');
  const message =
    err.expose || status === 503
      ? err.message
      : status === 500
        ? 'Internal server error'
        : err.message;

  if (status >= 500) {
    logger.error({ err, reqId: req.id, path: req.path }, err.message);
  } else {
    logger.warn({ err: err.message, reqId: req.id, path: req.path }, 'client error');
  }

  res.status(status).json({
    error: {
      code,
      message,
      details: err.details || undefined,
    },
  });
}

class AppError extends Error {
  constructor(message, statusCode = 400, code = 'BAD_REQUEST', details) {
    super(message);
    this.name = 'AppError';
    this.statusCode = statusCode;
    this.code = code;
    this.details = details;
    this.expose = statusCode < 500;
  }
}

module.exports = { errorHandler, AppError };
