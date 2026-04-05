require('dotenv').config();

module.exports = {
  db: {
    host: process.env.DB_HOST || 'localhost',
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD || '',
    database: process.env.DB_NAME || 'ewaste_schema',
  },
  port: process.env.PORT || 5000,
  flaskUrl: process.env.FLASK_URL || 'http://localhost:5001',
  logLevel: process.env.LOG_LEVEL || 'info',
  nodeEnv: process.env.NODE_ENV || 'development',
  mlRequestTimeoutMs: parseInt(process.env.ML_REQUEST_TIMEOUT_MS || '60000', 10),
};
