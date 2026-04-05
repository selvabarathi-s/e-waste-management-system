const pino = require('pino');
const config = require('../config/config');

const logger = pino({ level: config.logLevel || 'info' });

module.exports = logger;
