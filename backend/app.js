const express = require('express');
const cors = require('cors');
const pinoHttp = require('pino-http');
const dataRoutes = require('./routes/dataRoutes');
const predictRoutes = require('./routes/predictRoutes');
const geoRoutes = require('./routes/geoRoutes');
const simulationRoutes = require('./routes/simulationRoutes');
const sustainabilityRoutes = require('./routes/sustainabilityRoutes');
const recommendationRoutes = require('./routes/recommendationRoutes');
const cvRoutes = require('./routes/cvRoutes');
const alertRoutes = require('./routes/alertRoutes');
const customerRoutes = require('./routes/customerRoutes');
const hubRoutes = require('./routes/hubRoutes');
const serviceRoutes = require('./routes/serviceRoutes');
const { initDB } = require('./models/db');
const { migrateSimulationTables } = require('./models/simulationDb');
const config = require('./config/config');
const logger = require('./utils/logger');
const requestIdMiddleware = require('./middleware/requestId');
const { errorHandler } = require('./middleware/errorHandler');

const app = express();

app.use(requestIdMiddleware);
app.use(
  pinoHttp({
    logger,
    genReqId: (req) => req.id,
    customProps: (req) => ({ requestId: req.id }),
  })
);
app.use(cors());
app.use(express.json());

app.use('/api', dataRoutes);
app.use('/api', predictRoutes);
app.use('/api', geoRoutes);
app.use('/api', simulationRoutes);
app.use('/api', sustainabilityRoutes);
app.use('/api', recommendationRoutes);
app.use('/api', cvRoutes);
app.use('/api', alertRoutes);
app.use('/api', customerRoutes);
app.use('/api', hubRoutes);
app.use('/api', serviceRoutes);

app.use((req, res) => {
  res.status(404).json({ error: { code: 'NOT_FOUND', message: 'Not found' } });
});

app.use(errorHandler);

initDB()
  .then(() => migrateSimulationTables())
  .then(() => {
    app.listen(config.port, () => {
      logger.info(`Node.js Backend running on http://localhost:${config.port}`);
    });
  })
  .catch((err) => {
    logger.error(err, 'Failed to start');
    process.exit(1);
  });

module.exports = app;
