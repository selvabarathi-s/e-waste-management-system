const axios = require('axios');
const config = require('../config/config');
const { query, transaction } = require('../models/db');
const logger = require('../utils/logger');
const { AppError } = require('../middleware/errorHandler');

const http = axios.create({
  baseURL: config.flaskUrl,
  timeout: config.mlRequestTimeoutMs,
});

function extractFlaskMessage(err) {
  const d = err.response?.data;
  if (!d) return null;
  if (typeof d.error === 'string') return d.error;
  if (d.error && typeof d.error === 'object' && d.error.message) return String(d.error.message);
  return null;
}

function axiosFailureSummary(err) {
  const fromBody = extractFlaskMessage(err);
  if (fromBody) return fromBody;
  if (err.response) {
    return `HTTP ${err.response.status} ${err.response.statusText || ''}`.trim();
  }
  return err.message || 'request failed';
}

function isMlUnreachable(err) {
  return (
    !err.response &&
    ['ECONNREFUSED', 'ENOTFOUND', 'ETIMEDOUT', 'ECONNABORTED', 'EAI_AGAIN'].includes(err.code)
  );
}

function mlConnectionError() {
  return new AppError(
    `Cannot reach the ML service at ${config.flaskUrl}. Start it in another terminal: cd ml-service, then run "python train.py" (once) and "python app.py". Ensure backend/.env has FLASK_URL=${config.flaskUrl}`,
    503,
    'ML_UNAVAILABLE',
    { flaskUrl: config.flaskUrl, reason: 'connection' }
  );
}

function validateTabularInput(body) {
  const {
    region,
    forecast_year,
    sales_import_tonnes,
    population_millions,
    device_category,
    model_type,
  } = body;

  if (
    !region ||
    forecast_year === undefined ||
    sales_import_tonnes === undefined ||
    population_millions === undefined
  ) {
    throw new AppError(
      'Missing required fields: region, forecast_year, sales_import_tonnes, population_millions',
      400,
      'VALIDATION_ERROR'
    );
  }

  const forecastYear = parseInt(forecast_year, 10);
  const sales = parseFloat(sales_import_tonnes);
  const population = parseFloat(population_millions);

  if (Number.isNaN(forecastYear) || forecastYear < 2020 || forecastYear > 2100) {
    throw new AppError('Invalid forecast_year. Must be between 2020 and 2100.', 400, 'VALIDATION_ERROR');
  }
  if (Number.isNaN(sales) || sales < 0) {
    throw new AppError('Invalid sales_import_tonnes. Must be a non-negative number.', 400, 'VALIDATION_ERROR');
  }
  if (Number.isNaN(population) || population < 0) {
    throw new AppError('Invalid population_millions. Must be a non-negative number.', 400, 'VALIDATION_ERROR');
  }

  return {
    region: String(region).trim(),
    forecastYear,
    sales,
    population,
    deviceCategory: device_category ? String(device_category).trim() : null,
    modelType: model_type === 'timeseries' ? 'timeseries' : 'tabular',
  };
}

async function runTabularForecast(body) {
  const v = validateTabularInput(body);

  let flaskResponse;
  try {
    flaskResponse = await http.post('/forecast/tabular', {
      sales_import_tonnes: v.sales,
      population_millions: v.population,
      forecast_year: v.forecastYear,
    });
  } catch (err) {
    if (isMlUnreachable(err)) {
      throw mlConnectionError();
    }
    const tabularMsg = axiosFailureSummary(err);
    logger.warn({ err: err.message, tabularMsg }, 'tabular ML call failed, trying legacy /predict');
    try {
      const qs = new URLSearchParams({ forecast_year: String(v.forecastYear) }).toString();
      flaskResponse = await http.post(
        `/predict?${qs}`,
        [{ sales_import_tonnes: v.sales, population_millions: v.population }]
      );
    } catch (err2) {
      if (isMlUnreachable(err2)) {
        throw mlConnectionError();
      }
      const predictMsg = axiosFailureSummary(err2);
      logger.error({ tabularMsg, predictMsg }, 'ML tabular and legacy predict both failed');
      throw new AppError(
        `${predictMsg} If models are missing: open ml-service folder, run "python train.py" (or "python train_mock_model.py"), then "python app.py". ML base URL: ${config.flaskUrl}`,
        503,
        'ML_UNAVAILABLE',
        { flaskUrl: config.flaskUrl, tabularError: tabularMsg, predictError: predictMsg }
      );
    }
  }

  const data = flaskResponse.data;
  const predictions = data.predictions;
  if (!predictions || predictions.length === 0) {
    throw new AppError('No prediction returned from ML service', 500, 'ML_EMPTY');
  }

  const predicted_tonnes = predictions[0];
  const metrics = data.metrics || null;
  const modelVersion = data.model_version || 'legacy-linear';

  try {
    await query(
      `INSERT INTO predictions (region, forecast_year, predicted_tonnes, device_category, model_version, metric_snapshot, model_type)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        v.region,
        v.forecastYear,
        predicted_tonnes,
        v.deviceCategory,
        modelVersion,
        metrics ? JSON.stringify(metrics) : null,
        'tabular',
      ]
    );
  } catch (dbErr) {
    logger.error({ err: dbErr.message }, 'Failed to save tabular prediction to DB');
    throw new AppError('Failed to save prediction to database', 500, 'DB_INSERT_FAILED');
  }

  return {
    region: v.region,
    forecast_year: v.forecastYear,
    predicted_tonnes,
    device_category: v.deviceCategory,
    model_version: modelVersion,
    metrics,
    model_type: 'tabular',
  };
}

async function runTimeseriesForecast(body) {
  const { region, horizon_years, device_category } = body;
  if (!region) {
    throw new AppError('region is required', 400, 'VALIDATION_ERROR');
  }
  const horizon = parseInt(horizon_years, 10) || 5;
  if (horizon < 1 || horizon > 30) {
    throw new AppError('horizon_years must be between 1 and 30', 400, 'VALIDATION_ERROR');
  }

  const regionName = String(region).trim();
  let historyRows;
  if (device_category) {
    historyRows = await query(
      `SELECT year, SUM(disposal_amount_tonnes) AS disposal_amount_tonnes
       FROM ewaste_data WHERE region = ? AND device_category = ?
       GROUP BY year ORDER BY year ASC`,
      [regionName, String(device_category).trim()]
    );
  } else {
    historyRows = await query(
      `SELECT year, SUM(disposal_amount_tonnes) AS disposal_amount_tonnes
       FROM ewaste_data WHERE region = ?
       GROUP BY year ORDER BY year ASC`,
      [regionName]
    );
  }

  const history = historyRows.map((r) => ({
    year: r.year,
    disposal_amount_tonnes: parseFloat(r.disposal_amount_tonnes) || 0,
  }));

  if (history.length === 0) {
    throw new AppError(
      'No historical e-waste data for this region (and category). Upload data first.',
      400,
      'NO_HISTORY'
    );
  }

  let flaskResponse;
  try {
    flaskResponse = await http.post('/forecast/timeseries', {
      region: regionName,
      horizon_years: horizon,
      device_category: device_category ? String(device_category).trim() : null,
      history,
    });
  } catch (err) {
    if (isMlUnreachable(err)) {
      throw mlConnectionError();
    }
    const summary = axiosFailureSummary(err);
    logger.error({ err: err.message, summary }, 'timeseries ML call failed, trying legacy /predict');
    try {
      const qs = new URLSearchParams({
        forecast_year: String(new Date().getFullYear() + horizon),
      }).toString();
      flaskResponse = await http.post(
        `/predict?${qs}`,
        history.map((h) => ({
          sales_import_tonnes: h.disposal_amount_tonnes,
          population_millions: 1,
        }))
      );
      const legacyPreds = flaskResponse.data.predictions || [];
      const lastYear = history[history.length - 1].year;
      const series = legacyPreds.map((val, i) => ({
        year: lastYear + i + 1,
        predicted_tonnes: parseFloat(val) || 0,
      }));
      if (series.length === 0) {
        throw new AppError('No prediction returned from ML service', 500, 'ML_EMPTY');
      }
      try {
        await transaction(async (txQuery) => {
          for (const point of series) {
            await txQuery(
              `INSERT INTO predictions (region, forecast_year, predicted_tonnes, device_category, model_version, metric_snapshot, model_type)
               VALUES (?, ?, ?, ?, ?, ?, ?)`,
              [
                regionName,
                point.year,
                point.predicted_tonnes,
                device_category ? String(device_category).trim() : null,
                'legacy-linear',
                null,
                'timeseries',
              ]
            );
          }
        });
      } catch (dbErr) {
        logger.error({ err: dbErr.message }, 'Failed to save timeseries predictions to DB');
        throw new AppError('Failed to save predictions to database', 500, 'DB_INSERT_FAILED');
      }
      return {
        region: regionName,
        model_type: 'timeseries',
        model_version: 'legacy-linear',
        metrics: null,
        series,
      };
    } catch (err2) {
      if (isMlUnreachable(err2)) {
        throw mlConnectionError();
      }
      const detail = axiosFailureSummary(err2);
      logger.error({ timeseriesError: summary, legacyError: detail }, 'ML timeseries and legacy predict both failed');
      throw new AppError(
        extractFlaskMessage(err) ||
          `Timeseries failed (${summary}). Start ML: cd ml-service && python app.py — ${config.flaskUrl}`,
        503,
        'ML_UNAVAILABLE',
        { flaskUrl: config.flaskUrl, detail: err.response?.data }
      );
    }
  }

  const data = flaskResponse.data;
  const series = data.series || [];
  const metrics = data.metrics || null;
  const modelVersion = data.model_version || 'arima';

  if (series.length === 0) {
    throw new AppError('No timeseries prediction returned from ML service', 500, 'ML_EMPTY');
  }

  try {
    await transaction(async (txQuery) => {
      for (const point of series) {
        await txQuery(
          `INSERT INTO predictions (region, forecast_year, predicted_tonnes, device_category, model_version, metric_snapshot, model_type)
           VALUES (?, ?, ?, ?, ?, ?, ?)`,
          [
            regionName,
            point.year,
            point.predicted_tonnes,
            device_category ? String(device_category).trim() : null,
            modelVersion,
            metrics ? JSON.stringify(metrics) : null,
            'timeseries',
          ]
        );
      }
    });
  } catch (dbErr) {
    logger.error({ err: dbErr.message }, 'Failed to save timeseries predictions to DB');
    throw new AppError('Failed to save predictions to database', 500, 'DB_INSERT_FAILED');
  }

  return {
    region: regionName,
    model_type: 'timeseries',
    model_version: modelVersion,
    metrics,
    series,
  };
}

async function listPredictions(filters = {}) {
  const conditions = ['1=1'];
  const params = [];
  if (filters.region) {
    conditions.push('region LIKE ?');
    params.push(`%${filters.region}%`);
  }
  if (filters.forecast_year) {
    const y = parseInt(filters.forecast_year, 10);
    if (!Number.isNaN(y)) {
      conditions.push('forecast_year = ?');
      params.push(y);
    }
  }
  if (filters.device_category) {
    conditions.push('device_category = ?');
    params.push(filters.device_category);
  }
  if (filters.model_type) {
    conditions.push('model_type = ?');
    params.push(filters.model_type);
  }

  const whereClause = conditions.join(' AND ');
  const page = Math.max(1, parseInt(filters.page, 10) || 1);
  const limit = Math.min(500, Math.max(1, parseInt(filters.limit, 10) || 100));
  const offset = (page - 1) * limit;

  const countRows = await query(`SELECT COUNT(*) AS total FROM predictions WHERE ${whereClause}`, params);
  const rows = await query(
    `SELECT * FROM predictions WHERE ${whereClause} ORDER BY forecast_year DESC, created_at DESC LIMIT ? OFFSET ?`,
    [...params, limit, offset]
  );

  return {
    data: rows,
    pagination: { page, limit, total: countRows[0]?.total || rows.length, hasMore: rows.length === limit },
  };
}

module.exports = {
  runTabularForecast,
  runTimeseriesForecast,
  listPredictions,
  validateTabularInput,
};
