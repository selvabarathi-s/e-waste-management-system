const { query } = require('../models/db');
const logger = require('../utils/logger');
const { AppError } = require('../middleware/errorHandler');

async function runSimulation(params) {
  const {
    name,
    description,
    sales_change_pct = 0,
    recycling_rate_change = 0,
    policy_factor = 1.0,
    forecast_horizon_years = 5,
    region,
  } = params;

  if (!name) throw new AppError('Scenario name is required', 400, 'VALIDATION_ERROR');
  if (forecast_horizon_years < 1 || forecast_horizon_years > 50) {
    throw new AppError('Horizon must be 1-50 years', 400, 'VALIDATION_ERROR');
  }

  let historicalRows;
  try {
    historicalRows = await query(
      `SELECT year, SUM(sales_import_tonnes) AS sales, SUM(disposal_amount_tonnes) AS disposal
       FROM ewaste_data
       ${region ? 'WHERE region LIKE ?' : ''}
       GROUP BY year ORDER BY year ASC`,
      region ? [`%${region}%`] : []
    );
  } catch (dbErr) {
    logger.error({ err: dbErr.message }, 'Failed to fetch historical data for simulation');
    throw new AppError('Failed to fetch historical data', 500, 'DB_QUERY_FAILED');
  }

  if (historicalRows.length === 0) {
    throw new AppError('No historical data found. Upload data first.', 400, 'NO_DATA');
  }

  const latestYear = historicalRows[historicalRows.length - 1];
  const baselineSales = parseFloat(latestYear.sales) || 0;
  const baselineDisposal = parseFloat(latestYear.disposal) || 0;

  const salesMultiplier = 1 + sales_change_pct / 100;
  const recyclingMultiplier = 1 - recycling_rate_change / 100;

  const projectedSeries = [];
  let cumulativeProjected = 0;
  let cumulativeBaseline = 0;

  const growthRate = historicalRows.length >= 2
    ? (parseFloat(historicalRows[historicalRows.length - 1].disposal) -
       parseFloat(historicalRows[0].disposal)) /
      (historicalRows.length - 1) /
      (parseFloat(historicalRows[0].disposal) || 1)
    : 0.05;

  for (let i = 1; i <= forecast_horizon_years; i++) {
    const year = parseInt(latestYear.year) + i;
    const baselineProjected = baselineDisposal * Math.pow(1 + growthRate, i);
    const simulatedSales = baselineSales * Math.pow(salesMultiplier, i);
    const simulatedDisposal = baselineProjected * Math.pow(salesMultiplier, i) * recyclingMultiplier * policy_factor;

    cumulativeBaseline += baselineProjected;
    cumulativeProjected += simulatedDisposal;

    projectedSeries.push({
      year,
      baseline_tonnes: Math.round(baselineProjected * 100) / 100,
      projected_sales: Math.round(simulatedSales * 100) / 100,
      projected_disposal: Math.round(simulatedDisposal * 100) / 100,
      difference: Math.round((simulatedDisposal - baselineProjected) * 100) / 100,
    });
  }

  const totalImpact = cumulativeProjected - cumulativeBaseline;
  const impactPct = cumulativeBaseline > 0 ? (totalImpact / cumulativeBaseline) * 100 : 0;

  try {
    const result = await query(
      `INSERT INTO simulation_scenarios
       (name, description, sales_change_pct, recycling_rate_change, policy_factor,
        forecast_horizon_years, baseline_tonnes, projected_tonnes, impact_tonnes, impact_pct, result_data)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        name,
        description || null,
        sales_change_pct,
        recycling_rate_change,
        policy_factor,
        forecast_horizon_years,
        Math.round(cumulativeBaseline * 100) / 100,
        Math.round(cumulativeProjected * 100) / 100,
        Math.round(totalImpact * 100) / 100,
        Math.round(impactPct * 100) / 100,
        JSON.stringify({ series: projectedSeries, region: region || 'all' }),
      ]
    );

    logger.info(
      { scenarioId: result.insertId, name, impactPct: impactPct.toFixed(2) },
      'Simulation scenario created'
    );

    return {
      id: result.insertId,
      name,
      baseline_tonnes: Math.round(cumulativeBaseline * 100) / 100,
      projected_tonnes: Math.round(cumulativeProjected * 100) / 100,
      impact_tonnes: Math.round(totalImpact * 100) / 100,
      impact_pct: Math.round(impactPct * 100) / 100,
      series: projectedSeries,
      parameters: {
        sales_change_pct,
        recycling_rate_change,
        policy_factor,
        forecast_horizon_years,
        region: region || 'all',
      },
    };
  } catch (dbErr) {
    logger.error({ err: dbErr.message }, 'Failed to save simulation scenario');
    throw new AppError('Failed to save simulation scenario', 500, 'DB_INSERT_FAILED');
  }
}

async function getScenarios() {
  try {
    return await query('SELECT * FROM simulation_scenarios ORDER BY created_at DESC');
  } catch (dbErr) {
    logger.error({ err: dbErr.message }, 'Failed to fetch simulation scenarios');
    throw new AppError('Failed to fetch scenarios', 500, 'DB_QUERY_FAILED');
  }
}

async function deleteScenario(id) {
  const result = await query('DELETE FROM simulation_scenarios WHERE id = ?', [id]);
  if (result.affectedRows === 0) {
    throw new AppError('Scenario not found', 404, 'NOT_FOUND');
  }
  return { deleted: true, id };
}

async function compareScenarios(ids) {
  if (!ids || ids.length < 2) {
    throw new AppError('At least 2 scenario IDs are required for comparison', 400, 'VALIDATION_ERROR');
  }

  let scenarios;
  try {
    const placeholders = ids.map(() => '?').join(',');
    scenarios = await query(
      `SELECT * FROM simulation_scenarios WHERE id IN (${placeholders}) ORDER BY created_at ASC`,
      ids
    );
  } catch (dbErr) {
    logger.error({ err: dbErr.message }, 'Failed to fetch scenarios for comparison');
    throw new AppError('Failed to fetch scenarios for comparison', 500, 'DB_QUERY_FAILED');
  }

  if (scenarios.length < 2) {
    throw new AppError('Could not find enough scenarios to compare', 404, 'NOT_FOUND');
  }

  const comparison = scenarios.map((s) => {
    let resultData;
    try {
      resultData = typeof s.result_data === 'string' ? JSON.parse(s.result_data) : s.result_data;
    } catch (parseErr) {
      logger.warn({ scenarioId: s.id }, 'Malformed result_data in scenario, using empty series');
      resultData = { series: [] };
    }
    return {
      id: s.id,
      name: s.name,
      sales_change_pct: s.sales_change_pct,
      recycling_rate_change: s.recycling_rate_change,
      policy_factor: s.policy_factor,
      baseline_tonnes: s.baseline_tonnes,
      projected_tonnes: s.projected_tonnes,
      impact_tonnes: s.impact_tonnes,
      impact_pct: s.impact_pct,
      series: resultData?.series || [],
    };
  });

  const bestImpact = comparison.reduce((a, b) =>
    parseFloat(a.impact_pct) < parseFloat(b.impact_pct) ? b : a
  );

  const worstImpact = comparison.reduce((a, b) =>
    parseFloat(a.impact_pct) > parseFloat(b.impact_pct) ? b : a
  );

  const allYears = [...new Set(comparison.flatMap((s) => s.series.map((p) => p.year)))].sort();

  const yearByYear = allYears.map((year) => {
    const entry = { year };
    comparison.forEach((s) => {
      const point = s.series.find((p) => p.year === year);
      if (point) {
        entry[s.name] = point.projected_disposal;
      }
    });
    return entry;
  });

  return {
    scenarios: comparison,
    summary: {
      best_scenario: bestImpact.name,
      best_impact_pct: bestImpact.impact_pct,
      worst_scenario: worstImpact.name,
      worst_impact_pct: worstImpact.impact_pct,
      spread: (parseFloat(bestImpact.impact_pct) - parseFloat(worstImpact.impact_pct)).toFixed(2),
    },
    year_by_year: yearByYear,
  };
}

module.exports = { runSimulation, getScenarios, deleteScenario, compareScenarios };
