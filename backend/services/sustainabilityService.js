const { query } = require('../models/db');
const logger = require('../utils/logger');

const RISK_THRESHOLDS = {
  green: 60,
  yellow: 35,
};

function calculateRiskLevel(score) {
  if (score >= RISK_THRESHOLDS.green) return 'Green';
  if (score >= RISK_THRESHOLDS.yellow) return 'Yellow';
  return 'Red';
}

function calculateSustainabilityScore({
  wasteGenerated,
  estimatedRecycled,
  population,
  areaKm2,
}) {
  const recyclingRate = wasteGenerated > 0 ? (estimatedRecycled / wasteGenerated) * 100 : 0;
  const popDensity = areaKm2 > 0 ? population / areaKm2 : 0;
  const perCapitaWaste = population > 0 ? (wasteGenerated / (population * 1e6)) : 0;

  let score = 0;
  score += Math.min(recyclingRate * 0.5, 50);
  score += perCapitaWaste < 0.5 ? 25 : perCapitaWaste < 1 ? 15 : 5;
  score += popDensity < 500 ? 15 : popDensity < 2000 ? 10 : 5;
  score = Math.max(0, Math.min(100, score));

  const riskLevel = calculateRiskLevel(score);

  return {
    score: Math.round(score * 100) / 100,
    recycling_rate_pct: Math.round(recyclingRate * 100) / 100,
    risk_level: riskLevel,
    per_capita_waste: Math.round(perCapitaWaste * 10000) / 10000,
    population_density: Math.round(popDensity * 100) / 100,
  };
}

async function calculateScoresForAllRegions() {
  const regionStats = await query(`
    SELECT e.region, e.year,
           SUM(e.sales_import_tonnes) AS waste_generated,
           SUM(e.disposal_amount_tonnes) AS disposal,
           AVG(e.population_millions) AS population_millions,
           r.latitude, r.longitude
    FROM ewaste_data e
    LEFT JOIN regions r ON e.region = r.name
    GROUP BY e.region, e.year
    ORDER BY e.region, e.year
  `);

  const populationByRegion = await query(`
    SELECT r.name AS region, AVG(rd.population) AS avg_population
    FROM regions r
    LEFT JOIN region_demographics rd ON r.id = rd.region_id
    GROUP BY r.name
  `);

  const popMap = {};
  for (const row of populationByRegion) {
    popMap[row.region] = parseFloat(row.avg_population) || 0;
  }

  const REGION_AREA_KM2 = {
    'Pollachi, Tamil Nadu': 800,
    'Coimbatore, Tamil Nadu': 247,
  };

  const results = [];
  for (const stat of regionStats) {
    const region = stat.region;
    const year = stat.year;
    const wasteGenerated = parseFloat(stat.waste_generated) || 0;
    const disposal = parseFloat(stat.disposal) || 0;
    const estimatedRecycled = Math.max(0, wasteGenerated - disposal);
    const population = popMap[region] || parseFloat(stat.population_millions) || 0.1;
    const areaKm2 = REGION_AREA_KM2[region] || 500;

    const metrics = calculateSustainabilityScore({
      wasteGenerated,
      estimatedRecycled,
      population,
      areaKm2,
    });

    await query(
      `INSERT INTO sustainability_scores
       (region, region_id, year, waste_generated_tonnes, estimated_recycled_tonnes,
        recycling_rate_pct, risk_level, sustainability_score, population_density,
        per_capita_waste, score_details)
       VALUES (?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         waste_generated_tonnes = VALUES(waste_generated_tonnes),
         estimated_recycled_tonnes = VALUES(estimated_recycled_tonnes),
         recycling_rate_pct = VALUES(recycling_rate_pct),
         risk_level = VALUES(risk_level),
         sustainability_score = VALUES(sustainability_score),
         population_density = VALUES(population_density),
         per_capita_waste = VALUES(per_capita_waste),
         score_details = VALUES(score_details)`,
      [
        region,
        year,
        wasteGenerated,
        estimatedRecycled,
        metrics.recycling_rate_pct,
        metrics.risk_level,
        metrics.score,
        metrics.population_density,
        metrics.per_capita_waste,
        JSON.stringify({
          waste_generated: wasteGenerated,
          disposal: disposal,
          estimated_recycled: estimatedRecycled,
          population_millions: population,
        }),
      ]
    );

    results.push({
      region,
      year,
      ...metrics,
      waste_generated_tonnes: wasteGenerated,
      estimated_recycled_tonnes: estimatedRecycled,
    });
  }

  logger.info({ regions: results.length }, 'Sustainability scores calculated');
  return results;
}

async function getAllScores() {
  return query('SELECT * FROM sustainability_scores ORDER BY sustainability_score ASC');
}

async function getRegionScore(region) {
  const rows = await query(
    'SELECT * FROM sustainability_scores WHERE region LIKE ? ORDER BY year DESC',
    [`%${region}%`]
  );
  if (rows.length === 0) {
    return null;
  }
  return rows;
}

async function generateAlerts() {
  const scores = await getAllScores();
  const alerts = [];

  for (const score of scores) {
    if (score.risk_level === 'Red') {
      const existing = await query(
        'SELECT id FROM alerts WHERE region = ? AND alert_type = ? AND year(created_at) = YEAR(CURRENT_DATE) AND acknowledged = 0',
        [score.region, 'HIGH_RISK']
      );
      if (existing.length === 0) {
        await query(
          `INSERT INTO alerts (region, alert_type, severity, message, metric_name, metric_value, threshold_value)
           VALUES (?, ?, 'critical', ?, 'sustainability_score', ?, ?)`,
          [
            score.region,
            'HIGH_RISK',
            `Critical: ${score.region} sustainability score is ${score.sustainability_score}/100. Immediate action required.`,
            score.sustainability_score,
            RISK_THRESHOLDS.yellow,
          ]
        );
        alerts.push({ region: score.region, type: 'critical' });
      }
    }

    if (score.recycling_rate_pct < 20) {
      const existing = await query(
        'SELECT id FROM alerts WHERE region = ? AND alert_type = ? AND year(created_at) = YEAR(CURRENT_DATE) AND acknowledged = 0',
        [score.region, 'LOW_RECYCLING']
      );
      if (existing.length === 0) {
        await query(
          `INSERT INTO alerts (region, alert_type, severity, message, metric_name, metric_value, threshold_value)
           VALUES (?, ?, 'warning', ?, 'recycling_rate', ?, ?)`,
          [
            score.region,
            'LOW_RECYCLING',
            `Low recycling rate in ${score.region}: ${score.recycling_rate_pct}%. Target: 20%.`,
            score.recycling_rate_pct,
            20,
          ]
        );
        alerts.push({ region: score.region, type: 'warning' });
      }
    }
  }

  return alerts;
}

async function getTrendAnalysis(region) {
  const scores = await query(
    'SELECT * FROM sustainability_scores WHERE region LIKE ? ORDER BY year ASC',
    region ? [`%${region}%`] : ['%%']
  );

  const regionMap = {};
  for (const s of scores) {
    if (!regionMap[s.region]) regionMap[s.region] = [];
    regionMap[s.region].push({
      year: s.year,
      score: parseFloat(s.sustainability_score),
      recycling_rate: parseFloat(s.recycling_rate_pct),
      per_capita_waste: parseFloat(s.per_capita_waste),
      risk_level: s.risk_level,
      waste_generated: parseFloat(s.waste_generated_tonnes),
    });
  }

  const result = {};
  for (const [regionName, data] of Object.entries(regionMap)) {
    if (data.length < 2) {
      result[regionName] = { data, trend: 'insufficient_data', message: 'Need at least 2 data points for trend analysis' };
      continue;
    }

    const first = data[0];
    const last = data[data.length - 1];
    const scoreChange = last.score - first.score;
    const recyclingChange = last.recycling_rate - first.recycling_rate;
    const wasteChange = last.waste_generated - first.waste_generated;

    const scoreTrend = scoreChange > 0 ? 'improving' : scoreChange < 0 ? 'declining' : 'stable';
    const recyclingTrend = recyclingChange > 0 ? 'improving' : recyclingChange < 0 ? 'declining' : 'stable';

    const avgScore = data.reduce((a, b) => a + b.score, 0) / data.length;
    const avgRecycling = data.reduce((a, b) => a + b.recycling_rate, 0) / data.length;

    result[regionName] = {
      data,
      trend: scoreTrend,
      recycling_trend: recyclingTrend,
      score_change: Math.round(scoreChange * 100) / 100,
      recycling_change: Math.round(recyclingChange * 100) / 100,
      waste_change: Math.round(wasteChange * 100) / 100,
      avg_score: Math.round(avgScore * 100) / 100,
      avg_recycling_rate: Math.round(avgRecycling * 100) / 100,
      data_points: data.length,
      current_risk: last.risk_level,
      forecast: data.length >= 3 ? {
        next_year_score: Math.round((last.score + (scoreChange / (data.length - 1))) * 100) / 100,
        direction: scoreTrend,
      } : null,
    };
  }

  return result;
}

module.exports = {
  calculateScoresForAllRegions,
  getAllScores,
  getRegionScore,
  generateAlerts,
  calculateSustainabilityScore,
  RISK_THRESHOLDS,
  getTrendAnalysis,
};
