const { query } = require('../models/db');
const logger = require('../utils/logger');

const RECYCLING_CENTER_MIN_DISTANCE_KM = 5;
const BIN_PLACEMENT_RADIUS_KM = 2;

function haversineDistance(lat1, lng1, lat2, lng2) {
  const R = 6371;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLng = ((lng2 - lng1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLng / 2) *
      Math.sin(dLng / 2);
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

async function getRecyclingCenterLocations() {
  const hotspots = await query(`
    SELECT l.region, l.latitude, l.longitude, l.severity,
           COALESCE(SUM(e.disposal_amount_tonnes), 0) AS total_disposal,
           COUNT(e.id) AS data_points
    FROM locations l
    LEFT JOIN ewaste_data e ON e.region = l.region
    GROUP BY l.id, l.latitude, l.longitude, l.region, l.severity
    HAVING total_disposal > 0
    ORDER BY total_disposal DESC
  `);

  if (hotspots.length === 0) {
    return { centers: [], message: 'No waste data available. Upload data first.' };
  }

  const centers = [];
  const placedCoords = [];

  for (const spot of hotspots) {
    const lat = parseFloat(spot.latitude);
    const lng = parseFloat(spot.longitude);
    const disposal = parseFloat(spot.total_disposal);

    let tooClose = false;
    for (const placed of placedCoords) {
      if (haversineDistance(lat, lng, placed.lat, placed.lng) < RECYCLING_CENTER_MIN_DISTANCE_KM) {
        tooClose = true;
        break;
      }
    }

    if (!tooClose) {
      const priorityScore = disposal * (spot.severity === 'High' ? 3 : spot.severity === 'Medium' ? 2 : 1);
      const capacity = Math.ceil(disposal * 1.5);

      centers.push({
        region: spot.region,
        latitude: lat,
        longitude: lng,
        priority_score: Math.round(priorityScore * 100) / 100,
        estimated_capacity_tonnes: capacity,
        severity: spot.severity,
        rationale: `High waste density area (${disposal.toFixed(1)} tonnes recorded). Optimal for regional recycling hub.`,
      });

      placedCoords.push({ lat, lng });
    }
  }

  centers.sort((a, b) => b.priority_score - a.priority_score);

  logger.info({ centers: centers.length }, 'Recycling center locations calculated');
  return { centers, total_centers: centers.length };
}

async function getBinPlacements() {
  const hotspots = await query(`
    SELECT l.region, l.latitude, l.longitude,
           COALESCE(SUM(e.disposal_amount_tonnes), 0) AS total_disposal,
           l.severity
    FROM locations l
    LEFT JOIN ewaste_data e ON e.region = l.region
    GROUP BY l.id, l.latitude, l.longitude, l.region, l.severity
    HAVING total_disposal > 0
    ORDER BY total_disposal DESC
    LIMIT 20
  `);

  if (hotspots.length === 0) {
    return { placements: [], message: 'No waste data available.' };
  }

  const placements = [];

  for (const spot of hotspots) {
    const lat = parseFloat(spot.latitude);
    const lng = parseFloat(spot.longitude);
    const disposal = parseFloat(spot.total_disposal);

    const binCount = Math.max(1, Math.ceil(disposal / 20));
    const offsets = generateBinOffsets(binCount, BIN_PLACEMENT_RADIUS_KM);

    for (const offset of offsets) {
      placements.push({
        latitude: lat + offset.latOffset,
        longitude: lng + offset.lngOffset,
        bin_type: spot.severity === 'High' ? 'Large (500L)' : spot.severity === 'Medium' ? 'Medium (250L)' : 'Small (120L)',
        region: spot.region,
        priority: spot.severity,
        estimated_fill_rate_days: spot.severity === 'High' ? 7 : spot.severity === 'Medium' ? 14 : 21,
      });
    }
  }

  logger.info({ placements: placements.length }, 'Bin placements calculated');
  return { placements, total_bins: placements.length };
}

function generateBinOffsets(count, radiusKm) {
  const offsets = [];
  const latPerKm = 0.009;
  const lngPerKm = 0.009;

  if (count === 1) {
    offsets.push({ latOffset: 0, lngOffset: 0 });
    return offsets;
  }

  const layers = Math.ceil(Math.sqrt(count));
  let placed = 0;

  for (let i = 0; i < layers && placed < count; i++) {
    const angle = (2 * Math.PI * i) / Math.min(layers, count - placed);
    const r = (radiusKm * (i + 1)) / layers;
    offsets.push({
      latOffset: (r * Math.cos(angle)) * latPerKm,
      lngOffset: (r * Math.sin(angle)) * lngPerKm,
    });
    placed++;
  }

  while (offsets.length < count) {
    const idx = offsets.length % offsets.length;
    offsets.push({
      latOffset: offsets[idx].latOffset + 0.001,
      lngOffset: offsets[idx].lngOffset + 0.001,
    });
  }

  return offsets.slice(0, count);
}

async function getRankedRecommendations(limit = 20) {
  const centersResult = await getRecyclingCenterLocations();
  const binsResult = await getBinPlacements();

  const rankedCenters = (centersResult.centers || []).map((c, i) => ({
    ...c,
    rank: i + 1,
    priority_tier: c.priority_score > 500 ? 'critical' : c.priority_score > 200 ? 'high' : c.priority_score > 50 ? 'medium' : 'low',
    action: c.priority_score > 500
      ? 'Deploy within 30 days'
      : c.priority_score > 200
        ? 'Deploy within 90 days'
        : 'Plan for next quarter',
  }));

  const rankedBins = (binsResult.placements || []).slice(0, limit).map((b, i) => ({
    ...b,
    rank: i + 1,
    urgency_score: b.priority === 'High' ? 3 : b.priority === 'Medium' ? 2 : 1,
  }));

  const insights = [];
  const highRiskRegions = await query(
    `SELECT DISTINCT region FROM sustainability_scores WHERE risk_level = 'Red'`
  );

  if (highRiskRegions.length > 0) {
    insights.push({
      type: 'urgent',
      title: 'Critical Regions Detected',
      description: `${highRiskRegions.length} region(s) flagged as high-risk. Prioritize recycling center deployment.`,
      regions: highRiskRegions.map((r) => r.region),
    });
  }

  const topHotspots = await query(
    `SELECT l.region, SUM(e.disposal_amount_tonnes) AS total
     FROM locations l
     JOIN ewaste_data e ON e.region = l.region
     GROUP BY l.region
     ORDER BY total DESC
     LIMIT 3`
  );

  if (topHotspots.length > 0) {
    insights.push({
      type: 'recommendation',
      title: 'Top Priority Zones',
      description: `Focus collection efforts on: ${topHotspots.map((h) => `${h.region} (${parseFloat(h.total).toFixed(0)}t)`).join(', ')}.`,
    });
  }

  return {
    ranked_centers: rankedCenters,
    ranked_bins: rankedBins,
    insights,
    summary: {
      total_centers: rankedCenters.length,
      total_bins: rankedBins.length,
      critical_centers: rankedCenters.filter((c) => c.priority_tier === 'critical').length,
      high_priority_bins: rankedBins.filter((b) => b.urgency_score === 3).length,
    },
  };
}

async function getRecommendations() {
  const [centersResult, binsResult] = await Promise.all([
    getRecyclingCenterLocations(),
    getBinPlacements(),
  ]);

  const insights = [];
  const highRiskRegions = await query(
    `SELECT DISTINCT region FROM sustainability_scores WHERE risk_level = 'Red'`
  );

  if (highRiskRegions.length > 0) {
    insights.push({
      type: 'urgent',
      title: 'Critical Regions Detected',
      description: `${highRiskRegions.length} region(s) flagged as high-risk. Prioritize recycling center deployment.`,
      regions: highRiskRegions.map((r) => r.region),
    });
  }

  const topHotspots = await query(
    `SELECT l.region, SUM(e.disposal_amount_tonnes) AS total
     FROM locations l
     JOIN ewaste_data e ON e.region = l.region
     GROUP BY l.region
     ORDER BY total DESC
     LIMIT 3`
  );

  if (topHotspots.length > 0) {
    insights.push({
      type: 'recommendation',
      title: 'Top Priority Zones',
      description: `Focus collection efforts on: ${topHotspots.map((h) => `${h.region} (${parseFloat(h.total).toFixed(0)}t)`).join(', ')}.`,
    });
  }

  return {
    recycling_centers: centersResult,
    bin_placements: binsResult,
    ai_insights: insights,
  };
}

module.exports = {
  getRecyclingCenterLocations,
  getBinPlacements,
  getRecommendations,
  getRankedRecommendations,
  haversineDistance,
};
