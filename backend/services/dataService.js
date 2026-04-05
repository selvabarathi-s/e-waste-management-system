const csv = require('csv-parser');
const xlsx = require('xlsx');
const { Readable } = require('stream');
const { query } = require('../models/db');

const COIMBATORE_MAP = {
  Gandhipuram: { lat: 11.0183, lng: 76.9682 },
  Saravanampatti: { lat: 11.077, lng: 77.0163 },
  Peelamedu: { lat: 11.0267, lng: 77.0118 },
  'RS Puram': { lat: 11.0089, lng: 76.9507 },
  Thudiyalur: { lat: 11.0725, lng: 76.9405 },
  Singanallur: { lat: 11.0006, lng: 77.0253 },
  Vadavalli: { lat: 11.0232, lng: 76.9024 },
  Kuniyamuthur: { lat: 10.9667, lng: 76.95 },
  Ukkadam: { lat: 10.9954, lng: 76.9601 },
};

const REQUIRED_UPLOAD_FIELDS = [
  'region',
  'year',
  'sales_import_tonnes',
  'population_millions',
  'disposal_amount_tonnes',
];

function normalizeRow(raw) {
  const item = {};
  for (const [k, v] of Object.entries(raw)) {
    item[String(k).trim().toLowerCase().replace(/\s+/g, '_')] =
      v === undefined || v === null ? v : typeof v === 'string' ? v.trim() : v;
  }
  return item;
}

function parseCSV(buffer) {
  return new Promise((resolve, reject) => {
    const results = [];
    const stream = Readable.from(buffer.toString('utf8'));
    stream
      .pipe(csv())
      .on('data', (data) => results.push(normalizeRow(data)))
      .on('end', () => resolve(results))
      .on('error', (err) => reject(err));
  });
}

function parseExcel(buffer) {
  const workbook = xlsx.read(buffer, { type: 'buffer' });
  const sheetName = workbook.SheetNames[0];
  const sheet = workbook.Sheets[sheetName];
  const rows = xlsx.utils.sheet_to_json(sheet);
  return rows.map(normalizeRow);
}

async function findOrCreateRegion(regionName) {
  const name = String(regionName).trim();
  const rows = await query('SELECT id, latitude, longitude FROM regions WHERE name = ?', [name]);
  if (rows.length > 0) {
    return rows[0];
  }
  const coords = COIMBATORE_MAP[name] || {
    lat: 11.0168 + (Math.random() - 0.5) * 0.08,
    lng: 76.9558 + (Math.random() - 0.5) * 0.08,
  };
  const r = await query(
    'INSERT INTO regions (name, latitude, longitude, admin_level) VALUES (?, ?, ?, ?)',
    [name, coords.lat, coords.lng, 'area']
  );
  return { id: r.insertId, latitude: coords.lat, longitude: coords.lng };
}

async function ensureLocationForRegion(regionName, regionRow) {
  const region = String(regionName).trim();
  const existing = await query('SELECT id FROM locations WHERE region = ?', [region]);
  if (existing.length > 0) {
    if (regionRow && regionRow.id) {
      await query('UPDATE locations SET region_id = ? WHERE region = ?', [regionRow.id, region]);
    }
    return;
  }
  const lat = regionRow ? parseFloat(regionRow.latitude) : 11.0168;
  const lng = regionRow ? parseFloat(regionRow.longitude) : 76.9558;
  await query(
    'INSERT INTO locations (region, latitude, longitude, severity, region_id) VALUES (?, ?, ?, ?, ?)',
    [region, lat, lng, 'Medium', regionRow ? regionRow.id : null]
  );
}

async function uploadFromRows(rows) {
  let inserted = 0;
  const uniqueRegions = new Set();
  const regionCache = new Map();

  const validRows = [];
  for (const item of rows) {
    const missing = REQUIRED_UPLOAD_FIELDS.filter(
      (f) => item[f] === undefined || item[f] === null || String(item[f]).trim() === ''
    );
    if (missing.length > 0) continue;

    const year = parseInt(item.year, 10);
    const sales = parseFloat(item.sales_import_tonnes);
    const population = parseFloat(item.population_millions);
    const disposal = parseFloat(item.disposal_amount_tonnes);
    const region = String(item.region).trim();
    const deviceCategory =
      item.device_category && String(item.device_category).trim()
        ? String(item.device_category).trim()
        : 'General';

    if (Number.isNaN(year) || Number.isNaN(sales) || Number.isNaN(population) || Number.isNaN(disposal)) {
      continue;
    }
    if (year < 1900 || year > 2100) continue;
    if (sales < 0 || population < 0 || disposal < 0) continue;

    validRows.push({ region, year, sales, population, disposal, deviceCategory });
    uniqueRegions.add(region);
  }

  for (const region of uniqueRegions) {
    let reg = regionCache.get(region);
    if (!reg) {
      const rows = await query('SELECT id, latitude, longitude FROM regions WHERE name = ?', [region]);
      if (rows.length > 0) {
        reg = rows[0];
      } else {
        const coords = COIMBATORE_MAP[region] || {
          lat: 11.0168 + (Math.random() - 0.5) * 0.08,
          lng: 76.9558 + (Math.random() - 0.5) * 0.08,
        };
        const r = await query(
          'INSERT INTO regions (name, latitude, longitude, admin_level) VALUES (?, ?, ?, ?)',
          [region, coords.lat, coords.lng, 'area']
        );
        reg = { id: r.insertId, latitude: coords.lat, longitude: coords.lng };
      }
      regionCache.set(region, reg);
    }

    await ensureLocationForRegion(region, reg);
  }

  if (validRows.length > 0) {
    const batchSize = 100;
    for (let i = 0; i < validRows.length; i += batchSize) {
      const batch = validRows.slice(i, i + batchSize);
      const placeholders = batch.map(() => '(?, ?, ?, ?, ?, ?, ?)').join(', ');
      const values = batch.flatMap((r) => [
        r.region, r.year, r.sales, r.population, r.disposal,
        regionCache.get(r.region).id, r.deviceCategory,
      ]);
      await query(
        `INSERT INTO ewaste_data (region, year, sales_import_tonnes, population_millions, disposal_amount_tonnes, region_id, device_category) VALUES ${placeholders}`,
        values
      );
      inserted += batch.length;
    }
  }

  return { inserted, regionsTouched: uniqueRegions.size };
}

async function getEwasteData(filters = {}) {
  let sql = `SELECT e.*, r.latitude AS region_lat, r.longitude AS region_lng FROM ewaste_data e
    LEFT JOIN regions r ON e.region_id = r.id WHERE 1=1`;
  const params = [];
  if (filters.year) {
    const y = parseInt(filters.year, 10);
    if (!Number.isNaN(y)) {
      sql += ' AND e.year = ?';
      params.push(y);
    }
  }
  if (filters.region) {
    sql += ' AND e.region LIKE ?';
    params.push(`%${filters.region}%`);
  }
  if (filters.device_category) {
    sql += ' AND e.device_category = ?';
    params.push(filters.device_category);
  }

  const page = Math.max(1, parseInt(filters.page, 10) || 1);
  const limit = Math.min(500, Math.max(1, parseInt(filters.limit, 10) || 100));
  const offset = (page - 1) * limit;

  const [countRows] = await require('../models/db').queryRaw
    ? await require('../models/db').queryRaw(`SELECT COUNT(*) AS total FROM (${sql.replace(/ORDER BY.*$/, '')}) AS t`, params)
    : [{ total: 0 }];

  sql += ' ORDER BY e.year DESC LIMIT ? OFFSET ?';
  const rows = await query(sql, [...params, limit, offset]);

  return {
    data: rows,
    pagination: {
      page,
      limit,
      total: countRows.total || rows.length,
      hasMore: rows.length === limit,
    },
  };
}

async function getLocations() {
  return query(`SELECT * FROM locations`);
}

async function getHeatmapPoints() {
  return query(`
    SELECT l.latitude AS lat, l.longitude AS lng,
           COALESCE(SUM(e.disposal_amount_tonnes), 1) AS weight
    FROM locations l
    LEFT JOIN ewaste_data e ON e.region = l.region
    GROUP BY l.id, l.latitude, l.longitude
  `);
}

async function getClusterInputPoints() {
  const rows = await query(`
    SELECT l.latitude AS lat, l.longitude AS lng, l.region,
           COALESCE(SUM(e.disposal_amount_tonnes), 0) AS weight
    FROM locations l
    LEFT JOIN ewaste_data e ON e.region = l.region
    GROUP BY l.id, l.latitude, l.longitude, l.region
  `);
  return rows.map((r) => ({
    lat: parseFloat(r.lat),
    lng: parseFloat(r.lng),
    weight: Math.max(parseFloat(r.weight) || 0, 0.01),
    region: r.region,
  }));
}

async function validateData(rows) {
  const errors = [];
  const warnings = [];
  const stats = {
    total_rows: rows.length,
    valid_rows: 0,
    invalid_rows: 0,
    unique_regions: new Set(),
    year_range: { min: Infinity, max: -Infinity },
    device_categories: new Set(),
    total_sales: 0,
    total_disposal: 0,
  };

  for (let i = 0; i < rows.length; i++) {
    const item = rows[i];
    const rowErrors = [];
    const rowWarnings = [];

    const missing = REQUIRED_UPLOAD_FIELDS.filter(
      (f) => item[f] === undefined || item[f] === null || String(item[f]).trim() === ''
    );

    if (missing.length > 0) {
      rowErrors.push(`Missing required fields: ${missing.join(', ')}`);
    }

    const year = parseInt(item.year, 10);
    const sales = parseFloat(item.sales_import_tonnes);
    const population = parseFloat(item.population_millions);
    const disposal = parseFloat(item.disposal_amount_tonnes);

    if (!Number.isNaN(year)) {
      if (year < 1900 || year > 2100) {
        rowErrors.push(`Year ${year} out of valid range (1900-2100)`);
      } else {
        stats.year_range.min = Math.min(stats.year_range.min, year);
        stats.year_range.max = Math.max(stats.year_range.max, year);
      }
    } else if (item.year !== undefined) {
      rowErrors.push(`Invalid year value: ${item.year}`);
    }

    if (!Number.isNaN(sales)) {
      if (sales < 0) {
        rowErrors.push(`Negative sales value: ${sales}`);
      } else {
        stats.total_sales += sales;
      }
    } else if (item.sales_import_tonnes !== undefined) {
      rowErrors.push(`Invalid sales value: ${item.sales_import_tonnes}`);
    }

    if (!Number.isNaN(population)) {
      if (population < 0) rowErrors.push(`Negative population: ${population}`);
    }

    if (!Number.isNaN(disposal)) {
      if (disposal < 0) {
        rowErrors.push(`Negative disposal value: ${disposal}`);
      } else {
        stats.total_disposal += disposal;
      }
    }

    if (item.region && String(item.region).trim()) {
      const region = String(item.region).trim();
      stats.unique_regions.add(region);
    }

    const deviceCategory = item.device_category && String(item.device_category).trim()
      ? String(item.device_category).trim()
      : null;
    if (deviceCategory) {
      stats.device_categories.add(deviceCategory);
    }

    if (rowErrors.length > 0) {
      errors.push({ row: i + 1, errors: rowErrors });
      stats.invalid_rows++;
    } else {
      stats.valid_rows++;
    }

    if (rowWarnings.length > 0) {
      warnings.push({ row: i + 1, warnings: rowWarnings });
    }
  }

  if (stats.year_range.min === Infinity) {
    stats.year_range = { min: 0, max: 0 };
  }

  return {
    is_valid: errors.length === 0,
    errors,
    warnings,
    stats: {
      ...stats,
      unique_regions: Array.from(stats.unique_regions),
      device_categories: Array.from(stats.device_categories),
      total_sales: Math.round(stats.total_sales * 100) / 100,
      total_disposal: Math.round(stats.total_disposal * 100) / 100,
    },
    preview: rows.slice(0, 5).map((r, i) => ({
      row: i + 1,
      region: r.region || '—',
      year: r.year || '—',
      sales: r.sales_import_tonnes || '—',
      disposal: r.disposal_amount_tonnes || '—',
    })),
  };
}

module.exports = {
  parseCSV,
  parseExcel,
  uploadFromRows,
  getEwasteData,
  getLocations,
  getHeatmapPoints,
  getClusterInputPoints,
  validateData,
  COIMBATORE_MAP,
};
