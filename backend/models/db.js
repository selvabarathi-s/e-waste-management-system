const sqlite3 = require('sqlite3').verbose();
const { open } = require('sqlite');

let db;

async function query(sql, params = []) {
  if (!db) await initDB();
  const upperSql = sql.trim().toUpperCase();
  if (upperSql.startsWith('SELECT') || upperSql.startsWith('PRAGMA')) {
    return await db.all(sql, params);
  } else {
    return await db.run(sql, params);
  }
}

async function transaction(callback) {
  if (!db) await initDB();
  await db.exec('BEGIN TRANSACTION');
  try {
    const txQuery = async (sql, params = []) => {
      const upperSql = sql.trim().toUpperCase();
      if (upperSql.startsWith('SELECT') || upperSql.startsWith('PRAGMA')) {
        return await db.all(sql, params);
      } else {
        return await db.run(sql, params);
      }
    };
    const result = await callback(txQuery);
    await db.exec('COMMIT');
    return result;
  } catch (err) {
    await db.exec('ROLLBACK');
    throw err;
  }
}

async function migrateColumns() {
  const alters = [
    "ALTER TABLE ewaste_data ADD COLUMN region_id INTEGER NULL",
    "ALTER TABLE ewaste_data ADD COLUMN device_category VARCHAR(64) DEFAULT 'General'",
    "ALTER TABLE predictions ADD COLUMN device_category VARCHAR(64) NULL",
    "ALTER TABLE predictions ADD COLUMN model_version VARCHAR(64) NULL",
    "ALTER TABLE predictions ADD COLUMN metric_snapshot JSON NULL",
    "ALTER TABLE predictions ADD COLUMN model_type VARCHAR(32) DEFAULT 'tabular'",
    "ALTER TABLE locations ADD COLUMN region_id INTEGER NULL",
  ];
  for (const sql of alters) {
    try {
      await db.run(sql);
    } catch (e) {
      if (!e.message.includes('duplicate column name')) {
        console.warn('Migration note:', sql, e.message);
      }
    }
  }
}

async function initDB() {
  try {
    db = await open({
      filename: './database.sqlite',
      driver: sqlite3.Database
    });

    await db.exec('PRAGMA foreign_keys = ON');

    await db.exec(`
      CREATE TABLE IF NOT EXISTS regions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(255) NOT NULL UNIQUE,
        latitude DECIMAL(10,6) NOT NULL,
        longitude DECIMAL(10,6) NOT NULL,
        admin_level VARCHAR(64) NULL,
        external_id VARCHAR(128) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await db.exec(`
      CREATE TABLE IF NOT EXISTS ewaste_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        region VARCHAR(255) NOT NULL,
        year INTEGER NOT NULL,
        sales_import_tonnes DECIMAL(10,2) NOT NULL,
        population_millions DECIMAL(10,2) NOT NULL,
        disposal_amount_tonnes DECIMAL(10,2) NOT NULL,
        region_id INTEGER NULL,
        device_category VARCHAR(64) DEFAULT 'General',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    await db.exec('CREATE INDEX IF NOT EXISTS idx_ewaste_year ON ewaste_data(year)');
    await db.exec('CREATE INDEX IF NOT EXISTS idx_ewaste_region ON ewaste_data(region)');
    await db.exec('CREATE INDEX IF NOT EXISTS idx_ewaste_region_id ON ewaste_data(region_id)');

    await db.exec(`
      CREATE TABLE IF NOT EXISTS predictions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        region VARCHAR(255) NOT NULL,
        forecast_year INTEGER NOT NULL,
        predicted_tonnes DECIMAL(10,2) NOT NULL,
        device_category VARCHAR(64) NULL,
        model_version VARCHAR(64) NULL,
        metric_snapshot JSON NULL,
        model_type VARCHAR(32) DEFAULT 'tabular',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    await db.exec('CREATE INDEX IF NOT EXISTS idx_pred_year ON predictions(forecast_year)');
    await db.exec('CREATE INDEX IF NOT EXISTS idx_pred_region ON predictions(region)');

    await db.exec(`
      CREATE TABLE IF NOT EXISTS locations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        region VARCHAR(255) NOT NULL,
        latitude DECIMAL(10,6) NOT NULL,
        longitude DECIMAL(10,6) NOT NULL,
        severity VARCHAR(50) NOT NULL,
        region_id INTEGER NULL
      )
    `);
    await db.exec('CREATE INDEX IF NOT EXISTS idx_loc_region ON locations(region)');

    await db.exec(`
      CREATE TABLE IF NOT EXISTS device_sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        region_id INTEGER NOT NULL,
        year INTEGER NOT NULL,
        device_category VARCHAR(128) NOT NULL,
        units_sold DECIMAL(14,2) NULL,
        revenue DECIMAL(14,2) NULL,
        source VARCHAR(255) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (region_id) REFERENCES regions(id) ON DELETE CASCADE
      )
    `);
    await db.exec('CREATE INDEX IF NOT EXISTS idx_ds_region_year ON device_sales(region_id, year)');

    await db.exec(`
      CREATE TABLE IF NOT EXISTS trade_flows (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        region_id INTEGER NOT NULL,
        year INTEGER NOT NULL,
        flow_type VARCHAR(32) NOT NULL,
        category_or_hs VARCHAR(128) NOT NULL,
        tonnes DECIMAL(14,4) NOT NULL,
        value_optional DECIMAL(14,2) NULL,
        source VARCHAR(255) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (region_id) REFERENCES regions(id) ON DELETE CASCADE
      )
    `);
    await db.exec('CREATE INDEX IF NOT EXISTS idx_tf_region_year ON trade_flows(region_id, year)');

    await db.exec(`
      CREATE TABLE IF NOT EXISTS region_demographics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        region_id INTEGER NOT NULL,
        year INTEGER NOT NULL,
        population DECIMAL(14,4) NOT NULL,
        urban_density_pct DECIMAL(6,2) NULL,
        data_source VARCHAR(255) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(region_id, year),
        FOREIGN KEY (region_id) REFERENCES regions(id) ON DELETE CASCADE
      )
    `);

    await db.exec(`
      CREATE TABLE IF NOT EXISTS device_lifespan_assumptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        device_category VARCHAR(128) NOT NULL,
        mean_lifespan_years DECIMAL(6,2) NOT NULL,
        stdev_years DECIMAL(6,2) NULL,
        effective_from_year INTEGER NOT NULL,
        source VARCHAR(255) NULL
      )
    `);
    await db.exec('CREATE INDEX IF NOT EXISTS idx_lifespan_cat ON device_lifespan_assumptions(device_category)');

    await db.exec(`
      CREATE TABLE IF NOT EXISTS cv_classifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename VARCHAR(255) NOT NULL,
        predicted_class VARCHAR(64) NOT NULL,
        confidence DECIMAL(5,4) NOT NULL,
        is_ewaste BOOLEAN DEFAULT 1,
        device_category VARCHAR(64) NULL,
        estimated_weight_kg DECIMAL(8,2) NULL,
        all_probabilities JSON NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    await db.exec('CREATE INDEX IF NOT EXISTS idx_cv_class ON cv_classifications(predicted_class)');
    await db.exec('CREATE INDEX IF NOT EXISTS idx_cv_created ON cv_classifications(created_at)');

    // New Role Tables
    await db.exec(`
      CREATE TABLE IF NOT EXISTS customer_devices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username VARCHAR(128) NULL,
        device_type VARCHAR(64) NOT NULL,
        age_years INTEGER NOT NULL,
        condition_status VARCHAR(64) NOT NULL,
        ai_suggestion VARCHAR(255) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await db.exec(`
      CREATE TABLE IF NOT EXISTS hub_inventory (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id VARCHAR(64) NOT NULL,
        source VARCHAR(128) NOT NULL,
        category VARCHAR(128) NOT NULL,
        weight_kg DECIMAL(10,2) NOT NULL,
        ai_classification VARCHAR(64) NULL,
        status VARCHAR(64) DEFAULT 'Pending Classification',
        destination VARCHAR(128) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await db.exec(`
      CREATE TABLE IF NOT EXISTS service_jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_type VARCHAR(64) NOT NULL,
        device_or_material VARCHAR(128) NOT NULL,
        issue_or_details VARCHAR(255) NULL,
        weight_kg DECIMAL(10,2) NULL,
        status VARCHAR(64) DEFAULT 'Pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await migrateColumns();

    await db.run(`
      INSERT OR IGNORE INTO regions (name, latitude, longitude, admin_level) VALUES 
      ('Pollachi, Tamil Nadu', 10.6609, 77.0048, 'town'),
      ('Gandhipuram, Coimbatore', 11.0183, 76.9682, 'town'),
      ('Saravanampatti, Coimbatore', 11.077, 77.0163, 'town'),
      ('RS Puram, Coimbatore', 11.0089, 76.9507, 'town'),
      ('Ukkadam, Coimbatore', 10.9954, 76.9601, 'town')
    `);

    // locations lack a UNIQUE constraint on region, so INSERT OR IGNORE won't prevent dupes.
    // Let's do a simple count check
    const locCount = await db.all('SELECT COUNT(*) as c FROM locations');
    if (locCount[0].c === 0) {
      await db.run(`
        INSERT INTO locations (region, latitude, longitude, severity) VALUES 
        ('Pollachi, Tamil Nadu', 10.6609, 77.0048, 'High'),
        ('Gandhipuram, Coimbatore', 11.0183, 76.9682, 'Critical'),
        ('Saravanampatti, Coimbatore', 11.077, 77.0163, 'Medium'),
        ('RS Puram, Coimbatore', 11.0089, 76.9507, 'High'),
        ('Ukkadam, Coimbatore', 10.9954, 76.9601, 'Critical')
      `);

      await db.run(`
        INSERT INTO ewaste_data (region, year, sales_import_tonnes, population_millions, disposal_amount_tonnes, device_category) VALUES 
        ('Gandhipuram, Coimbatore', 2026, 120, 1.2, 85, 'General'),
        ('Saravanampatti, Coimbatore', 2026, 90, 0.8, 60, 'General'),
        ('RS Puram, Coimbatore', 2026, 150, 1.5, 110, 'General'),
        ('Ukkadam, Coimbatore', 2026, 200, 2.0, 140, 'General')
      `);
    }

    console.log('Database & tables initialized successfully with SQLite.');
  } catch (error) {
    console.error('Failed to initialize database:', error);
    process.exit(1);
  }
}

module.exports = {
  initDB,
  query,
  transaction,
};
