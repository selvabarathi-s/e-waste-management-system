const mysql = require('mysql2/promise');
const config = require('../config/config');

let pool;

/**
 * Runs SQL and returns the row array from mysql2 execute().
 * (Previously returned [rows, null]; callers now receive rows directly.)
 */
async function query(sql, params = []) {
  if (!pool) await initDB();
  const [rows] = await pool.query(sql, params);
  return rows;
}

/**
 * Executes a callback within a database transaction.
 * The callback receives a transaction-aware query function.
 */
async function transaction(callback) {
  if (!pool) await initDB();
  const connection = await pool.getConnection();
  await connection.beginTransaction();
  try {
    const txQuery = async (sql, params = []) => {
      const [rows] = await connection.execute(sql, params);
      return rows;
    };
    const result = await callback(txQuery);
    await connection.commit();
    return result;
  } catch (err) {
    await connection.rollback();
    throw err;
  } finally {
    connection.release();
  }
}

async function migrateColumns() {
  const alters = [
    "ALTER TABLE ewaste_data ADD COLUMN region_id INT NULL",
    "ALTER TABLE ewaste_data ADD COLUMN device_category VARCHAR(64) DEFAULT 'General'",
    "ALTER TABLE predictions ADD COLUMN device_category VARCHAR(64) NULL",
    "ALTER TABLE predictions ADD COLUMN model_version VARCHAR(64) NULL",
    "ALTER TABLE predictions ADD COLUMN metric_snapshot JSON NULL",
    "ALTER TABLE predictions ADD COLUMN model_type VARCHAR(32) NULL DEFAULT 'tabular'",
    "ALTER TABLE locations ADD COLUMN region_id INT NULL",
  ];
  for (const sql of alters) {
    try {
      await pool.execute(sql);
    } catch (e) {
      if (e.code !== 'ER_DUP_FIELDNAME') {
        console.warn('Migration note:', sql, e.message);
      }
    }
  }
}

async function initDB() {
  try {
    pool = mysql.createPool({
      host: config.db.host,
      user: config.db.user,
      password: config.db.password,
      database: config.db.database,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
    });

    await pool.execute(`
      CREATE TABLE IF NOT EXISTS regions (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        latitude DECIMAL(10,6) NOT NULL,
        longitude DECIMAL(10,6) NOT NULL,
        admin_level VARCHAR(64) NULL,
        external_id VARCHAR(128) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_regions_name (name)
      )
    `);

    await pool.execute(`
      CREATE TABLE IF NOT EXISTS ewaste_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        region VARCHAR(255) NOT NULL,
        year INT NOT NULL,
        sales_import_tonnes DECIMAL(10,2) NOT NULL,
        population_millions DECIMAL(10,2) NOT NULL,
        disposal_amount_tonnes DECIMAL(10,2) NOT NULL,
        region_id INT NULL,
        device_category VARCHAR(64) DEFAULT 'General',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_ewaste_year (year),
        INDEX idx_ewaste_region (region),
        INDEX idx_ewaste_region_id (region_id)
      )
    `);

    await pool.execute(`
      CREATE TABLE IF NOT EXISTS predictions (
        id INT AUTO_INCREMENT PRIMARY KEY,
        region VARCHAR(255) NOT NULL,
        forecast_year INT NOT NULL,
        predicted_tonnes DECIMAL(10,2) NOT NULL,
        device_category VARCHAR(64) NULL,
        model_version VARCHAR(64) NULL,
        metric_snapshot JSON NULL,
        model_type VARCHAR(32) NULL DEFAULT 'tabular',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_pred_year (forecast_year),
        INDEX idx_pred_region (region)
      )
    `);

    await pool.execute(`
      CREATE TABLE IF NOT EXISTS locations (
        id INT AUTO_INCREMENT PRIMARY KEY,
        region VARCHAR(255) NOT NULL,
        latitude DECIMAL(10,6) NOT NULL,
        longitude DECIMAL(10,6) NOT NULL,
        severity VARCHAR(50) NOT NULL,
        region_id INT NULL,
        INDEX idx_loc_region (region)
      )
    `);

    await pool.execute(`
      CREATE TABLE IF NOT EXISTS device_sales (
        id INT AUTO_INCREMENT PRIMARY KEY,
        region_id INT NOT NULL,
        year INT NOT NULL,
        device_category VARCHAR(128) NOT NULL,
        units_sold DECIMAL(14,2) NULL,
        revenue DECIMAL(14,2) NULL,
        source VARCHAR(255) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_ds_region_year (region_id, year),
        FOREIGN KEY (region_id) REFERENCES regions(id) ON DELETE CASCADE
      )
    `);

    await pool.execute(`
      CREATE TABLE IF NOT EXISTS trade_flows (
        id INT AUTO_INCREMENT PRIMARY KEY,
        region_id INT NOT NULL,
        year INT NOT NULL,
        flow_type ENUM('import','export') NOT NULL,
        category_or_hs VARCHAR(128) NOT NULL,
        tonnes DECIMAL(14,4) NOT NULL,
        value_optional DECIMAL(14,2) NULL,
        source VARCHAR(255) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_tf_region_year (region_id, year),
        FOREIGN KEY (region_id) REFERENCES regions(id) ON DELETE CASCADE
      )
    `);

    await pool.execute(`
      CREATE TABLE IF NOT EXISTS region_demographics (
        id INT AUTO_INCREMENT PRIMARY KEY,
        region_id INT NOT NULL,
        year INT NOT NULL,
        population DECIMAL(14,4) NOT NULL,
        urban_density_pct DECIMAL(6,2) NULL,
        data_source VARCHAR(255) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uq_demo_region_year (region_id, year),
        FOREIGN KEY (region_id) REFERENCES regions(id) ON DELETE CASCADE
      )
    `);

    await pool.execute(`
      CREATE TABLE IF NOT EXISTS device_lifespan_assumptions (
        id INT AUTO_INCREMENT PRIMARY KEY,
        device_category VARCHAR(128) NOT NULL,
        mean_lifespan_years DECIMAL(6,2) NOT NULL,
        stdev_years DECIMAL(6,2) NULL,
        effective_from_year INT NOT NULL,
        source VARCHAR(255) NULL,
        INDEX idx_lifespan_cat (device_category)
      )
    `);

    await pool.execute(`
      CREATE TABLE IF NOT EXISTS cv_classifications (
        id INT AUTO_INCREMENT PRIMARY KEY,
        filename VARCHAR(255) NOT NULL,
        predicted_class VARCHAR(64) NOT NULL,
        confidence DECIMAL(5,4) NOT NULL,
        is_ewaste BOOLEAN DEFAULT TRUE,
        device_category VARCHAR(64) NULL,
        estimated_weight_kg DECIMAL(8,2) NULL,
        all_probabilities JSON NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_cv_class (predicted_class),
        INDEX idx_cv_created (created_at)
      )
    `);

    // New Role Tables
    await pool.execute(`
      CREATE TABLE IF NOT EXISTS customer_devices (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(128) NULL,
        device_type VARCHAR(64) NOT NULL,
        age_years INT NOT NULL,
        condition_status VARCHAR(64) NOT NULL,
        ai_suggestion VARCHAR(255) NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await pool.execute(`
      CREATE TABLE IF NOT EXISTS hub_inventory (
        id INT AUTO_INCREMENT PRIMARY KEY,
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

    await pool.execute(`
      CREATE TABLE IF NOT EXISTS service_jobs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        job_type ENUM('Repair', 'Scrap') NOT NULL,
        device_or_material VARCHAR(128) NOT NULL,
        issue_or_details VARCHAR(255) NULL,
        weight_kg DECIMAL(10,2) NULL,
        status VARCHAR(64) DEFAULT 'Pending',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);

    await migrateColumns();

    await pool.execute(`
      INSERT IGNORE INTO regions (name, latitude, longitude, admin_level) VALUES 
      ('Pollachi, Tamil Nadu', 10.6609, 77.0048, 'town'),
      ('Gandhipuram, Coimbatore', 11.0183, 76.9682, 'town'),
      ('Saravanampatti, Coimbatore', 11.077, 77.0163, 'town'),
      ('RS Puram, Coimbatore', 11.0089, 76.9507, 'town'),
      ('Ukkadam, Coimbatore', 10.9954, 76.9601, 'town')
    `);

    await pool.execute(`
      INSERT IGNORE INTO locations (region, latitude, longitude, severity) VALUES 
      ('Pollachi, Tamil Nadu', 10.6609, 77.0048, 'High'),
      ('Gandhipuram, Coimbatore', 11.0183, 76.9682, 'Critical'),
      ('Saravanampatti, Coimbatore', 11.077, 77.0163, 'Medium'),
      ('RS Puram, Coimbatore', 11.0089, 76.9507, 'High'),
      ('Ukkadam, Coimbatore', 10.9954, 76.9601, 'Critical')
    `);

    // Add some default weights so DBSCAN forms logical dense clusters based on these 5 locations
    await pool.execute(`
      INSERT IGNORE INTO ewaste_data (region, year, sales_import_tonnes, population_millions, disposal_amount_tonnes, device_category) VALUES 
      ('Gandhipuram, Coimbatore', 2026, 120, 1.2, 85, 'General'),
      ('Saravanampatti, Coimbatore', 2026, 90, 0.8, 60, 'General'),
      ('RS Puram, Coimbatore', 2026, 150, 1.5, 110, 'General'),
      ('Ukkadam, Coimbatore', 2026, 200, 2.0, 140, 'General')
    `);

    console.log('Database & tables initialized successfully with MySQL.');
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
