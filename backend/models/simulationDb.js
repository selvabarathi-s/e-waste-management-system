const { query } = require('../models/db');

async function migrateSimulationTables() {
  const creates = [
    `CREATE TABLE IF NOT EXISTS simulation_scenarios (
      id INT AUTO_INCREMENT PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      description TEXT NULL,
      sales_change_pct DECIMAL(8,2) NOT NULL DEFAULT 0,
      recycling_rate_change DECIMAL(8,2) NOT NULL DEFAULT 0,
      policy_factor DECIMAL(8,2) NOT NULL DEFAULT 1.0,
      forecast_horizon_years INT NOT NULL DEFAULT 5,
      baseline_tonnes DECIMAL(14,2) NULL,
      projected_tonnes DECIMAL(14,2) NULL,
      impact_tonnes DECIMAL(14,2) NULL,
      impact_pct DECIMAL(8,2) NULL,
      result_data JSON NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_sim_created (created_at)
    )`,
    `CREATE TABLE IF NOT EXISTS sustainability_scores (
      id INT AUTO_INCREMENT PRIMARY KEY,
      region VARCHAR(255) NOT NULL,
      region_id INT NULL,
      year INT NOT NULL,
      waste_generated_tonnes DECIMAL(14,2) NOT NULL DEFAULT 0,
      estimated_recycled_tonnes DECIMAL(14,2) NOT NULL DEFAULT 0,
      recycling_rate_pct DECIMAL(6,2) NOT NULL DEFAULT 0,
      risk_level ENUM('Green', 'Yellow', 'Red') NOT NULL DEFAULT 'Green',
      sustainability_score DECIMAL(6,2) NOT NULL DEFAULT 0,
      population_density DECIMAL(10,2) NULL,
      per_capita_waste DECIMAL(10,4) NULL,
      score_details JSON NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uq_sustain_region_year (region, year),
      INDEX idx_sustain_region (region),
      INDEX idx_sustain_risk (risk_level)
    )`,
    `CREATE TABLE IF NOT EXISTS alerts (
      id INT AUTO_INCREMENT PRIMARY KEY,
      region VARCHAR(255) NOT NULL,
      alert_type VARCHAR(64) NOT NULL,
      severity ENUM('info', 'warning', 'critical') NOT NULL DEFAULT 'warning',
      message TEXT NOT NULL,
      metric_name VARCHAR(128) NULL,
      metric_value DECIMAL(14,4) NULL,
      threshold_value DECIMAL(14,4) NULL,
      acknowledged TINYINT(1) DEFAULT 0,
      acknowledged_at TIMESTAMP NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_alert_region (region),
      INDEX idx_alert_severity (severity),
      INDEX idx_alert_ack (acknowledged)
    )`,
  ];

  for (const sql of creates) {
    try {
      await query(sql);
    } catch (e) {
      if (e.code !== 'ER_DUP_FIELDNAME' && e.code !== 'ER_DUP_KEYNAME') {
        console.warn('Simulation migration note:', sql, e.message);
      }
    }
  }

  console.log('Simulation & sustainability tables initialized');
}

module.exports = { migrateSimulationTables };
