const { query } = require('../models/db');

async function migrateSimulationTables() {
  const creates = [
    {
      sql: `CREATE TABLE IF NOT EXISTS simulation_scenarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(255) NOT NULL,
        description TEXT NULL,
        sales_change_pct DECIMAL(8,2) NOT NULL DEFAULT 0,
        recycling_rate_change DECIMAL(8,2) NOT NULL DEFAULT 0,
        policy_factor DECIMAL(8,2) NOT NULL DEFAULT 1.0,
        forecast_horizon_years INTEGER NOT NULL DEFAULT 5,
        baseline_tonnes DECIMAL(14,2) NULL,
        projected_tonnes DECIMAL(14,2) NULL,
        impact_tonnes DECIMAL(14,2) NULL,
        impact_pct DECIMAL(8,2) NULL,
        result_data JSON NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,
      indexes: ['CREATE INDEX IF NOT EXISTS idx_sim_created ON simulation_scenarios(created_at)'],
    },
    {
      sql: `CREATE TABLE IF NOT EXISTS sustainability_scores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        region VARCHAR(255) NOT NULL,
        region_id INTEGER NULL,
        year INTEGER NOT NULL,
        waste_generated_tonnes DECIMAL(14,2) NOT NULL DEFAULT 0,
        estimated_recycled_tonnes DECIMAL(14,2) NOT NULL DEFAULT 0,
        recycling_rate_pct DECIMAL(6,2) NOT NULL DEFAULT 0,
        risk_level VARCHAR(16) NOT NULL DEFAULT 'Green',
        sustainability_score DECIMAL(6,2) NOT NULL DEFAULT 0,
        population_density DECIMAL(10,2) NULL,
        per_capita_waste DECIMAL(10,4) NULL,
        score_details JSON NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(region, year)
      )`,
      indexes: [
        'CREATE INDEX IF NOT EXISTS idx_sustain_region ON sustainability_scores(region)',
        'CREATE INDEX IF NOT EXISTS idx_sustain_risk ON sustainability_scores(risk_level)',
      ],
    },
    {
      sql: `CREATE TABLE IF NOT EXISTS alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        region VARCHAR(255) NOT NULL,
        alert_type VARCHAR(64) NOT NULL,
        severity VARCHAR(16) NOT NULL DEFAULT 'warning',
        message TEXT NOT NULL,
        metric_name VARCHAR(128) NULL,
        metric_value DECIMAL(14,4) NULL,
        threshold_value DECIMAL(14,4) NULL,
        acknowledged INTEGER DEFAULT 0,
        acknowledged_at TIMESTAMP NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )`,
      indexes: [
        'CREATE INDEX IF NOT EXISTS idx_alert_region ON alerts(region)',
        'CREATE INDEX IF NOT EXISTS idx_alert_severity ON alerts(severity)',
        'CREATE INDEX IF NOT EXISTS idx_alert_ack ON alerts(acknowledged)',
      ],
    },
  ];

  for (const { sql, indexes } of creates) {
    try {
      await query(sql);
      for (const idx of indexes || []) {
        await query(idx);
      }
    } catch (e) {
      if (!e.message?.includes('already exists')) {
        console.warn('Simulation migration note:', e.message);
      }
    }
  }

  console.log('Simulation & sustainability tables initialized');
}

module.exports = { migrateSimulationTables };

