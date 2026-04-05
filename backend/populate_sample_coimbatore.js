const mysql = require('mysql2/promise');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });

async function populate() {
  const connection = await mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
  });

  console.log("Connected to database. Populating sample data...");

  // 1. Clear existing sample data to avoid duplicates (optional, but good for demo)
  await connection.query('DELETE FROM ewaste_data');
  await connection.query('DELETE FROM locations');
  await connection.query('DELETE FROM predictions');

  // 2. Add Locations around Coimbatore
  const locations = [
    { region: 'Gandhipuram, Coimbatore', lat: 11.0183, lng: 76.9682, severity: 'High' },
    { region: 'Saravanampatti, Coimbatore', lat: 11.0770, lng: 77.0163, severity: 'Medium' },
    { region: 'Peelamedu, Coimbatore', lat: 11.0267, lng: 77.0118, severity: 'High' },
    { region: 'RS Puram, Coimbatore', lat: 11.0089, lng: 76.9507, severity: 'Low' },
    { region: 'Thudiyalur, Coimbatore', lat: 11.0725, lng: 76.9405, severity: 'Medium' }
  ];

  for (const loc of locations) {
    await connection.query(
      'INSERT INTO locations (region, latitude, longitude, severity) VALUES (?, ?, ?, ?)',
      [loc.region, loc.lat, loc.lng, loc.severity]
    );
  }
  console.log("Added 5 locations in Coimbatore.");

  // 3. Add Data from CSVs
  const sampleFolder = path.join(__dirname, '..', 'sample_data');
  const files = ['gandhipuram.csv', 'saravanampatti.csv', 'peelamedu.csv', 'rspuram.csv', 'thudiyalur.csv'];

  for (const file of files) {
    const filePath = path.join(sampleFolder, file);
    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.split('\n').filter(l => l.trim());
    const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
    
    for (let i = 1; i < lines.length; i++) {
        const values = lines[i].split(',').map(v => v.trim().replace(/^"|"$/g, ''));
        const region = values[0];
        const year = parseInt(values[1]);
        const sales = parseFloat(values[2]);
        const pop = parseFloat(values[3]);
        const disposal = parseFloat(values[4]);

        await connection.query(
            'INSERT INTO ewaste_data (region, year, sales_import_tonnes, population_millions, disposal_amount_tonnes) VALUES (?, ?, ?, ?, ?)',
            [region, year, sales, pop, disposal]
        );
    }
    console.log(`Imported data from ${file}`);
  }

  await connection.end();
  console.log("Population complete!");
}

populate().catch(console.error);
