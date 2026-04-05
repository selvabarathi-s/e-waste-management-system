# AI-based E-Waste Quantification & Forecasting System
**Detailed Implementation Plan**

---

## 1. Project Overview & Architecture Model
**Problem Statement:** Poor E-waste Quantification and Forecasting.  
**Solution:** Develop AI/ML forecasting models using sales/import data, population trends, and GIS mapping of disposal clusters.

**System Architecture Flow:**
`[React Frontend]` <--(Axios/HTTP)--> `[Node.js Backend]` <--(HTTP)--> `[Flask ML Service]`
                                          |
                                    `[MySQL DB]`

---

## 2. Milestone Phases

### Phase 1: Environment & Repository Setup
- **Goal:** Initialize Git repositories, define folder structures, and install baseline dependencies.
- **Tasks:**
  - Create the exact directory tree (`frontend`, `backend`, `ml-service`).
  - Initialize React app.
  - Initialize Node.js app (`npm init`, install express, mysql2, cors, axios).
  - Initialize Python virtual environment (`python -m venv venv`, install flask, pandas, scikit-learn).
  - Define `package.json` and `requirements.txt`.

### Phase 2: Database Schema & Setup (MySQL)
- **Goal:** Design and set up relational tables to store e-waste data and system predictions.
- **Tasks:**
  - Create `ewaste_data` table: stores historical sales, import details, population inputs, disposal clusters.
  - Create `predictions` table: stores historical model outputs mapped by region and timestamp.
  - Create `locations` table: stores lat/long GIS data for e-waste hotspots.
  - Implement database connection logic in `backend/models/db.js`.

### Phase 3: AI/ML Service Development (Python + Flask)
- **Goal:** Build the predictive engine for forecasting future e-waste trends.
- **Tasks:**
  - Clean mock/historical data using **Pandas** (`ml-service/preprocess.py`).
  - Train a predictive model using **Scikit-learn** (e.g., Random Forest or Linear Regression for time-series/population forecasting). Export to `model.pkl`.
  - Create a web server (`ml-service/app.py`) using **Flask**.
  - Expose API Endpoint: `POST /predict` – Accepts JSON payload containing population, sales, and regional data; loads `model.pkl`; and returns forecasted e-waste amounts.

### Phase 4: Node.js Backend API Development
- **Goal:** Develop the main controller that manages data routing between the DB, UI, and ML service.
- **Tasks:**
  - Build `POST /upload`: API to accept raw Excel/CSV data, parse it, and insert it into MySQL.
  - Build `GET /data`: Fetch raw and aggregated data from MySQL for the dashboard.
  - Build `GET /map-data`: Fetch GIS hotspot locations from MySQL.
  - Build `POST /predict`: Receives request from React, makes internal HTTP request to Flask `POST /predict`, stores result in MySQL, and returns it to React.

### Phase 5: React Frontend Development
- **Goal:** Build the interactive UI to visualize forecasts and hotspots.
- **Tasks:**
  - **Upload Component:** Form to upload historical data (CSV/Excel).
  - **Dashboard Component:** Use **Recharts** (or Chart.js) to show time-series trends (Past Data vs. Forecasted Data).
  - **Map Component:** Use **React-Leaflet** to render GIS disposal clusters / e-waste hotspots on a map.
  - **API Integration:** Use **Axios** to connect to Node.js backend. State management using React Hooks.

### Phase 6: System Integration & Testing
- **Goal:** Ensure smooth interoperability under the "Critical Rules" constraints.
- **Validations:**
  - Verify Frontend sends requests strictly to Node.js.
  - Verify Node.js handles data properly before calling Flask.
  - Verify Flask *does not* talk to MySQL.
  - Cross-Origin Resource Sharing (CORS) configuration is valid.
  - E2E testing of the Upload -> Train -> Predict -> Visualize flow.

### Phase 7: Deployment (AWS EC2)
- **Goal:** Deploy the full stack to production.
- **Tasks:**
  - Provision AWS EC2 instance (Ubuntu).
  - Setup AWS RDS for MySQL.
  - Run Node.js with **PM2** (e.g., port 5000).
  - Run Flask with **Gunicorn** (e.g., port 5001).
  - Serve React Frontend (via Nginx/Apache or as static files served by Node).
  - Configure Security Groups (open ports 80, 443; keep database/Flask ports internal).

---

## 3. Communication Rules (System Guardrails)
> [!IMPORTANT]  
> 1. **React** ➔ **Node.js**: Allowed (via REST/Axios)
> 2. **Node.js** ➔ **MySQL**: Allowed (via mysql2 package)
> 3. **Node.js** ➔ **Flask**: Allowed (via internal HTTP/Axios)
> 4. **React** ➔ **Flask**: **DENIED** (Must route through Node.js)
> 5. **Flask** ➔ **MySQL**: **DENIED** (Flask receives inputs and returns outputs statelessly)
