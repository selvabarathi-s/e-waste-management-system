import React, { useState, useEffect } from 'react';
import {
  Shield,
  ShieldCheck,
  ShieldAlert,
  Loader2,
  AlertTriangle,
  TrendingUp,
  Recycle,
  Users,
  RefreshCw,
  Bell,
  BarChart3,
} from 'lucide-react';
import Chart from '../components/Chart';

const RISK_COLORS = {
  Green: '#10b981',
  Yellow: '#f59e0b',
  Red: '#ef4444',
};

const RISK_ICONS = {
  Green: ShieldCheck,
  Yellow: Shield,
  Red: ShieldAlert,
};

const API_BASE = import.meta.env.VITE_API_BASE_URL || 'http://localhost:5000/api';

const Sustainability = () => {
  const [scores, setScores] = useState([]);
  const [loading, setLoading] = useState(true);
  const [recalculating, setRecalculating] = useState(false);
  const [alerts, setAlerts] = useState([]);
  const [selectedRegion, setSelectedRegion] = useState(null);
  const [viewMode, setViewMode] = useState('scores');
  const [trends, setTrends] = useState(null);
  const [trendsLoading, setTrendsLoading] = useState(false);

  const loadScores = async () => {
    try {
      setLoading(true);
      const [scoresRes, alertsRes] = await Promise.all([
        fetch(`${API_BASE}/scores`),
        fetch(`${API_BASE}/recommendations`),
      ]);
      const scoresData = await scoresRes.json();
      if (Array.isArray(scoresData)) setScores(scoresData);

      const recData = await alertsRes.json();
      if (recData.ai_insights) {
        setAlerts(recData.ai_insights);
      }
    } catch (e) {
      console.error('Failed to load scores', e);
    } finally {
      setLoading(false);
    }
  };

  const loadTrends = async () => {
    try {
      setTrendsLoading(true);
      const res = await fetch(`${API_BASE}/scores/trends`);
      const data = await res.json();
      setTrends(data);
    } catch (e) {
      console.error('Failed to load trends', e);
    } finally {
      setTrendsLoading(false);
    }
  };

  const handleRecalculate = async () => {
    try {
      setRecalculating(true);
      const res = await fetch(`${API_BASE}/scores/recalculate`, { method: 'POST' });
      const data = await res.json();
      if (data.scores) setScores(data.scores);
      if (data.alerts && data.alerts.length > 0) {
        setAlerts((prev) => [
          ...data.alerts.map((a) => ({ type: 'urgent', title: a.region, description: 'Risk alert triggered' })),
          ...prev,
        ]);
      }
    } catch (e) {
      console.error('Recalculation failed', e);
    } finally {
      setRecalculating(false);
    }
  };

  useEffect(() => {
    loadScores();
    if (viewMode === 'trends') loadTrends();
  }, []);

  useEffect(() => {
    if (viewMode === 'trends') loadTrends();
  }, [viewMode]);

  const regionSummary = scores.reduce((acc, s) => {
    if (!acc[s.region]) {
      acc[s.region] = { region: s.region, score: 0, risk_level: 'Green', years: 0 };
    }
    acc[s.region].score = Math.max(acc[s.region].score, parseFloat(s.sustainability_score) || 0);
    if (s.risk_level === 'Red') acc[s.region].risk_level = 'Red';
    else if (s.risk_level === 'Yellow' && acc[s.region].risk_level !== 'Red') acc[s.region].risk_level = 'Yellow';
    acc[s.region].years++;
    return acc;
  }, {});

  const regions = Object.values(regionSummary);
  const greenCount = regions.filter((r) => r.risk_level === 'Green').length;
  const yellowCount = regions.filter((r) => r.risk_level === 'Yellow').length;
  const redCount = regions.filter((r) => r.risk_level === 'Red').length;

  const selectedData = selectedRegion ? scores.filter((s) => s.region === selectedRegion) : [];

  const trendChartData = trends && selectedRegion && trends[selectedRegion]
    ? trends[selectedRegion].data.map((d) => ({
        year: d.year,
        score: d.score,
        recycling_rate: d.recycling_rate,
      }))
    : [];

  return (
    <div>
      <div className="page-header fade-in">
        <h2>Sustainability Scores</h2>
        <p>Region-wise sustainability ratings with Green / Yellow / Red risk zones</p>
      </div>

      <div className="card p-3 mb-4 fade-in">
        <div className="btn-group w-100" role="group">
          <button
            className={`btn btn-sm ${viewMode === 'scores' ? 'btn-primary' : 'btn-outline-primary'}`}
            onClick={() => setViewMode('scores')}
          >
            <ShieldCheck size={14} style={{ marginRight: '4px' }} />
            Scores
          </button>
          <button
            className={`btn btn-sm ${viewMode === 'trends' ? 'btn-primary' : 'btn-outline-primary'}`}
            onClick={() => setViewMode('trends')}
          >
            <TrendingUp size={14} style={{ marginRight: '4px' }} />
            Trend Analysis
          </button>
        </div>
      </div>

      {viewMode === 'scores' ? (
        <>
          <div className="d-flex justify-content-end mb-3">
            <button
              className="btn btn-sm btn-primary d-flex align-items-center gap-2"
              onClick={handleRecalculate}
              disabled={recalculating}
            >
              {recalculating ? <Loader2 size={16} className="spinner" /> : <RefreshCw size={16} />}
              {recalculating ? 'Recalculating...' : 'Recalculate Scores'}
            </button>
          </div>

          <div className="row g-3 mb-4">
            <div className="col-md-4">
              <div className="kpi-card">
                <div className="kpi-icon green"><ShieldCheck size={24} /></div>
                <div>
                  <div className="kpi-value" style={{ color: '#10b981' }}>{greenCount}</div>
                  <div className="kpi-label">Green Zones</div>
                </div>
              </div>
            </div>
            <div className="col-md-4">
              <div className="kpi-card">
                <div className="kpi-icon amber"><Shield size={24} /></div>
                <div>
                  <div className="kpi-value" style={{ color: '#d97706' }}>{yellowCount}</div>
                  <div className="kpi-label">Yellow Zones</div>
                </div>
              </div>
            </div>
            <div className="col-md-4">
              <div className="kpi-card">
                <div className="kpi-icon red"><ShieldAlert size={24} /></div>
                <div>
                  <div className="kpi-value" style={{ color: '#dc2626' }}>{redCount}</div>
                  <div className="kpi-label">Red Zones</div>
                </div>
              </div>
            </div>
          </div>

          {alerts.length > 0 && (
            <div className="card p-3 mb-4 fade-in">
              <h4 className="mb-3" style={{ fontSize: '1.05rem', fontWeight: 700, display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                <Bell size={18} style={{ color: '#ef4444' }} />
                AI Insights & Alerts
              </h4>
              {alerts.map((a, i) => (
                <div key={i} className={`p-3 mb-2 rounded ${a.type === 'urgent' ? 'alert-danger-custom' : 'alert-success-custom'}`}>
                  <div className="d-flex align-items-start gap-2">
                    <AlertTriangle size={16} className="mt-1" style={{ flexShrink: 0 }} />
                    <div>
                      <strong style={{ fontSize: '0.9rem' }}>{a.title}</strong>
                      <p className="text-muted small mb-0">{a.description}</p>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}

          {loading ? (
            <div className="spinner-container">
              <Loader2 className="spinner" size={40} />
              <p className="mt-3 mb-0">Loading sustainability scores...</p>
            </div>
          ) : regions.length > 0 ? (
            <div className="row g-4">
              <div className="col-lg-5">
                <div className="card p-3 fade-in">
                  <h4 className="card-title mb-3" style={{ fontSize: '1.05rem', fontWeight: 700 }}>
                    Region Scores
                  </h4>
                  <div className="list-group">
                    {regions.map((r) => {
                      const Icon = RISK_ICONS[r.risk_level];
                      return (
                        <button
                          key={r.region}
                          className={`list-group-item list-group-item-action d-flex align-items-center justify-content-between ${selectedRegion === r.region ? 'active' : ''}`}
                          onClick={() => setSelectedRegion(selectedRegion === r.region ? null : r.region)}
                          style={{ cursor: 'pointer', borderLeft: `4px solid ${RISK_COLORS[r.risk_level]}` }}
                        >
                          <div className="d-flex align-items-center gap-2">
                            <Icon size={18} style={{ color: RISK_COLORS[r.risk_level] }} />
                            <div>
                              <div style={{ fontWeight: 600, fontSize: '0.9rem' }}>{r.region}</div>
                              <div className="text-muted small">Score: {r.score}/100</div>
                            </div>
                          </div>
                          <span
                            className="badge-custom"
                            style={{
                              background: RISK_COLORS[r.risk_level] + '20',
                              color: RISK_COLORS[r.risk_level],
                            }}
                          >
                            {r.risk_level}
                          </span>
                        </button>
                      );
                    })}
                  </div>
                </div>
              </div>

              <div className="col-lg-7">
                {selectedData.length > 0 ? (
                  <div className="card p-3 fade-in fade-in-delay-1">
                    <h4 className="card-title mb-3" style={{ fontSize: '1.05rem', fontWeight: 700 }}>
                      {selectedRegion} — Details
                    </h4>
                    <div className="table-responsive">
                      <table className="table table-sm" style={{ fontSize: '0.85rem' }}>
                        <thead>
                          <tr>
                            <th>Year</th>
                            <th>Waste (t)</th>
                            <th>Recycled (t)</th>
                            <th>Recycling %</th>
                            <th>Per Capita</th>
                            <th>Score</th>
                            <th>Risk</th>
                          </tr>
                        </thead>
                        <tbody>
                          {selectedData.map((s) => (
                            <tr key={s.id}>
                              <td><strong>{s.year}</strong></td>
                              <td>{parseFloat(s.waste_generated_tonnes).toFixed(1)}</td>
                              <td>{parseFloat(s.estimated_recycled_tonnes).toFixed(1)}</td>
                              <td>{parseFloat(s.recycling_rate_pct).toFixed(1)}%</td>
                              <td>{parseFloat(s.per_capita_waste).toFixed(3)}</td>
                              <td><strong>{s.sustainability_score}</strong></td>
                              <td>
                                <span className="badge-custom" style={{ background: RISK_COLORS[s.risk_level] + '20', color: RISK_COLORS[s.risk_level] }}>
                                  {s.risk_level}
                                </span>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                ) : (
                  <div className="card p-5 text-center fade-in">
                    <div className="empty-state">
                      <Shield className="empty-state-icon" style={{ color: '#6366f1' }} />
                      <h5 style={{ color: '#1e293b', fontWeight: 600 }}>Select a Region</h5>
                      <p className="text-muted">Click on a region to view detailed sustainability metrics and trends.</p>
                    </div>
                  </div>
                )}
              </div>
            </div>
          ) : (
            <div className="card p-5 text-center fade-in">
              <div className="empty-state">
                <Recycle className="empty-state-icon" />
                <h5 style={{ color: '#1e293b', fontWeight: 600 }}>No Scores Yet</h5>
                <p className="text-muted mb-4">Upload e-waste data and click "Recalculate Scores" to generate sustainability ratings.</p>
                <button className="btn-primary-custom d-inline-flex align-items-center gap-2" onClick={handleRecalculate} disabled={recalculating}>
                  {recalculating ? <Loader2 size={18} className="spinner" /> : <RefreshCw size={18} />}
                  Calculate Now
                </button>
              </div>
            </div>
          )}
        </>
      ) : (
        <div>
          {trendsLoading ? (
            <div className="spinner-container">
              <Loader2 className="spinner" size={40} />
              <p className="mt-3 mb-0">Loading trend analysis...</p>
            </div>
          ) : trends ? (
            <div className="row g-4">
              <div className="col-lg-4">
                <div className="card p-3 fade-in">
                  <h4 className="card-title mb-3" style={{ fontSize: '1.05rem', fontWeight: 700 }}>
                    <TrendingUp size={18} style={{ marginRight: '6px', color: '#6366f1' }} />
                    Region Trends
                  </h4>
                  <div className="list-group">
                    {Object.entries(trends).map(([regionName, t]) => (
                      <button
                        key={regionName}
                        className={`list-group-item list-group-item-action d-flex align-items-center justify-content-between ${selectedRegion === regionName ? 'active' : ''}`}
                        onClick={() => setSelectedRegion(selectedRegion === regionName ? null : regionName)}
                        style={{ cursor: 'pointer', fontSize: '0.85rem' }}
                      >
                        <div>
                          <strong>{regionName}</strong>
                          <div className="text-muted" style={{ fontSize: '0.75rem' }}>
                            Avg Score: {t.avg_score}/100
                          </div>
                        </div>
                        <span
                          className={`badge-custom ${t.trend === 'improving' ? 'badge-green' : t.trend === 'declining' ? 'badge-purple' : ''}`}
                          style={t.trend === 'stable' ? { background: '#e2e8f0', color: '#64748b' } : {}}
                        >
                          {t.trend}
                        </span>
                      </button>
                    ))}
                  </div>
                </div>
              </div>

              <div className="col-lg-8">
                {selectedRegion && trends[selectedRegion] && (
                  <div className="fade-in">
                    <div className="row g-3 mb-4">
                      <div className="col-md-4">
                        <div className="kpi-card">
                          <div className={`kpi-icon ${trends[selectedRegion].trend === 'improving' ? 'green' : trends[selectedRegion].trend === 'declining' ? 'red' : 'amber'}`}>
                            <TrendingUp size={20} />
                          </div>
                          <div>
                            <div className="kpi-value" style={{ fontSize: '1.2rem' }}>{trends[selectedRegion].score_change > 0 ? '+' : ''}{trends[selectedRegion].score_change}</div>
                            <div className="kpi-label">Score Change</div>
                          </div>
                        </div>
                      </div>
                      <div className="col-md-4">
                        <div className="kpi-card">
                          <div className="kpi-icon green"><Recycle size={20} /></div>
                          <div>
                            <div className="kpi-value" style={{ fontSize: '1.2rem' }}>{trends[selectedRegion].avg_recycling_rate}%</div>
                            <div className="kpi-label">Avg Recycling Rate</div>
                          </div>
                        </div>
                      </div>
                      <div className="col-md-4">
                        <div className="kpi-card">
                          <div className="kpi-icon purple"><Users size={20} /></div>
                          <div>
                            <div className="kpi-value" style={{ fontSize: '1.2rem' }}>{trends[selectedRegion].data_points}</div>
                            <div className="kpi-label">Data Points</div>
                          </div>
                        </div>
                      </div>
                    </div>

                    {trendChartData.length > 1 && (
                      <div className="card p-3 mb-4">
                        <h4 className="card-title mb-3" style={{ fontSize: '1.05rem', fontWeight: 700 }}>
                          Score Trend Over Time
                        </h4>
                        <Chart
                          data={trendChartData}
                          xKey="year"
                          yKeys={['score', 'recycling_rate']}
                          colors={['#6366f1', '#10b981']}
                        />
                      </div>
                    )}

                    <div className="card p-3">
                      <h4 className="card-title mb-3" style={{ fontSize: '1.05rem', fontWeight: 700 }}>
                        {selectedRegion} — Trend Details
                      </h4>
                      <div className="table-responsive">
                        <table className="table table-sm" style={{ fontSize: '0.85rem' }}>
                          <thead>
                            <tr>
                              <th>Year</th>
                              <th>Score</th>
                              <th>Recycling %</th>
                              <th>Waste (t)</th>
                              <th>Risk</th>
                            </tr>
                          </thead>
                          <tbody>
                            {trends[selectedRegion].data.map((d) => (
                              <tr key={d.year}>
                                <td><strong>{d.year}</strong></td>
                                <td>{d.score}</td>
                                <td>{d.recycling_rate}%</td>
                                <td>{d.waste_generated}</td>
                                <td>
                                  <span className="badge-custom" style={{ background: RISK_COLORS[d.risk_level] + '20', color: RISK_COLORS[d.risk_level] }}>
                                    {d.risk_level}
                                  </span>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  </div>
                )}
              </div>
            </div>
          ) : (
            <div className="card p-5 text-center fade-in">
              <div className="empty-state">
                <BarChart3 className="empty-state-icon" style={{ color: '#6366f1' }} />
                <h5 style={{ color: '#1e293b', fontWeight: 600 }}>No Trends Available</h5>
                <p className="text-muted">Recalculate scores first to generate trend analysis.</p>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default Sustainability;
