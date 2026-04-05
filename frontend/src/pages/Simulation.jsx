import React, { useState, useEffect } from 'react';
import {
  Sliders,
  TrendingUp,
  TrendingDown,
  Activity,
  Loader2,
  CheckCircle,
  AlertCircle,
  Zap,
  Recycle,
  FileText,
  Trash2,
  BarChart3,
} from 'lucide-react';
import Chart from '../components/Chart';

const parseApiError = (error) => {
  if (!error.response) return error.message || 'Network error';
  const d = error.response.data?.error;
  if (d?.message) return String(d.message);
  if (typeof d === 'string') return d;
  return error.message || 'Request failed';
};

const API_BASE = import.meta.env.VITE_API_BASE_URL || 'http://localhost:5000/api';

const Simulation = () => {
  const [params, setParams] = useState({
    name: '',
    description: '',
    sales_change_pct: 10,
    recycling_rate_change: 0,
    policy_factor: 1.0,
    forecast_horizon_years: 5,
    region: '',
  });
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState(null);
  const [scenarios, setScenarios] = useState([]);
  const [feedback, setFeedback] = useState({ text: '', type: '' });
  const [errors, setErrors] = useState({});
  const [compareMode, setCompareMode] = useState(false);
  const [selectedForCompare, setSelectedForCompare] = useState([]);
  const [compareResult, setCompareResult] = useState(null);
  const [compareLoading, setCompareLoading] = useState(false);

  useEffect(() => {
    loadScenarios();
  }, []);

  const loadScenarios = async () => {
    try {
      const res = await fetch(`${API_BASE}/scenarios`);
      const data = await res.json();
      if (Array.isArray(data)) setScenarios(data);
    } catch (e) {
      console.error('Failed to load scenarios', e);
    }
  };

  const validate = () => {
    const e = {};
    if (!params.name.trim()) e.name = 'Scenario name is required';
    if (params.forecast_horizon_years < 1 || params.forecast_horizon_years > 50) e.horizon = 'Must be 1-50';
    setErrors(e);
    return Object.keys(e).length === 0;
  };

  const handleRun = async () => {
    if (!validate()) return;
    setLoading(true);
    setFeedback({ text: '', type: '' });
    setResult(null);
    try {
      const res = await fetch(`${API_BASE}/simulate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(params),
      });
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.error?.message || 'Simulation failed');
      }
      const data = await res.json();
      setResult(data);
      setFeedback({ text: `Scenario "${data.name}" created! Impact: ${data.impact_pct > 0 ? '+' : ''}${data.impact_pct.toFixed(1)}%`, type: 'success' });
      loadScenarios();
    } catch (e) {
      setFeedback({ text: parseApiError(e), type: 'error' });
    } finally {
      setLoading(false);
    }
  };

  const presets = [
    { label: 'Sales Boom (+30%)', sales_change_pct: 30, recycling_rate_change: 0, policy_factor: 1.0 },
    { label: 'Aggressive Recycling', sales_change_pct: 0, recycling_rate_change: 40, policy_factor: 1.0 },
    { label: 'Strict Policy', sales_change_pct: -10, recycling_rate_change: 20, policy_factor: 0.8 },
    { label: 'Business as Usual', sales_change_pct: 5, recycling_rate_change: 0, policy_factor: 1.0 },
  ];

  const applyPreset = (preset) => {
    setParams({ ...params, ...preset, name: preset.label });
  };

  const toggleCompareSelection = (id) => {
    setSelectedForCompare((prev) =>
      prev.includes(id) ? prev.filter((x) => x !== id) : prev.length < 5 ? [...prev, id] : prev
    );
  };

  const handleCompare = async () => {
    if (selectedForCompare.length < 2) return;
    setCompareLoading(true);
    try {
      const res = await fetch(`${API_BASE}/scenarios/compare`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ ids: selectedForCompare }),
      });
      if (!res.ok) throw new Error('Comparison failed');
      const data = await res.json();
      setCompareResult(data);
      setCompareMode(true);
    } catch (e) {
      setFeedback({ text: parseApiError(e), type: 'error' });
    } finally {
      setCompareLoading(false);
    }
  };

  const chartData = result?.series?.map((s) => ({
    year: s.year,
    baseline: s.baseline_tonnes,
    projected: s.projected_disposal,
  })) || [];

  const compareChartData = compareResult?.year_by_year?.map((entry) => {
    const d = { year: entry.year };
    compareResult.scenarios.forEach((s) => {
      d[s.name] = entry[s.name];
    });
    return d;
  }) || [];

  return (
    <div>
      <div className="page-header fade-in">
        <h2>What-If Simulation</h2>
        <p>Simulate policy changes, sales shifts, and recycling improvements to see future impact</p>
      </div>

      <div className="card p-3 mb-4 fade-in">
        <div className="btn-group w-100" role="group">
          <button
            className={`btn btn-sm ${!compareMode ? 'btn-primary' : 'btn-outline-primary'}`}
            onClick={() => { setCompareMode(false); setCompareResult(null); }}
          >
            <Zap size={14} style={{ marginRight: '4px' }} />
            Run Simulation
          </button>
          <button
            className={`btn btn-sm ${compareMode ? 'btn-primary' : 'btn-outline-primary'}`}
            onClick={() => setCompareMode(true)}
          >
            <BarChart3 size={14} style={{ marginRight: '4px' }} />
            Compare Scenarios
          </button>
        </div>
      </div>

      {compareMode ? (
        <div>
          <div className="row g-4">
            <div className="col-lg-4">
              <div className="card p-4 fade-in">
                <h4 className="mb-3" style={{ fontSize: '1.05rem', fontWeight: 700 }}>
                  <BarChart3 size={20} style={{ color: '#6366f1', marginRight: '6px' }} />
                  Select Scenarios to Compare
                </h4>
                <p className="text-muted small mb-3">Choose 2-5 scenarios to compare side-by-side</p>
                {scenarios.length === 0 ? (
                  <div className="text-center text-muted py-4">
                    <p>No scenarios yet. Run some simulations first.</p>
                  </div>
                ) : (
                  <div className="list-group mb-3">
                    {scenarios.map((s) => (
                      <label
                        key={s.id}
                        className="list-group-item list-group-item-action d-flex align-items-center gap-2"
                        style={{ cursor: 'pointer', fontSize: '0.85rem' }}
                      >
                        <input
                          type="checkbox"
                          checked={selectedForCompare.includes(s.id)}
                          onChange={() => toggleCompareSelection(s.id)}
                          disabled={!selectedForCompare.includes(s.id) && selectedForCompare.length >= 5}
                        />
                        <div className="flex-grow-1">
                          <strong>{s.name}</strong>
                          <div className="text-muted" style={{ fontSize: '0.75rem' }}>
                            Impact: {parseFloat(s.impact_pct) > 0 ? '+' : ''}{parseFloat(s.impact_pct).toFixed(1)}%
                          </div>
                        </div>
                      </label>
                    ))}
                  </div>
                )}
                <button
                  className="btn-accent w-100 d-flex align-items-center justify-content-center gap-2"
                  onClick={handleCompare}
                  disabled={selectedForCompare.length < 2 || compareLoading}
                >
                  {compareLoading ? <Loader2 size={18} className="spinner" /> : <BarChart3 size={18} />}
                  Compare ({selectedForCompare.length} selected)
                </button>
              </div>
            </div>

            <div className="col-lg-8">
              {compareResult && (
                <div className="fade-in">
                  <div className="row g-3 mb-4">
                    <div className="col-md-6">
                      <div className="kpi-card">
                        <div className="kpi-icon green"><TrendingDown size={20} /></div>
                        <div>
                          <div className="kpi-value" style={{ fontSize: '1rem', color: '#10b981' }}>{compareResult.summary.best_scenario}</div>
                          <div className="kpi-label">Best Impact: {compareResult.summary.best_impact_pct}%</div>
                        </div>
                      </div>
                    </div>
                    <div className="col-md-6">
                      <div className="kpi-card">
                        <div className="kpi-icon red"><TrendingUp size={20} /></div>
                        <div>
                          <div className="kpi-value" style={{ fontSize: '1rem', color: '#dc2626' }}>{compareResult.summary.worst_scenario}</div>
                          <div className="kpi-label">Worst Impact: {compareResult.summary.worst_impact_pct}%</div>
                        </div>
                      </div>
                    </div>
                  </div>

                  <div className="card p-3 mb-4">
                    <h4 className="card-title mb-3" style={{ fontSize: '1.05rem', fontWeight: 700 }}>
                      Scenario Comparison Chart
                    </h4>
                    <Chart
                      data={compareChartData}
                      xKey="year"
                      yKeys={compareResult.scenarios.map((s) => s.name)}
                      colors={['#6366f1', '#10b981', '#f59e0b', '#ec4899', '#06b6d4']}
                    />
                  </div>

                  <div className="card p-3">
                    <h4 className="card-title mb-3" style={{ fontSize: '1.05rem', fontWeight: 700 }}>
                      Comparison Summary
                    </h4>
                    <div className="table-responsive">
                      <table className="table table-sm" style={{ fontSize: '0.85rem' }}>
                        <thead>
                          <tr>
                            <th>Scenario</th>
                            <th>Sales Δ</th>
                            <th>Recycling Δ</th>
                            <th>Policy</th>
                            <th>Baseline (t)</th>
                            <th>Projected (t)</th>
                            <th>Impact</th>
                          </tr>
                        </thead>
                        <tbody>
                          {compareResult.scenarios.map((s) => (
                            <tr key={s.id}>
                              <td><strong>{s.name}</strong></td>
                              <td>{s.sales_change_pct > 0 ? '+' : ''}{s.sales_change_pct}%</td>
                              <td>+{s.recycling_rate_change}%</td>
                              <td>{s.policy_factor}x</td>
                              <td>{s.baseline_tonnes.toLocaleString()}</td>
                              <td>{s.projected_tonnes.toLocaleString()}</td>
                              <td>
                                <span className={`badge-custom ${parseFloat(s.impact_pct) > 0 ? 'badge-purple' : 'badge-green'}`}>
                                  {parseFloat(s.impact_pct) > 0 ? '+' : ''}{parseFloat(s.impact_pct).toFixed(1)}%
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
        </div>
      ) : (
        <div className="row g-4">
          <div className="col-lg-4">
            <div className="card p-4 fade-in">
              <h4 className="mb-3" style={{ fontSize: '1.05rem', fontWeight: 700, display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                <Sliders size={20} style={{ color: '#6366f1' }} />
                Simulation Parameters
              </h4>

              <div className="mb-3">
                <label className="form-label">Scenario Name</label>
                <input
                  className={`form-control form-control-sm ${errors.name ? 'is-invalid' : ''}`}
                  value={params.name}
                  onChange={(e) => setParams({ ...params, name: e.target.value })}
                  placeholder="e.g., Sales Boom 2026"
                />
                {errors.name && <small className="text-danger">{errors.name}</small>}
              </div>

              <div className="mb-3">
                <label className="form-label">Region (optional)</label>
                <input
                  className="form-control form-control-sm"
                  value={params.region}
                  onChange={(e) => setParams({ ...params, region: e.target.value })}
                  placeholder="All regions if empty"
                />
              </div>

              <div className="mb-3">
                <label className="form-label">
                  <TrendingUp size={14} style={{ marginRight: '4px' }} />
                  Sales Change: {params.sales_change_pct > 0 ? '+' : ''}{params.sales_change_pct}%
                </label>
                <input
                  type="range"
                  className="form-range"
                  min={-50}
                  max={100}
                  value={params.sales_change_pct}
                  onChange={(e) => setParams({ ...params, sales_change_pct: parseInt(e.target.value) })}
                />
                <div className="d-flex justify-content-between small text-muted">
                  <span>-50%</span><span>0%</span><span>+100%</span>
                </div>
              </div>

              <div className="mb-3">
                <label className="form-label">
                  <Recycle size={14} style={{ marginRight: '4px' }} />
                  Recycling Rate Improvement: {params.recycling_rate_change}%
                </label>
                <input
                  type="range"
                  className="form-range"
                  min={0}
                  max={80}
                  value={params.recycling_rate_change}
                  onChange={(e) => setParams({ ...params, recycling_rate_change: parseInt(e.target.value) })}
                />
                <div className="d-flex justify-content-between small text-muted">
                  <span>0%</span><span>40%</span><span>80%</span>
                </div>
              </div>

              <div className="mb-3">
                <label className="form-label">
                  <FileText size={14} style={{ marginRight: '4px' }} />
                  Policy Factor: {params.policy_factor.toFixed(2)}x
                </label>
                <input
                  type="range"
                  className="form-range"
                  min={0.5}
                  max={1.5}
                  step={0.05}
                  value={params.policy_factor}
                  onChange={(e) => setParams({ ...params, policy_factor: parseFloat(e.target.value) })}
                />
                <div className="d-flex justify-content-between small text-muted">
                  <span>0.5x (strict)</span><span>1.0x</span><span>1.5x (lax)</span>
                </div>
              </div>

              <div className="mb-3">
                <label className="form-label">Forecast Horizon (years)</label>
                <input
                  className={`form-control form-control-sm ${errors.horizon ? 'is-invalid' : ''}`}
                  type="number"
                  min={1}
                  max={50}
                  value={params.forecast_horizon_years}
                  onChange={(e) => setParams({ ...params, forecast_horizon_years: parseInt(e.target.value) || 5 })}
                />
              </div>

              <div className="mb-3">
                <label className="form-label small text-muted mb-2">Quick Presets</label>
                <div className="d-flex flex-wrap gap-2">
                  {presets.map((p) => (
                    <button
                      key={p.label}
                      className="btn btn-sm btn-outline-secondary d-flex align-items-center gap-1"
                      onClick={() => applyPreset(p)}
                      style={{ fontSize: '0.75rem' }}
                    >
                      {p.label}
                    </button>
                  ))}
                </div>
              </div>

              <button
                className="btn-accent w-100 d-flex align-items-center justify-content-center gap-2"
                onClick={handleRun}
                disabled={loading}
              >
                {loading ? <Loader2 size={18} className="spinner" /> : <Zap size={18} />}
                {loading ? 'Running...' : 'Run Simulation'}
              </button>

              {feedback.text && (
                <div className={`mt-3 p-3 d-flex align-items-start gap-2 ${feedback.type === 'success' ? 'alert-success-custom' : 'alert-danger-custom'}`}>
                  {feedback.type === 'success' ? <CheckCircle size={18} /> : <AlertCircle size={18} />}
                  <span style={{ fontSize: '0.85rem' }}>{feedback.text}</span>
                </div>
              )}
            </div>
          </div>

          <div className="col-lg-8">
            {result ? (
              <div className="fade-in">
                <div className="row g-3 mb-4">
                  <div className="col-md-4">
                    <div className="kpi-card">
                      <div className="kpi-icon green"><TrendingUp size={20} /></div>
                      <div>
                        <div className="kpi-value" style={{ fontSize: '1.2rem' }}>{result.baseline_tonnes.toLocaleString()}</div>
                        <div className="kpi-label">Baseline (t)</div>
                      </div>
                    </div>
                  </div>
                  <div className="col-md-4">
                    <div className="kpi-card">
                      <div className="kpi-icon purple"><Activity size={20} /></div>
                      <div>
                        <div className="kpi-value" style={{ fontSize: '1.2rem' }}>{result.projected_tonnes.toLocaleString()}</div>
                        <div className="kpi-label">Projected (t)</div>
                      </div>
                    </div>
                  </div>
                  <div className="col-md-4">
                    <div className="kpi-card">
                      <div className={`kpi-icon ${result.impact_pct > 0 ? 'red' : 'green'}`}>
                        {result.impact_pct > 0 ? <TrendingUp size={20} /> : <TrendingDown size={20} />}
                      </div>
                      <div>
                        <div className="kpi-value" style={{ fontSize: '1.2rem', color: result.impact_pct > 0 ? '#dc2626' : '#10b981' }}>
                          {result.impact_pct > 0 ? '+' : ''}{result.impact_pct.toFixed(1)}%
                        </div>
                        <div className="kpi-label">Impact</div>
                      </div>
                    </div>
                  </div>
                </div>

                <div className="card p-3 mb-4">
                  <h4 className="card-title mb-3" style={{ fontSize: '1.05rem', fontWeight: 700 }}>
                    Baseline vs Projected
                  </h4>
                  <Chart
                    data={chartData}
                    xKey="year"
                    yKeys={['baseline', 'projected']}
                    colors={['#94a3b8', '#6366f1']}
                  />
                </div>
              </div>
            ) : (
              <div className="card p-5 text-center fade-in">
                <div className="empty-state">
                  <Zap className="empty-state-icon" style={{ color: '#6366f1' }} />
                  <h5 style={{ color: '#1e293b', fontWeight: 600 }}>Configure & Run</h5>
                  <p className="text-muted">Adjust parameters and click "Run Simulation" to see the projected impact on e-waste generation.</p>
                </div>
              </div>
            )}

            {scenarios.length > 0 && (
              <div className="card p-3 fade-in fade-in-delay-1">
                <h4 className="card-title mb-3" style={{ fontSize: '1.05rem', fontWeight: 700 }}>
                  <Trash2 size={16} style={{ marginRight: '6px' }} />
                  Past Scenarios ({scenarios.length})
                </h4>
                <div className="table-responsive">
                  <table className="table table-sm table-hover" style={{ fontSize: '0.85rem' }}>
                    <thead>
                      <tr>
                        <th>Name</th>
                        <th>Sales Δ</th>
                        <th>Recycling Δ</th>
                        <th>Policy</th>
                        <th>Impact</th>
                        <th>Created</th>
                      </tr>
                    </thead>
                    <tbody>
                      {scenarios.map((s) => (
                        <tr key={s.id}>
                          <td><strong>{s.name}</strong></td>
                          <td>{s.sales_change_pct > 0 ? '+' : ''}{s.sales_change_pct}%</td>
                          <td>+{s.recycling_rate_change}%</td>
                          <td>{s.policy_factor}x</td>
                          <td>
                            <span className={`badge-custom ${parseFloat(s.impact_pct) > 0 ? 'badge-purple' : 'badge-green'}`}>
                              {parseFloat(s.impact_pct) > 0 ? '+' : ''}{parseFloat(s.impact_pct).toFixed(1)}%
                            </span>
                          </td>
                          <td className="text-muted">{new Date(s.created_at).toLocaleDateString()}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default Simulation;
