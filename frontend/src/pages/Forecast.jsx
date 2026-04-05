import React, { useState, useEffect, useMemo } from 'react';
import {
  getPredictions,
  predictEwaste,
  predictTimeseries,
  getMlHealth,
} from '../services/api';
import Chart from '../components/Chart';
import {
  TrendingUp,
  MapPin,
  Calendar,
  BarChart3,
  Users,
  Loader2,
  CheckCircle,
  AlertCircle,
  Filter,
  Activity,
} from 'lucide-react';

const DEVICE_OPTIONS = ['General', 'IT_equipment', 'Mobile', 'Large_appliances', 'Batteries'];

const emptyFeedback = () => ({ text: '', type: '' });

const parseApiError = (error) => {
  if (!error.response) {
    if (error.code === 'ECONNABORTED') return 'Request timed out. Check that the server is running.';
    if (error.code === 'ERR_NETWORK') return 'Network error. Is the API reachable?';
    return error.message || 'Request failed';
  }
  const body = error.response.data;
  const d = body?.error;
  if (d && typeof d === 'object' && d.message) return String(d.message);
  if (typeof d === 'string') return d;
  if (typeof body?.message === 'string') return body.message;
  return error.message || 'Request failed';
};

const Forecast = () => {
  const [forecasts, setForecasts] = useState([]);
  const [loading, setLoading] = useState(false);
  const [loadingPredictions, setLoadingPredictions] = useState(true);
  const [tabularFeedback, setTabularFeedback] = useState(emptyFeedback);
  const [timeseriesFeedback, setTimeseriesFeedback] = useState(emptyFeedback);
  const [predictionsLoadError, setPredictionsLoadError] = useState('');
  const [input, setInput] = useState({
    region: 'Pollachi, Tamil Nadu',
    forecast_year: 2026,
    sales_import_tonnes: 90,
    population_millions: 0.16,
    device_category: 'General',
  });
  const [tsInput, setTsInput] = useState({
    region: 'Pollachi, Tamil Nadu',
    horizon_years: 5,
    device_category: 'General',
  });
  const [errors, setErrors] = useState({});
  const [modelTab, setModelTab] = useState('tabular');
  const [filters, setFilters] = useState({
    region: '',
    forecast_year: '',
    device_category: '',
    model_type: '',
  });
  const [lastTabularMetrics, setLastTabularMetrics] = useState(null);
  const [lastTsMetrics, setLastTsMetrics] = useState(null);
  const [mlHealth, setMlHealth] = useState(null);

  const buildFilterParams = () => {
    const p = {};
    if (filters.region.trim()) p.region = filters.region.trim();
    if (filters.forecast_year) p.forecast_year = filters.forecast_year;
    if (filters.device_category) p.device_category = filters.device_category;
    if (filters.model_type) p.model_type = filters.model_type;
    return p;
  };

  const loadPredictions = async () => {
    try {
      setLoadingPredictions(true);
      setPredictionsLoadError('');
      const res = await getPredictions(buildFilterParams());
      setForecasts(Array.isArray(res.data?.data) ? res.data.data : []);
    } catch (error) {
      console.error('Error fetching', error);
      setPredictionsLoadError(parseApiError(error));
      setForecasts([]);
    } finally {
      setLoadingPredictions(false);
    }
  };

  useEffect(() => {
    loadPredictions();
    // Initial load only; filters applied via "Apply" or after new predictions
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    getMlHealth()
      .then((r) => setMlHealth(r.data))
      .catch(() => setMlHealth({ ok: false }));
  }, []);

  const validateTabular = () => {
    const newErrors = {};
    if (!input.region.trim()) newErrors.region = 'Region is required';
    if (!input.forecast_year || input.forecast_year < 2020 || input.forecast_year > 2100) {
      newErrors.forecast_year = 'Year must be between 2020 and 2100';
    }
    if (!input.sales_import_tonnes || input.sales_import_tonnes <= 0) {
      newErrors.sales_import_tonnes = 'Enter a valid value';
    }
    if (input.population_millions === null || input.population_millions < 0) {
      newErrors.population_millions = 'Enter a valid value';
    }
    setErrors(newErrors);
    return Object.keys(newErrors).length === 0;
  };

  const handlePredictTabular = async () => {
    if (!validateTabular()) return;
    setLoading(true);
    setTabularFeedback(emptyFeedback());
    try {
      const res = await predictEwaste(input);
      setLastTabularMetrics(res.data.metrics || null);
      setTabularFeedback({ text: 'Forecast generated successfully!', type: 'success' });
      loadPredictions();
    } catch (error) {
      setTabularFeedback({ text: parseApiError(error), type: 'error' });
      console.error(error);
    } finally {
      setLoading(false);
    }
  };

  const handlePredictTimeseries = async () => {
    if (!tsInput.region.trim()) {
      setTimeseriesFeedback({ text: 'Region is required for timeseries.', type: 'error' });
      return;
    }
    setLoading(true);
    setTimeseriesFeedback(emptyFeedback());
    try {
      const res = await predictTimeseries({
        region: tsInput.region.trim(),
        horizon_years: tsInput.horizon_years,
        device_category: tsInput.device_category === 'General' ? undefined : tsInput.device_category,
      });
      setLastTsMetrics(res.data.metrics || null);
      setTimeseriesFeedback({
        text: `Timeseries forecast: ${res.data.series?.length || 0} years stored.`,
        type: 'success',
      });
      loadPredictions();
    } catch (error) {
      setTimeseriesFeedback({ text: parseApiError(error), type: 'error' });
      console.error(error);
    } finally {
      setLoading(false);
    }
  };

  const updateInput = (field, value) => {
    setInput({ ...input, [field]: value });
    if (errors[field]) setErrors({ ...errors, [field]: null });
  };

  const applyFilters = () => {
    loadPredictions();
  };

  /** One point per forecast_year (latest run wins) so the x-axis does not repeat the same year. */
  const chartData = useMemo(() => {
    const byYear = new Map();
    for (const row of forecasts) {
      const y = Number(row.forecast_year);
      if (Number.isNaN(y)) continue;
      const prev = byYear.get(y);
      const rid = row.id != null ? Number(row.id) : 0;
      const prevRid = prev?.id != null ? Number(prev.id) : -1;
      if (!prev || rid >= prevRid) {
        byYear.set(y, row);
      }
    }
    return [...byYear.values()].sort((a, b) => a.forecast_year - b.forecast_year);
  }, [forecasts]);

  return (
    <div>
      <div className="page-header fade-in">
        <h2>AI Forecasting</h2>
        <p>Tabular ML (RF/XGBoost) and time-series (ARIMA / trend) with geospatial-ready metrics</p>
      </div>

      {mlHealth && (
        <div
          className="card p-3 mb-4 fade-in"
          style={{ borderLeft: mlHealth.ok !== false ? '4px solid #10b981' : '4px solid #f59e0b' }}
        >
          <div className="d-flex align-items-center gap-2 mb-1">
            <Activity size={18} />
            <strong style={{ fontSize: '0.95rem' }}>ML service</strong>
            <span className="badge-custom badge-green">
              {mlHealth.tabular_loaded ? 'tabular ready' : 'tabular offline'}
            </span>
          </div>
          {mlHealth.metrics?.tabular && (
            <p className="text-muted small mb-0">
              Training MAE: {Number(mlHealth.metrics.tabular.mae).toFixed(3)} · RMSE:{' '}
              {Number(mlHealth.metrics.tabular.rmse).toFixed(3)} ({mlHealth.metrics.tabular.model})
            </p>
          )}
          {mlHealth.ok === false && (
            <p className="text-muted small mb-0">Start ml-service on port 5001 and run train.py.</p>
          )}
        </div>
      )}

      <div className="card p-3 mb-4 fade-in">
        <h4
          className="mb-3"
          style={{ fontSize: '1rem', fontWeight: 700, display: 'flex', alignItems: 'center', gap: '0.5rem' }}
        >
          <Filter size={18} />
          Filter predictions
        </h4>
        <div className="row g-2 align-items-end">
          <div className="col-md-3">
            <label className="form-label small text-muted mb-1">Region contains</label>
            <input
              className="form-control form-control-sm"
              value={filters.region}
              onChange={(e) => setFilters({ ...filters, region: e.target.value })}
              placeholder="e.g. Pollachi"
            />
          </div>
          <div className="col-md-2">
            <label className="form-label small text-muted mb-1">Forecast year</label>
            <input
              className="form-control form-control-sm"
              type="number"
              value={filters.forecast_year}
              onChange={(e) => setFilters({ ...filters, forecast_year: e.target.value })}
              placeholder="Any"
            />
          </div>
          <div className="col-md-2">
            <label className="form-label small text-muted mb-1">Device</label>
            <select
              className="form-select form-select-sm"
              value={filters.device_category}
              onChange={(e) => setFilters({ ...filters, device_category: e.target.value })}
            >
              <option value="">Any</option>
              {DEVICE_OPTIONS.map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
          </div>
          <div className="col-md-2">
            <label className="form-label small text-muted mb-1">Model type</label>
            <select
              className="form-select form-select-sm"
              value={filters.model_type}
              onChange={(e) => setFilters({ ...filters, model_type: e.target.value })}
            >
              <option value="">Any</option>
              <option value="tabular">tabular</option>
              <option value="timeseries">timeseries</option>
            </select>
          </div>
          <div className="col-md-2">
            <button type="button" className="btn btn-sm btn-primary w-100" onClick={applyFilters}>
              Apply
            </button>
          </div>
        </div>
      </div>

      <div className="row g-4">
        <div className="col-lg-4">
          <div className="card p-4 fade-in">
            <div className="btn-group w-100 mb-3" role="group">
              <button
                type="button"
                className={`btn btn-sm ${modelTab === 'tabular' ? 'btn-primary' : 'btn-outline-primary'}`}
                onClick={() => setModelTab('tabular')}
              >
                Tabular
              </button>
              <button
                type="button"
                className={`btn btn-sm ${modelTab === 'timeseries' ? 'btn-primary' : 'btn-outline-primary'}`}
                onClick={() => setModelTab('timeseries')}
              >
                Time series
              </button>
            </div>

            {modelTab === 'tabular' && (
              <>
                <h4
                  className="mb-3"
                  style={{
                    fontSize: '1.1rem',
                    fontWeight: 700,
                    display: 'flex',
                    alignItems: 'center',
                    gap: '0.5rem',
                  }}
                >
                  <TrendingUp size={20} style={{ color: '#10b981' }} />
                  New forecast
                </h4>

                <div className="forecast-input-group">
                  <label className="form-label">
                    <MapPin size={14} style={{ marginRight: '4px' }} />
                    Region
                  </label>
                  <input
                    className={`form-control ${errors.region ? 'is-invalid' : ''}`}
                    value={input.region}
                    onChange={(e) => updateInput('region', e.target.value)}
                    placeholder="e.g., Pollachi, Tamil Nadu"
                  />
                  {errors.region && <small className="text-danger">{errors.region}</small>}
                </div>

                <div className="forecast-input-group">
                  <label className="form-label">
                    <Calendar size={14} style={{ marginRight: '4px' }} />
                    Forecast year
                  </label>
                  <input
                    className={`form-control ${errors.forecast_year ? 'is-invalid' : ''}`}
                    type="number"
                    value={input.forecast_year}
                    onChange={(e) => updateInput('forecast_year', parseInt(e.target.value, 10) || '')}
                    placeholder="e.g., 2026"
                    min="2020"
                    max="2100"
                  />
                  {errors.forecast_year && <small className="text-danger">{errors.forecast_year}</small>}
                </div>

                <div className="forecast-input-group">
                  <label className="form-label">Device category</label>
                  <select
                    className="form-select"
                    value={input.device_category}
                    onChange={(e) => updateInput('device_category', e.target.value)}
                  >
                    {DEVICE_OPTIONS.map((d) => (
                      <option key={d} value={d}>
                        {d}
                      </option>
                    ))}
                  </select>
                </div>

                <div className="forecast-input-group">
                  <label className="form-label">
                    <BarChart3 size={14} style={{ marginRight: '4px' }} />
                    Sales/Imports (tonnes)
                  </label>
                  <input
                    className={`form-control ${errors.sales_import_tonnes ? 'is-invalid' : ''}`}
                    type="number"
                    value={input.sales_import_tonnes}
                    onChange={(e) => updateInput('sales_import_tonnes', parseFloat(e.target.value) || '')}
                    placeholder="e.g., 90"
                    min="0"
                    step="0.1"
                  />
                  {errors.sales_import_tonnes && (
                    <small className="text-danger">{errors.sales_import_tonnes}</small>
                  )}
                </div>

                <div className="forecast-input-group">
                  <label className="form-label">
                    <Users size={14} style={{ marginRight: '4px' }} />
                    Population (millions)
                  </label>
                  <input
                    className={`form-control ${errors.population_millions ? 'is-invalid' : ''}`}
                    type="number"
                    value={input.population_millions}
                    onChange={(e) => updateInput('population_millions', parseFloat(e.target.value) || '')}
                    placeholder="e.g., 0.16"
                    min="0"
                    step="0.01"
                  />
                  {errors.population_millions && (
                    <small className="text-danger">{errors.population_millions}</small>
                  )}
                </div>

                <button
                  className="btn-accent w-100 d-flex align-items-center justify-content-center gap-2"
                  onClick={handlePredictTabular}
                  disabled={loading}
                >
                  {loading ? (
                    <>
                      <Loader2 size={18} className="spinner" style={{ width: '18px', height: '18px' }} />
                      Generating...
                    </>
                  ) : (
                    <>
                      <TrendingUp size={18} />
                      Generate (tabular)
                    </>
                  )}
                </button>

                {lastTabularMetrics && (
                  <div className="mt-3 p-2 rounded small" style={{ background: '#f1f5f9' }}>
                    <strong>Last run metrics</strong>
                    <div className="text-muted">
                      MAE: {lastTabularMetrics.mae != null ? lastTabularMetrics.mae.toFixed(3) : '—'} · RMSE:{' '}
                      {lastTabularMetrics.rmse != null ? lastTabularMetrics.rmse.toFixed(3) : '—'}
                    </div>
                  </div>
                )}

                {tabularFeedback.text && (
                  <div
                    className={`mt-3 p-3 d-flex align-items-start gap-2 ${
                      tabularFeedback.type === 'success' ? 'alert-success-custom' : 'alert-danger-custom'
                    }`}
                    role="alert"
                  >
                    {tabularFeedback.type === 'success' ? (
                      <CheckCircle size={20} className="mt-1" style={{ flexShrink: 0 }} />
                    ) : (
                      <AlertCircle size={20} className="mt-1" style={{ flexShrink: 0 }} />
                    )}
                    <span style={{ fontSize: '0.9rem' }}>{tabularFeedback.text}</span>
                  </div>
                )}
              </>
            )}

            {modelTab === 'timeseries' && (
              <>
                <h4 className="mb-3" style={{ fontSize: '1.1rem', fontWeight: 700 }}>
                  Region history → future
                </h4>
                <p className="text-muted small">
                  Uses uploaded disposal totals by year for the region (optional device filter).
                </p>
                <div className="forecast-input-group">
                  <label className="form-label">Region</label>
                  <input
                    className="form-control"
                    value={tsInput.region}
                    onChange={(e) => setTsInput({ ...tsInput, region: e.target.value })}
                  />
                </div>
                <div className="forecast-input-group">
                  <label className="form-label">Horizon (years)</label>
                  <input
                    className="form-control"
                    type="number"
                    min={1}
                    max={30}
                    value={tsInput.horizon_years}
                    onChange={(e) =>
                      setTsInput({ ...tsInput, horizon_years: parseInt(e.target.value, 10) || 5 })
                    }
                  />
                </div>
                <div className="forecast-input-group">
                  <label className="form-label">Device category</label>
                  <select
                    className="form-select"
                    value={tsInput.device_category}
                    onChange={(e) => setTsInput({ ...tsInput, device_category: e.target.value })}
                  >
                    {DEVICE_OPTIONS.map((d) => (
                      <option key={d} value={d}>
                        {d}
                      </option>
                    ))}
                  </select>
                </div>
                <button
                  className="btn-accent w-100 d-flex align-items-center justify-content-center gap-2"
                  onClick={handlePredictTimeseries}
                  disabled={loading}
                >
                  {loading ? (
                    <Loader2 size={18} className="spinner" />
                  ) : (
                    <TrendingUp size={18} />
                  )}
                  Run timeseries
                </button>
                {lastTsMetrics && (
                  <div className="mt-3 p-2 rounded small" style={{ background: '#f1f5f9' }}>
                    <strong>In-sample metrics</strong>
                    <div className="text-muted">
                      MAE: {lastTsMetrics.mae != null ? lastTsMetrics.mae.toFixed(3) : '—'} · RMSE:{' '}
                      {lastTsMetrics.rmse != null ? lastTsMetrics.rmse.toFixed(3) : '—'}
                    </div>
                  </div>
                )}

                {timeseriesFeedback.text && (
                  <div
                    className={`mt-3 p-3 d-flex align-items-start gap-2 ${
                      timeseriesFeedback.type === 'success' ? 'alert-success-custom' : 'alert-danger-custom'
                    }`}
                    role="alert"
                  >
                    {timeseriesFeedback.type === 'success' ? (
                      <CheckCircle size={20} className="mt-1" style={{ flexShrink: 0 }} />
                    ) : (
                      <AlertCircle size={20} className="mt-1" style={{ flexShrink: 0 }} />
                    )}
                    <span style={{ fontSize: '0.9rem' }}>{timeseriesFeedback.text}</span>
                  </div>
                )}
              </>
            )}
          </div>
        </div>

        <div className="col-lg-8">
          <div className="card p-3 fade-in fade-in-delay-1">
            <div className="d-flex align-items-center justify-content-between mb-3 flex-wrap gap-2">
              <h4 className="card-title mb-0" style={{ fontSize: '1.1rem', fontWeight: 700 }}>
                Forecast timeline
              </h4>
              {forecasts.length > 0 && (
                <span className="badge-custom badge-purple">
                  {forecasts.length} saved
                  {chartData.length !== forecasts.length
                    ? ` · ${chartData.length} yr on chart`
                    : ''}
                </span>
              )}
            </div>

            {predictionsLoadError && (
              <div
                className="mb-3 p-3 d-flex align-items-start gap-2 alert-danger-custom"
                role="alert"
              >
                <AlertCircle size={20} className="mt-1" style={{ flexShrink: 0 }} />
                <span style={{ fontSize: '0.9rem' }}>{predictionsLoadError}</span>
              </div>
            )}

            {loadingPredictions ? (
              <div className="spinner-container">
                <Loader2 className="spinner" size={40} />
                <p className="mt-3 mb-0">Loading forecasts...</p>
              </div>
            ) : chartData.length > 0 ? (
              <Chart data={chartData} xKey="forecast_year" yKeys={['predicted_tonnes']} />
            ) : (
              <div className="empty-state">
                <TrendingUp className="empty-state-icon" />
                <h5 style={{ color: '#1e293b', fontWeight: 600, marginBottom: '0.5rem' }}>No forecasts yet</h5>
                <p className="text-muted">Generate a tabular or timeseries forecast to populate the chart.</p>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default Forecast;
