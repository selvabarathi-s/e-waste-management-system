import React, { useState, useEffect } from 'react';
import {
  Camera,
  Upload,
  Image as ImageIcon,
  Loader2,
  CheckCircle,
  AlertCircle,
  Zap,
  Smartphone,
  Laptop,
  Cpu,
  Battery,
  Monitor,
  Plug,
  WashingMachine,
  X,
  Trash2,
  History,
  BarChart3,
} from 'lucide-react';

const API_BASE = import.meta.env.VITE_API_BASE_URL || 'http://localhost:5000/api';

const CLASS_ICONS = {
  mobile_phone: Smartphone,
  laptop: Laptop,
  circuit_board: Cpu,
  battery: Battery,
  cable_wire: Plug,
  monitor_screen: Monitor,
  appliance: WashingMachine,
  non_ewaste: Trash2,
};

const CLASS_COLORS = {
  mobile_phone: '#6366f1',
  laptop: '#8b5cf6',
  circuit_board: '#10b981',
  battery: '#f59e0b',
  cable_wire: '#06b6d4',
  monitor_screen: '#ec4899',
  appliance: '#f97316',
  non_ewaste: '#94a3b8',
};

const CV = () => {
  const [mode, setMode] = useState('single');
  const [singleResult, setSingleResult] = useState(null);
  const [batchResults, setBatchResults] = useState(null);
  const [loading, setLoading] = useState(false);
  const [preview, setPreview] = useState(null);
  const [batchFiles, setBatchFiles] = useState([]);
  const [cvHealth, setCvHealth] = useState(null);
  const [feedback, setFeedback] = useState({ text: '', type: '' });
  const [history, setHistory] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [historyPage, setHistoryPage] = useState(1);
  const [historyTotal, setHistoryTotal] = useState(0);
  const [historyFilter, setHistoryFilter] = useState({ class: '', is_ewaste: '' });
  const [cvStats, setCvStats] = useState(null);

  useEffect(() => {
    checkHealth();
    loadHistory();
    loadStats();
  }, []);

  useEffect(() => {
    loadHistory();
  }, [historyPage, historyFilter]);

  const checkHealth = async () => {
    try {
      const res = await fetch(`${API_BASE}/cv/health`);
      const data = await res.json();
      setCvHealth(data);
    } catch (e) {
      setCvHealth({ ok: false });
    }
  };

  const loadHistory = async () => {
    try {
      setHistoryLoading(true);
      const params = new URLSearchParams({ limit: 20, page: historyPage, ...historyFilter });
      const res = await fetch(`${API_BASE}/cv/history?${params}`);
      const data = await res.json();
      if (data.classifications) setHistory(data.classifications);
      if (data.pagination) setHistoryTotal(data.pagination.total);
    } catch (e) {
      console.error('Failed to load history', e);
    } finally {
      setHistoryLoading(false);
    }
  };

  const loadStats = async () => {
    try {
      const res = await fetch(`${API_BASE}/cv/stats`);
      const data = await res.json();
      setCvStats(data);
    } catch (e) {
      console.error('Failed to load stats', e);
    }
  };

  const handleSingleFile = async (e) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setPreview(URL.createObjectURL(file));
    setLoading(true);
    setFeedback({ text: '', type: '' });
    setSingleResult(null);

    try {
      const formData = new FormData();
      formData.append('image', file);

      const res = await fetch(`${API_BASE}/classify`, { method: 'POST', body: formData });
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.error?.message || 'Classification failed');
      }
      const data = await res.json();
      setSingleResult(data);
      setFeedback({ text: `Detected: ${data.predicted_class.replace('_', ' ')} (${(data.confidence * 100).toFixed(1)}%)`, type: 'success' });
      loadHistory();
      loadStats();
    } catch (e) {
      setFeedback({ text: e.message, type: 'error' });
    } finally {
      setLoading(false);
    }
  };

  const handleBatchFiles = (e) => {
    const files = Array.from(e.target.files || []);
    setBatchFiles(files);
    setBatchResults(null);
  };

  const handleBatchClassify = async () => {
    if (batchFiles.length === 0) return;
    setLoading(true);
    setFeedback({ text: '', type: '' });

    try {
      const formData = new FormData();
      batchFiles.forEach((f) => formData.append('images', f));

      const res = await fetch(`${API_BASE}/classify/batch`, { method: 'POST', body: formData });
      if (!res.ok) throw new Error('Batch classification failed');
      const data = await res.json();
      setBatchResults(data);
      setFeedback({ text: `Classified ${data.total} images: ${data.ewaste_detected} e-waste, ${data.non_ewaste} non-e-waste`, type: 'success' });
      loadHistory();
      loadStats();
    } catch (e) {
      setFeedback({ text: e.message, type: 'error' });
    } finally {
      setLoading(false);
    }
  };

  const clearPreview = () => {
    setPreview(null);
    setSingleResult(null);
  };

  const totalPages = Math.ceil(historyTotal / 20);

  return (
    <div>
      <div className="page-header fade-in">
        <h2>Computer Vision Detection</h2>
        <p>Upload images of e-waste bins or dump yards to automatically classify electronic waste types</p>
      </div>

      <div className="card p-3 mb-4 fade-in">
        <div className="btn-group w-100" role="group">
          <button className={`btn btn-sm ${mode === 'single' ? 'btn-primary' : 'btn-outline-primary'}`} onClick={() => setMode('single')}>
            <ImageIcon size={14} style={{ marginRight: '4px' }} />
            Single Image
          </button>
          <button className={`btn btn-sm ${mode === 'batch' ? 'btn-primary' : 'btn-outline-primary'}`} onClick={() => setMode('batch')}>
            <Upload size={14} style={{ marginRight: '4px' }} />
            Batch Upload
          </button>
          <button className={`btn btn-sm ${mode === 'history' ? 'btn-primary' : 'btn-outline-primary'}`} onClick={() => setMode('history')}>
            <History size={14} style={{ marginRight: '4px' }} />
            Classification History
          </button>
        </div>
      </div>

      {cvHealth && mode !== 'history' && (
        <div className={`card p-3 mb-4 fade-in ${cvHealth.ok !== false ? 'border-start border-success border-4' : 'border-start border-warning border-4'}`}>
          <div className="d-flex align-items-center gap-2">
            <Camera size={18} />
            <strong style={{ fontSize: '0.9rem' }}>CV Service</strong>
            <span className={`badge-custom ${cvHealth.ok !== false ? 'badge-green' : 'badge-purple'}`}>
              {cvHealth.ok !== false ? `${cvHealth.classes?.length || 0} classes` : 'offline'}
            </span>
          </div>
        </div>
      )}

      {cvStats && mode === 'history' && (
        <div className="row g-3 mb-4 fade-in">
          <div className="col-md-4">
            <div className="kpi-card">
              <div className="kpi-icon purple"><ImageIcon size={20} /></div>
              <div>
                <div className="kpi-value" style={{ fontSize: '1.2rem' }}>{cvStats.total}</div>
                <div className="kpi-label">Total Classifications</div>
              </div>
            </div>
          </div>
          <div className="col-md-4">
            <div className="kpi-card">
              <div className="kpi-icon amber"><Zap size={20} /></div>
              <div>
                <div className="kpi-value" style={{ fontSize: '1.2rem' }}>{cvStats.ewaste_detected}</div>
                <div className="kpi-label">E-Waste Detected</div>
              </div>
            </div>
          </div>
          <div className="col-md-4">
            <div className="kpi-card">
              <div className="kpi-icon green"><CheckCircle size={20} /></div>
              <div>
                <div className="kpi-value" style={{ fontSize: '1.2rem' }}>{cvStats.non_ewaste}</div>
                <div className="kpi-label">Non-E-Waste</div>
              </div>
            </div>
          </div>
        </div>
      )}

      {mode === 'single' && (
        <div className="row g-4">
          <div className="col-lg-5">
            <div className="card p-4 fade-in">
              <h4 className="mb-3" style={{ fontSize: '1.05rem', fontWeight: 700 }}>
                <Camera size={20} style={{ marginRight: '6px', color: '#6366f1' }} />
                Upload Image
              </h4>

              <div className="upload-zone" style={{ padding: '2rem' }}>
                <label style={{ cursor: 'pointer' }}>
                  <input type="file" accept="image/*" onChange={handleSingleFile} hidden />
                  <Upload size={40} style={{ color: '#10b981', marginBottom: '1rem' }} />
                  <p className="mb-1" style={{ fontWeight: 600 }}>Click to upload</p>
                  <p className="text-muted small mb-0">PNG, JPG up to 10MB</p>
                </label>
              </div>

              {preview && (
                <div className="mt-3 position-relative">
                  <img src={preview} alt="Preview" className="img-fluid rounded" style={{ maxHeight: '250px', width: '100%', objectFit: 'cover' }} />
                  <button className="btn btn-sm btn-danger position-absolute top-0 end-0 m-2" onClick={clearPreview} style={{ borderRadius: '50%', width: '28px', height: '28px', padding: 0 }}>
                    <X size={14} />
                  </button>
                </div>
              )}

              {feedback.text && (
                <div className={`mt-3 p-3 d-flex align-items-start gap-2 ${feedback.type === 'success' ? 'alert-success-custom' : 'alert-danger-custom'}`}>
                  {feedback.type === 'success' ? <CheckCircle size={18} /> : <AlertCircle size={18} />}
                  <span style={{ fontSize: '0.85rem' }}>{feedback.text}</span>
                </div>
              )}
            </div>
          </div>

          <div className="col-lg-7">
            {singleResult ? (
              <div className="fade-in">
                <div className="card p-4 mb-3">
                  <div className="d-flex align-items-center gap-3 mb-3">
                    {(() => {
                      const Icon = CLASS_ICONS[singleResult.predicted_class] || ImageIcon;
                      return (
                        <div className="kpi-icon" style={{ background: CLASS_COLORS[singleResult.predicted_class] + '20', color: CLASS_COLORS[singleResult.predicted_class] }}>
                          <Icon size={28} />
                        </div>
                      );
                    })()}
                    <div>
                      <h4 style={{ fontSize: '1.2rem', fontWeight: 700, textTransform: 'capitalize', marginBottom: 0 }}>
                        {singleResult.predicted_class.replace('_', ' ')}
                      </h4>
                      <p className="text-muted small mb-0">
                        {singleResult.is_ewaste ? 'Electronic Waste Detected' : 'Non-E-Waste'}
                        {singleResult.device_category && ` • Category: ${singleResult.device_category}`}
                      </p>
                    </div>
                  </div>

                  <div className="row g-3 mb-3">
                    <div className="col-4">
                      <div className="text-center p-2 rounded" style={{ background: '#f8fafc' }}>
                        <div style={{ fontWeight: 700, fontSize: '1.3rem', color: '#6366f1' }}>
                          {(singleResult.confidence * 100).toFixed(1)}%
                        </div>
                        <div className="text-muted small">Confidence</div>
                      </div>
                    </div>
                    <div className="col-4">
                      <div className="text-center p-2 rounded" style={{ background: '#f8fafc' }}>
                        <div style={{ fontWeight: 700, fontSize: '1.3rem' }}>
                          ~{singleResult.estimated_weight_kg} kg
                        </div>
                        <div className="text-muted small">Est. Weight</div>
                      </div>
                    </div>
                    <div className="col-4">
                      <div className="text-center p-2 rounded" style={{ background: '#f8fafc' }}>
                        <div style={{ fontWeight: 700, fontSize: '1.3rem' }}>
                          {singleResult.device_category || '—'}
                        </div>
                        <div className="text-muted small">Category</div>
                      </div>
                    </div>
                  </div>

                  <h6 style={{ fontSize: '0.9rem', fontWeight: 600 }}>All Class Probabilities</h6>
                  <div className="d-flex flex-column gap-2">
                    {Object.entries(singleResult.all_probabilities)
                      .sort((a, b) => b[1] - a[1])
                      .map(([cls, prob]) => {
                        const Icon = CLASS_ICONS[cls] || ImageIcon;
                        return (
                          <div key={cls} className="d-flex align-items-center gap-2">
                            <Icon size={14} style={{ color: CLASS_COLORS[cls], width: '16px' }} />
                            <span style={{ fontSize: '0.8rem', width: '130px', textTransform: 'capitalize' }}>
                              {cls.replace('_', ' ')}
                            </span>
                            <div className="flex-grow-1" style={{ height: '8px', background: '#e2e8f0', borderRadius: '4px', overflow: 'hidden' }}>
                              <div
                                style={{
                                  width: `${prob * 100}%`,
                                  height: '100%',
                                  background: CLASS_COLORS[cls],
                                  borderRadius: '4px',
                                  transition: 'width 0.5s ease',
                                }}
                              />
                            </div>
                            <span style={{ fontSize: '0.8rem', fontWeight: 600, width: '45px', textAlign: 'right' }}>
                              {(prob * 100).toFixed(1)}%
                            </span>
                          </div>
                        );
                      })}
                  </div>
                </div>
              </div>
            ) : (
              <div className="card p-5 text-center fade-in">
                <div className="empty-state">
                  <Zap className="empty-state-icon" style={{ color: '#6366f1' }} />
                  <h5 style={{ color: '#1e293b', fontWeight: 600 }}>AI-Powered Detection</h5>
                  <p className="text-muted">Upload an image to classify e-waste type, estimate weight, and categorize automatically.</p>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {mode === 'batch' && (
        <div className="fade-in">
          <div className="card p-4 mb-4">
            <h4 className="mb-3" style={{ fontSize: '1.05rem', fontWeight: 700 }}>
              <Upload size={20} style={{ marginRight: '6px', color: '#6366f1' }} />
              Batch Classification
            </h4>
            <div className="upload-zone" style={{ padding: '2rem' }}>
              <label style={{ cursor: 'pointer' }}>
                <input type="file" accept="image/*" multiple onChange={handleBatchFiles} hidden />
                <Upload size={40} style={{ color: '#10b981', marginBottom: '1rem' }} />
                <p className="mb-1" style={{ fontWeight: 600 }}>Upload multiple images</p>
                <p className="text-muted small mb-0">Select up to 20 images at once</p>
              </label>
            </div>

            {batchFiles.length > 0 && (
              <div className="mt-3">
                <p className="small text-muted mb-2">{batchFiles.length} files selected</p>
                <div className="d-flex flex-wrap gap-2 mb-3">
                  {batchFiles.slice(0, 10).map((f, i) => (
                    <span key={i} className="badge bg-light text-dark" style={{ fontSize: '0.75rem' }}>
                      {f.name}
                    </span>
                  ))}
                  {batchFiles.length > 10 && (
                    <span className="badge bg-secondary" style={{ fontSize: '0.75rem' }}>+{batchFiles.length - 10} more</span>
                  )}
                </div>
                <button className="btn-accent d-flex align-items-center gap-2" onClick={handleBatchClassify} disabled={loading}>
                  {loading ? <Loader2 size={18} className="spinner" /> : <Zap size={18} />}
                  {loading ? 'Classifying...' : 'Classify All'}
                </button>
              </div>
            )}
          </div>

          {batchResults && (
            <div className="row g-3 mb-4">
              <div className="col-md-4">
                <div className="kpi-card">
                  <div className="kpi-icon purple"><ImageIcon size={20} /></div>
                  <div>
                    <div className="kpi-value" style={{ fontSize: '1.2rem' }}>{batchResults.total}</div>
                    <div className="kpi-label">Total Images</div>
                  </div>
                </div>
              </div>
              <div className="col-md-4">
                <div className="kpi-card">
                  <div className="kpi-icon amber"><Zap size={20} /></div>
                  <div>
                    <div className="kpi-value" style={{ fontSize: '1.2rem' }}>{batchResults.ewaste_detected}</div>
                    <div className="kpi-label">E-Waste Found</div>
                  </div>
                </div>
              </div>
              <div className="col-md-4">
                <div className="kpi-card">
                  <div className="kpi-icon green"><CheckCircle size={20} /></div>
                  <div>
                    <div className="kpi-value" style={{ fontSize: '1.2rem' }}>{batchResults.non_ewaste}</div>
                    <div className="kpi-label">Non-E-Waste</div>
                  </div>
                </div>
              </div>
            </div>
          )}

          {batchResults?.results && (
            <div className="card p-3">
              <h4 className="card-title mb-3" style={{ fontSize: '1.05rem', fontWeight: 700 }}>Results</h4>
              <div className="table-responsive">
                <table className="table table-sm table-hover" style={{ fontSize: '0.85rem' }}>
                  <thead>
                    <tr>
                      <th>File</th>
                      <th>Class</th>
                      <th>Confidence</th>
                      <th>E-Waste</th>
                      <th>Category</th>
                      <th>Est. Weight</th>
                    </tr>
                  </thead>
                  <tbody>
                    {batchResults.results.map((r, i) => (
                      <tr key={i}>
                        <td>{r.filename || r.error}</td>
                        <td style={{ textTransform: 'capitalize' }}>{r.predicted_class?.replace('_', ' ') || '—'}</td>
                        <td>{r.confidence != null ? `${(r.confidence * 100).toFixed(1)}%` : '—'}</td>
                        <td>
                          {r.is_ewaste != null ? (
                            <span className={`badge-custom ${r.is_ewaste ? 'badge-purple' : 'badge-green'}`}>
                              {r.is_ewaste ? 'Yes' : 'No'}
                            </span>
                          ) : '—'}
                        </td>
                        <td>{r.device_category || '—'}</td>
                        <td>{r.estimated_weight_kg != null ? `~${r.estimated_weight_kg} kg` : '—'}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}

      {mode === 'history' && (
        <div className="fade-in">
          <div className="card p-3 mb-4">
            <div className="row g-3 align-items-end">
              <div className="col-md-4">
                <label className="form-label small">Filter by Class</label>
                <select
                  className="form-select form-select-sm"
                  value={historyFilter.class}
                  onChange={(e) => { setHistoryFilter({ ...historyFilter, class: e.target.value }); setHistoryPage(1); }}
                >
                  <option value="">All Classes</option>
                  {cvStats?.class_distribution?.map((c) => (
                    <option key={c.predicted_class} value={c.predicted_class}>{c.predicted_class.replace('_', ' ')}</option>
                  ))}
                </select>
              </div>
              <div className="col-md-3">
                <label className="form-label small">E-Waste Filter</label>
                <select
                  className="form-select form-select-sm"
                  value={historyFilter.is_ewaste}
                  onChange={(e) => { setHistoryFilter({ ...historyFilter, is_ewaste: e.target.value }); setHistoryPage(1); }}
                >
                  <option value="">All</option>
                  <option value="true">E-Waste Only</option>
                  <option value="false">Non-E-Waste Only</option>
                </select>
              </div>
              <div className="col-md-5 text-end">
                <span className="text-muted small">{historyTotal} total records</span>
              </div>
            </div>
          </div>

          {historyLoading ? (
            <div className="spinner-container">
              <Loader2 className="spinner" size={40} />
              <p className="mt-3 mb-0">Loading classification history...</p>
            </div>
          ) : history.length > 0 ? (
            <div>
              <div className="card p-3 mb-4">
                <div className="table-responsive">
                  <table className="table table-sm table-hover" style={{ fontSize: '0.85rem' }}>
                    <thead>
                      <tr>
                        <th>Filename</th>
                        <th>Predicted Class</th>
                        <th>Confidence</th>
                        <th>E-Waste</th>
                        <th>Category</th>
                        <th>Est. Weight</th>
                        <th>Time</th>
                      </tr>
                    </thead>
                    <tbody>
                      {history.map((h) => (
                        <tr key={h.id}>
                          <td>{h.filename}</td>
                          <td style={{ textTransform: 'capitalize' }}>
                            {(() => {
                              const Icon = CLASS_ICONS[h.predicted_class] || ImageIcon;
                              return (
                                <span className="d-flex align-items-center gap-1">
                                  <Icon size={14} style={{ color: CLASS_COLORS[h.predicted_class] }} />
                                  {h.predicted_class.replace('_', ' ')}
                                </span>
                              );
                            })()}
                          </td>
                          <td>{(parseFloat(h.confidence) * 100).toFixed(1)}%</td>
                          <td>
                            <span className={`badge-custom ${h.is_ewaste ? 'badge-purple' : 'badge-green'}`}>
                              {h.is_ewaste ? 'Yes' : 'No'}
                            </span>
                          </td>
                          <td>{h.device_category || '—'}</td>
                          <td>{h.estimated_weight_kg ? `~${h.estimated_weight_kg} kg` : '—'}</td>
                          <td className="text-muted">{new Date(h.created_at).toLocaleString()}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {totalPages > 1 && (
                <div className="d-flex justify-content-center gap-2">
                  <button
                    className="btn btn-sm btn-outline-secondary"
                    disabled={historyPage <= 1}
                    onClick={() => setHistoryPage(historyPage - 1)}
                  >
                    Previous
                  </button>
                  <span className="btn btn-sm btn-outline-secondary disabled">
                    Page {historyPage} of {totalPages}
                  </span>
                  <button
                    className="btn btn-sm btn-outline-secondary"
                    disabled={historyPage >= totalPages}
                    onClick={() => setHistoryPage(historyPage + 1)}
                  >
                    Next
                  </button>
                </div>
              )}
            </div>
          ) : (
            <div className="card p-5 text-center">
              <div className="empty-state">
                <History className="empty-state-icon" style={{ color: '#6366f1' }} />
                <h5 style={{ color: '#1e293b', fontWeight: 600 }}>No Classification History</h5>
                <p className="text-muted">Classify some images to see your history here.</p>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default CV;
