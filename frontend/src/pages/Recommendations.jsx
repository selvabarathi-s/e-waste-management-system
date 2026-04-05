import React, { useState, useEffect } from 'react';
import {
  MapPin,
  MapPinned,
  Loader2,
  AlertCircle,
  Lightbulb,
  Recycle,
  Trash2,
  Info,
  BarChart3,
  Download,
  Trophy,
  Activity,
  Zap,
  ShieldCheck,
  ChevronRight
} from 'lucide-react';

const API_BASE = import.meta.env.VITE_API_BASE_URL || 'http://localhost:5000/api';

const Recommendations = () => {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [activeTab, setActiveTab] = useState('centers');
  const [rankedData, setRankedData] = useState(null);
  const [rankedLoading, setRankedLoading] = useState(false);

  useEffect(() => {
    loadData();
    loadRanked();
  }, []);

  const loadData = async () => {
    try {
      setLoading(true);
      setError('');
      const res = await fetch(`${API_BASE}/recommendations`);
      if (!res.ok) throw new Error('Failed to load recommendations');
      const json = await res.json();
      setData(json);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  const loadRanked = async () => {
    try {
      setRankedLoading(true);
      const res = await fetch(`${API_BASE}/recommendations/ranked`);
      if (!res.ok) throw new Error('Failed to load ranked recommendations');
      const json = await res.json();
      setRankedData(json);
    } catch (e) {
      console.error('Failed to load ranked', e);
    } finally {
      setRankedLoading(false);
    }
  };

  const exportCSV = (rows, filename) => {
    if (!rows || rows.length === 0) return;
    const headers = Object.keys(rows[0]);
    const csvContent = [
      headers.join(','),
      ...rows.map((r) => headers.map((h) => `"${r[h] ?? ''}"`).join(',')),
    ].join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  };

  if (loading) {
    return (
      <div className="d-flex flex-column align-items-center justify-content-center" style={{ minHeight: '60vh' }}>
        <Loader2 className="spinner" size={50} style={{ color: '#6366f1' }}/>
        <p className="mt-4 fw-bold text-muted" style={{ letterSpacing: '1px' }}>SYNTHESIZING E-WASTE INTELLIGENCE...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="glass-card p-5 text-center fade-in">
        <div className="empty-state">
          <AlertCircle className="empty-state-icon" style={{ color: '#ef4444' }} />
          <h4 className="fw-bolder" style={{ color: '#1e293b' }}>Failed to Load Recommendations</h4>
          <p className="text-muted mb-4">{error}</p>
          <button className="btn-primary-custom" onClick={loadData}>Retry Connection</button>
        </div>
      </div>
    );
  }

  const centers = data?.recycling_centers?.centers || [];
  const bins = data?.bin_placements?.placements || [];
  const insights = data?.ai_insights || [];
  const rankedCenters = rankedData?.ranked_centers || [];
  const rankedBins = rankedData?.ranked_bins || [];
  const summary = rankedData?.summary || {};

  return (
    <div className="recommendations-page">
      <div className="page-header fade-in">
        <div className="d-inline-flex align-items-center justify-content-center glow-icon p-3 rounded-circle mb-3">
          <Zap size={32} color="#f59e0b" />
        </div>
        <h2 className="display-5 fw-extrabold hero-gradient-text">Smart Recommendations</h2>
        <p className="lead text-secondary opacity-75">AI-powered analytics for optimal resource distribution & facility mapping</p>
      </div>

      <div className="tab-navigation glass-panel mb-5 fade-in fade-in-delay-1 p-2 rounded-pill shadow-sm d-flex justify-content-center">
        <div className="btn-group" role="group" style={{ width: '100%', maxWidth: '600px' }}>
          <button
            className={`btn rounded-pill px-4 py-2 ${activeTab === 'centers' ? 'btn-gradient-active text-white' : 'btn-ghost text-muted'}`}
            onClick={() => setActiveTab('centers')}
            style={{ fontWeight: 600, border: 'none', transition: 'all 0.3s ease' }}
          >
            <Recycle size={18} className="me-2" />
            Facilities ({centers.length})
          </button>
          <button
            className={`btn rounded-pill px-4 py-2 ${activeTab === 'bins' ? 'btn-gradient-active text-white' : 'btn-ghost text-muted'}`}
            onClick={() => setActiveTab('bins')}
            style={{ fontWeight: 600, border: 'none', transition: 'all 0.3s ease' }}
          >
            <Trash2 size={18} className="me-2" />
            Bin Placements ({bins.length})
          </button>
          <button
            className={`btn rounded-pill px-4 py-2 ${activeTab === 'ranked' ? 'btn-gradient-active text-white' : 'btn-ghost text-muted'}`}
            onClick={() => setActiveTab('ranked')}
            style={{ fontWeight: 600, border: 'none', transition: 'all 0.3s ease' }}
          >
            <Trophy size={18} className="me-2" />
            Rankings
          </button>
        </div>
      </div>

      {insights.length > 0 && activeTab !== 'ranked' && (
        <div className="row g-4 mb-5 fade-in fade-in-delay-2">
          {insights.map((insight, i) => (
            <div key={i} className="col-lg-6">
              <div className={`premium-card h-100 p-4 position-relative overflow-hidden ${insight.type === 'urgent' ? 'border-danger-glow' : 'border-warning-glow'}`}>
                <div className="bg-blur-shape"></div>
                <div className="d-flex align-items-start gap-3 position-relative z-1">
                  <div className={`icon-box rounded-circle p-3 ${insight.type === 'urgent' ? 'bg-danger-soft text-danger' : 'bg-warning-soft text-warning'}`}>
                    {insight.type === 'urgent' ? <AlertCircle size={24} /> : <Lightbulb size={24} />}
                  </div>
                  <div>
                    <h5 className="fw-bold mb-2">{insight.title}</h5>
                    <p className="text-muted small mb-0 lh-lg">{insight.description}</p>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {activeTab === 'centers' && (
        <div className="row g-4 fade-in fade-in-delay-3">
          {centers.length > 0 ? (
            centers.map((c, i) => (
              <div key={i} className="col-md-6 col-xl-4">
                <div className="premium-card center-card p-0 h-100">
                  <div className="card-header-gradient p-4 text-white d-flex justify-content-between align-items-start"
                       style={{ background: c.severity === 'High' ? 'linear-gradient(135deg, #ef4444 0%, #b91c1c 100%)' :
                                           c.severity === 'Medium' ? 'linear-gradient(135deg, #f59e0b 0%, #d97706 100%)' :
                                           'linear-gradient(135deg, #10b981 0%, #059669 100%)' }}>
                    <div>
                      <span className="badge bg-white text-dark mb-2 px-3 py-1 rounded-pill fw-bold">Priority: {c.severity}</span>
                      <h5 className="mb-0 fw-bolder text-white d-flex align-items-center gap-2">
                        <MapPin size={20} /> {c.region}
                      </h5>
                    </div>
                    <Activity size={24} className="opacity-50" />
                  </div>
                  <div className="p-4 bg-white rounded-bottom">
                    <div className="d-flex justify-content-between mb-4 pb-3 border-bottom">
                      <div>
                        <div className="text-secondary small fw-semibold text-uppercase tracking-wider">Score</div>
                        <div className="display-6 fw-bold" style={{ color: '#1e293b' }}>{c.priority_score}</div>
                      </div>
                      <div className="text-end">
                        <div className="text-secondary small fw-semibold text-uppercase tracking-wider">Capacity</div>
                        <div className="display-6 fw-bold" style={{ color: '#1e293b' }}>{c.estimated_capacity_tonnes}<span className="fs-6 text-muted">t</span></div>
                      </div>
                    </div>
                    <div className="d-flex align-items-start gap-2 text-muted small bg-light p-3 rounded">
                      <Info size={16} className="mt-1 flex-shrink-0 text-primary" />
                      <span className="lh-base">{c.rationale}</span>
                    </div>
                  </div>
                </div>
              </div>
            ))
          ) : (
            <div className="col-12">
              <div className="glass-card p-5 text-center">
                <MapPinned size={64} className="text-muted opacity-50 mb-3" />
                <h4 className="fw-bold">No Facility Placements</h4>
                <p className="text-muted">Insufficient data to generate recycling center recommendations.</p>
              </div>
            </div>
          )}
        </div>
      )}

      {activeTab === 'bins' && (
        <div className="glass-card p-4 fade-in fade-in-delay-3" style={{ borderRadius: '24px' }}>
          {bins.length > 0 ? (
            <div className="table-responsive px-2">
              <table className="table table-borderless align-middle custom-hover-table mb-0">
                <thead className="thead-light">
                  <tr className="text-uppercase text-secondary small fw-bold tracking-wider" style={{ borderBottom: '2px solid #f1f5f9' }}>
                    <th className="pb-3 ps-3">Region Location</th>
                    <th className="pb-3">Coordinates</th>
                    <th className="pb-3">Bin Configuration</th>
                    <th className="pb-3">Priority</th>
                    <th className="pb-3">Estimated Fill</th>
                  </tr>
                </thead>
                <tbody>
                  {bins.map((b, i) => (
                    <tr key={i} className="bg-white shadow-sm-hover rounded-row transition-all">
                      <td className="py-4 ps-3">
                        <div className="d-flex align-items-center gap-3">
                          <div className="icon-circle bg-light text-primary"><MapPin size={18}/></div>
                          <strong className="text-dark fs-6">{b.region}</strong>
                        </div>
                      </td>
                      <td className="py-4 text-muted small font-monospace">
                        {b.latitude.toFixed(4)}, {b.longitude.toFixed(4)}
                      </td>
                      <td className="py-4">
                        <span className="fw-semibold text-dark d-flex align-items-center gap-2">
                          <Trash2 size={16} className="text-muted"/> {b.bin_type}
                        </span>
                      </td>
                      <td className="py-4">
                        <span className={`badge-pill ${b.priority === 'High' ? 'badge-danger-glow' : b.priority === 'Medium' ? 'badge-warning-glow' : 'badge-success-glow'}`}>
                          {b.priority}
                        </span>
                      </td>
                      <td className="py-4">
                        <div className="d-flex align-items-center gap-2 text-muted fw-medium">
                          <RefreshCw size={16} /> Every {b.estimated_fill_rate_days} days
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="text-center p-5">
              <Trash2 size={64} className="text-muted opacity-50 mb-3" />
              <h4 className="fw-bold">No Bin Deployments</h4>
              <p className="text-muted">Need more granular regional data for bin placement.</p>
            </div>
          )}
        </div>
      )}

      {activeTab === 'ranked' && (
        <div className="fade-in fade-in-delay-3">
          {rankedLoading ? (
            <div className="d-flex flex-column align-items-center p-5">
              <Loader2 className="spinner mb-3" size={40} style={{ color: '#6366f1' }}/>
              <p className="text-muted fw-bold">Compiling priority matrices...</p>
            </div>
          ) : rankedData ? (
            <>
              <div className="row g-4 mb-5">
                <div className="col-sm-6 col-xl-3">
                  <div className="kpi-glass-card purple">
                    <div className="kpi-glass-icon"><ShieldCheck size={28} /></div>
                    <div className="ms-4">
                      <div className="kpi-glass-val">{summary.total_centers}</div>
                      <div className="kpi-glass-lbl">Total Centers</div>
                    </div>
                  </div>
                </div>
                <div className="col-sm-6 col-xl-3">
                  <div className="kpi-glass-card red">
                    <div className="kpi-glass-icon"><AlertCircle size={28} /></div>
                    <div className="ms-4">
                      <div className="kpi-glass-val">{summary.critical_centers}</div>
                      <div className="kpi-glass-lbl">Critical Facilities</div>
                    </div>
                  </div>
                </div>
                <div className="col-sm-6 col-xl-3">
                  <div className="kpi-glass-card green">
                    <div className="kpi-glass-icon"><Trash2 size={28} /></div>
                    <div className="ms-4">
                      <div className="kpi-glass-val">{summary.total_bins}</div>
                      <div className="kpi-glass-lbl">Deployed Bins</div>
                    </div>
                  </div>
                </div>
                <div className="col-sm-6 col-xl-3">
                  <div className="kpi-glass-card amber">
                    <div className="kpi-glass-icon"><Zap size={28} /></div>
                    <div className="ms-4">
                      <div className="kpi-glass-val">{summary.high_priority_bins}</div>
                      <div className="kpi-glass-lbl">High Priority Bins</div>
                    </div>
                  </div>
                </div>
              </div>

              {rankedCenters.length > 0 && (
                <div className="glass-card p-4 mb-5" style={{ borderRadius: '24px' }}>
                  <div className="d-flex justify-content-between align-items-center mb-4">
                    <h4 className="fw-extrabold m-0 d-flex align-items-center gap-3">
                      <div className="icon-circle bg-warning-soft text-warning p-2"><Trophy size={20} /></div>
                      Master Facility Rankings
                    </h4>
                    <button className="btn-modern-export" onClick={() => exportCSV(rankedCenters, 'recycling_centers_ranked.csv')}>
                      <Download size={16} className="me-2"/> Export CSV
                    </button>
                  </div>
                  <div className="table-responsive px-2">
                    <table className="table table-borderless align-middle custom-hover-table mb-0">
                      <thead className="thead-light">
                        <tr className="text-uppercase text-secondary small fw-bold tracking-wider" style={{ borderBottom: '2px solid #f1f5f9' }}>
                          <th className="pb-3 ps-3">Rank</th>
                          <th className="pb-3">Hub Region</th>
                          <th className="pb-3">Priority Base</th>
                          <th className="pb-3">Capacity (Tons)</th>
                          <th className="pb-3">Tier</th>
                        </tr>
                      </thead>
                      <tbody>
                        {rankedCenters.map((c, i) => (
                          <tr key={c.id || c.rank} className="bg-white shadow-sm-hover rounded-row">
                            <td className="py-4 ps-4">
                              <span className={`rank-badge ${i < 3 ? 'top-rank' : 'normal-rank'}`}>#{c.rank}</span>
                            </td>
                            <td className="py-4"><strong className="fs-6 text-dark">{c.region}</strong></td>
                            <td className="py-4">
                              <div className="score-bar-container">
                                <span className="fw-bold me-3">{c.priority_score}</span>
                                <div className="score-bar-bg"><div className="score-bar-fill" style={{ width: `${Math.min(c.priority_score, 100)}%`, background: i < 3 ? '#ef4444' : '#6366f1' }}></div></div>
                              </div>
                            </td>
                            <td className="py-4 fw-semibold">{c.estimated_capacity_tonnes} <span className="text-muted fw-normal">t</span></td>
                            <td className="py-4">
                              <span className={`badge-pill ${c.priority_tier === 'critical' ? 'badge-danger-glow' : c.priority_tier === 'high' ? 'badge-success-glow' : 'bg-light text-secondary'}`}>
                                {c.priority_tier.toUpperCase()}
                              </span>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}

              {rankedBins.length > 0 && (
                <div className="glass-card p-4" style={{ borderRadius: '24px' }}>
                  <div className="d-flex justify-content-between align-items-center mb-4">
                    <h4 className="fw-extrabold m-0 d-flex align-items-center gap-3">
                      <div className="icon-circle bg-primary-soft text-primary p-2"><MapPinned size={20} /></div>
                      Tactical Bin Deployment Priority
                    </h4>
                    <button className="btn-modern-export" onClick={() => exportCSV(rankedBins, 'bin_placements_ranked.csv')}>
                      <Download size={16} className="me-2"/> Export CSV
                    </button>
                  </div>
                  <div className="table-responsive px-2">
                    <table className="table table-borderless align-middle custom-hover-table mb-0">
                      <thead className="thead-light">
                        <tr className="text-uppercase text-secondary small fw-bold tracking-wider" style={{ borderBottom: '2px solid #f1f5f9' }}>
                          <th className="pb-3 ps-3">Rank</th>
                          <th className="pb-3">Location Target</th>
                          <th className="pb-3">Required Bin Base</th>
                          <th className="pb-3">Urgency Rating</th>
                          <th className="pb-3">Turnaround Time</th>
                          <th></th>
                        </tr>
                      </thead>
                      <tbody>
                        {rankedBins.map((b) => (
                          <tr key={b.rank} className="bg-white shadow-sm-hover rounded-row">
                            <td className="py-4 ps-4">
                              <span className="fw-black text-muted fs-5">#{b.rank}</span>
                            </td>
                            <td className="py-4"><strong className="text-dark">{b.region}</strong></td>
                            <td className="py-4"><span className="fw-medium text-secondary">{b.bin_type}</span></td>
                            <td className="py-4">
                              <div className="d-flex gap-1 rating-dots">
                                {[1,2,3].map(star => (
                                  <div key={star} className={`rating-dot ${star <= b.urgency_score ? 'active' : ''}`}></div>
                                ))}
                              </div>
                            </td>
                            <td className="py-4">
                              <span className="bg-light px-3 py-1 rounded-pill small fw-semibold text-muted">
                                {b.estimated_fill_rate_days} Days
                              </span>
                            </td>
                            <td className="py-4 text-end pe-4">
                              <ChevronRight className="text-muted opacity-50" />
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </>
          ) : (
            <div className="glass-card p-5 text-center">
              <Trophy size={64} className="text-muted opacity-50 mb-3" />
              <h4 className="fw-bold">No Rankings Synthesized</h4>
              <p className="text-muted">Ensure data is uploaded to calculate regional priorities.</p>
            </div>
          )}
        </div>
      )}
    </div>
  );
};

export default Recommendations;
