import React, { useState, useEffect } from 'react';
import { Wrench, Recycle, CheckCircle, Activity, Box, Settings, Trash2 } from 'lucide-react';

const ServiceDashboard = () => {
  const [activeTab, setActiveTab] = useState('repair');
  const [jobs, setJobs] = useState([]);

  // Form State
  const [jobType, setJobType] = useState('Repair');
  const [deviceMaterial, setDeviceMaterial] = useState('');
  const [issue, setIssue] = useState('');
  const [weight, setWeight] = useState(0);

  useEffect(() => {
    fetchJobs();
  }, []);

  const fetchJobs = async () => {
    try {
      const resp = await fetch('http://localhost:5000/api/service/jobs');
      if (resp.ok) {
        setJobs(await resp.json());
      }
    } catch (err) {
      console.error('Error fetching jobs', err);
    }
  };

  const handleAddJob = async (e) => {
    e.preventDefault();
    try {
      await fetch('http://localhost:5000/api/service/jobs', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          job_type: activeTab === 'repair' ? 'Repair' : 'Scrap',
          device_or_material: deviceMaterial,
          issue_or_details: issue,
          weight_kg: weight
        })
      });
      fetchJobs();
      setDeviceMaterial('');
      setIssue('');
      setWeight(0);
    } catch (err) {
      console.error('Failed to add job', err);
    }
  };

  const handleUpdateStatus = async (id, newStatus) => {
    try {
      await fetch(`http://localhost:5000/api/service/jobs/${id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: newStatus })
      });
      fetchJobs();
    } catch (err) {
      console.error('Failed to update status', err);
    }
  };

  const handleDelete = async (id) => {
    try {
      await fetch(`http://localhost:5000/api/service/jobs/${id}`, {
        method: 'DELETE'
      });
      fetchJobs();
    } catch (err) {
      console.error('Failed to delete job', err);
    }
  };

  const repairJobs = jobs.filter(j => j.job_type === 'Repair');
  const scrapJobs = jobs.filter(j => j.job_type === 'Scrap');

  return (
    <div className="container mt-4">
      <div className="d-flex justify-content-between align-items-center mb-4">
        <div>
          <h2>Service & Recycling Center</h2>
          <p className="text-muted">Process incoming e-waste for repair or material recovery</p>
        </div>
      </div>

      <div className="row mb-4">
        <div className="col-md-4">
          <div className="premium-card bg-white">
            <div className="card-body d-flex align-items-center">
              <div className="bg-primary bg-opacity-10 p-3 rounded me-3 text-primary">
                <Box size={24} />
              </div>
              <div>
                <h6 className="text-muted mb-0">Total Jobs</h6>
                <h3 className="fw-bold mb-0">{jobs.length}</h3>
              </div>
            </div>
          </div>
        </div>
        <div className="col-md-4">
          <div className="premium-card bg-white">
            <div className="card-body d-flex align-items-center">
              <div className="bg-success bg-opacity-10 p-3 rounded me-3 text-success">
                <Wrench size={24} />
              </div>
              <div>
                <h6 className="text-muted mb-0">Repaired & Refurbished</h6>
                <h3 className="fw-bold mb-0">{repairJobs.filter(j => j.status === 'Completed').length} units</h3>
              </div>
            </div>
          </div>
        </div>
        <div className="col-md-4">
          <div className="premium-card bg-white">
            <div className="card-body d-flex align-items-center">
              <div className="bg-danger bg-opacity-10 p-3 rounded me-3 text-danger">
                <Recycle size={24} />
              </div>
              <div>
                <h6 className="text-muted mb-0">Materials Recovered</h6>
                <h3 className="fw-bold mb-0">
                  {scrapJobs.filter(j => j.status === 'Completed').reduce((acc, j) => acc + Number(j.weight_kg || 0), 0).toFixed(2)} kg
                </h3>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="premium-card mb-4">
        <div className="card-header bg-white border-bottom-0 pb-0 pt-3">
          <ul className="nav nav-tabs border-bottom-0">
            <li className="nav-item cursor-pointer" style={{ cursor: 'pointer' }}>
              <a className={`nav-link fw-bold ${activeTab === 'repair' ? 'active border-bottom border-success border-3 text-success' : 'text-muted border-0'}`} onClick={() => setActiveTab('repair')}>
                <Wrench size={18} className="me-2"/> Repair Handling
              </a>
            </li>
            <li className="nav-item cursor-pointer" style={{ cursor: 'pointer' }}>
              <a className={`nav-link fw-bold ${activeTab === 'scrap' ? 'active border-bottom border-danger border-3 text-danger' : 'text-muted border-0'}`} onClick={() => setActiveTab('scrap')}>
                <Recycle size={18} className="me-2"/> Scrap Processing
              </a>
            </li>
          </ul>
        </div>
        <div className="card-body bg-light">
          
          <div className="bg-white p-4 rounded shadow-sm border mb-4">
             <h5>Log New {activeTab === 'repair' ? 'Repair' : 'Scrap Batch'} Job</h5>
             <form className="row g-3 mt-2" onSubmit={handleAddJob}>
                <div className="col-md-4">
                  <label className="form-label">{activeTab === 'repair' ? 'Device Model' : 'Material Type'}</label>
                  <input type="text" className="form-control" value={deviceMaterial} onChange={e=>setDeviceMaterial(e.target.value)} required />
                </div>
                <div className="col-md-4">
                  <label className="form-label">{activeTab === 'repair' ? 'Reported Issue' : 'Extraction Info'}</label>
                  <input type="text" className="form-control" value={issue} onChange={e=>setIssue(e.target.value)} />
                </div>
                {activeTab === 'scrap' && (
                  <div className="col-md-4">
                    <label className="form-label">Weight (kg)</label>
                    <input type="number" className="form-control" value={weight} onChange={e=>setWeight(e.target.value)} required />
                  </div>
                )}
                <div className="col-12 mt-3">
                  <button type="submit" className="btn btn-primary">Add Job</button>
                </div>
             </form>
          </div>

          <div className="bg-white p-0 rounded shadow-sm border overflow-hidden table-responsive">
            {activeTab === 'repair' && (
              <table className="table table-hover mb-0">
                <thead className="table-light">
                  <tr>
                    <th className="ps-4">Job ID</th>
                    <th>Device</th>
                    <th>Issue</th>
                    <th>Status</th>
                    <th className="pe-4 text-end">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {repairJobs.map(job => (
                    <tr key={job.id}>
                      <td className="ps-4 fw-bold text-muted">#REP-{1000 + job.id}</td>
                      <td>{job.device_or_material}</td>
                      <td>{job.issue_or_details}</td>
                      <td>
                        <span className={`badge ${job.status === 'Completed' ? 'bg-success' : job.status === 'Failed' ? 'bg-danger' : job.status === 'In Progress' ? 'bg-warning text-dark' : 'bg-secondary'}`}>
                          {job.status}
                        </span>
                      </td>
                      <td className="pe-4 text-end">
                        {job.status !== 'Completed' && job.status !== 'Failed' && (
                          <>
                            {job.status === 'Pending' && <button onClick={() => handleUpdateStatus(job.id, 'In Progress')} className="btn btn-sm btn-primary me-2">Start Work</button>}
                            {job.status === 'In Progress' && <button onClick={() => handleUpdateStatus(job.id, 'Completed')} className="btn btn-sm btn-success me-2"><CheckCircle size={14} className="me-1"/> Done</button>}
                            <button onClick={() => handleUpdateStatus(job.id, 'Failed')} className="btn btn-sm btn-outline-danger me-2">Fail</button>
                          </>
                        )}
                        <button onClick={() => handleDelete(job.id)} className="btn btn-sm btn-outline-danger"><Trash2 size={14} /></button>
                      </td>
                    </tr>
                  ))}
                  {repairJobs.length === 0 && (
                     <tr><td colSpan="5" className="text-center p-3 text-muted">No repair jobs</td></tr>
                  )}
                </tbody>
              </table>
            )}

            {activeTab === 'scrap' && (
              <table className="table table-hover mb-0">
                <thead className="table-light">
                  <tr>
                    <th className="ps-4">Batch ID</th>
                    <th>Material Type</th>
                    <th>Weight processing</th>
                    <th>Extract / Yield</th>
                    <th>Status</th>
                    <th className="pe-4 text-end">Record Data</th>
                  </tr>
                </thead>
                <tbody>
                  {scrapJobs.map(job => (
                    <tr key={job.id}>
                      <td className="ps-4 fw-bold text-muted">#SCR-{2000 + job.id}</td>
                      <td>{job.device_or_material}</td>
                      <td>{job.weight_kg} kg</td>
                      <td><span className="text-warning fw-bold">{job.issue_or_details}</span></td>
                      <td>
                        <span className={`badge ${job.status === 'Completed' ? 'bg-success' : 'bg-secondary'}`}>
                          {job.status}
                        </span>
                      </td>
                      <td className="pe-4 text-end">
                         {job.status !== 'Completed' && (
                           <button onClick={() => handleUpdateStatus(job.id, 'Completed')} className="btn btn-sm btn-outline-primary me-2"><Activity size={14} className="me-1"/> Log Yield</button>
                         )}
                         <button onClick={() => handleDelete(job.id)} className="btn btn-sm btn-outline-danger"><Trash2 size={14} /></button>
                      </td>
                    </tr>
                  ))}
                  {scrapJobs.length === 0 && (
                     <tr><td colSpan="6" className="text-center p-3 text-muted">No scrap jobs</td></tr>
                  )}
                </tbody>
              </table>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default ServiceDashboard;
