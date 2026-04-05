import React, { useState, useCallback } from 'react';
import { uploadData } from '../services/api';
import { UploadCloud, FileText, CheckCircle, XCircle, Loader2, AlertCircle, Eye, Table } from 'lucide-react';

const API_BASE = import.meta.env.VITE_API_BASE_URL || 'http://localhost:5000/api';

const Upload = () => {
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [message, setMessage] = useState('');
  const [isDragActive, setIsDragActive] = useState(false);
  const [messageType, setMessageType] = useState('');
  const [validationResult, setValidationResult] = useState(null);
  const [validating, setValidating] = useState(false);
  const [showPreview, setShowPreview] = useState(false);

  const handleFileChange = useCallback((selectedFile) => {
    if (!selectedFile) return;
    const validTypes = ['.csv', '.xlsx', '.xls'];
    const ext = '.' + selectedFile.name.split('.').pop().toLowerCase();
    if (!validTypes.includes(ext)) {
      setMessage('Please select a CSV or Excel file.');
      setMessageType('error');
      return;
    }
    setFile(selectedFile);
    setMessage('');
    setMessageType('');
    setValidationResult(null);
    setShowPreview(false);
  }, []);

  const handleDrop = useCallback((e) => {
    e.preventDefault();
    setIsDragActive(false);
    const droppedFile = e.dataTransfer.files[0];
    handleFileChange(droppedFile);
  }, [handleFileChange]);

  const handleDragOver = useCallback((e) => {
    e.preventDefault();
    setIsDragActive(true);
  }, []);

  const handleDragLeave = useCallback((e) => {
    e.preventDefault();
    setIsDragActive(false);
  }, []);

  const handleValidate = async () => {
    if (!file) {
      setMessage('Please select a CSV or Excel file first.');
      setMessageType('error');
      return;
    }

    setValidating(true);
    setValidationResult(null);
    setShowPreview(false);

    try {
      const formData = new FormData();
      formData.append('file', file);
      const res = await fetch(`${API_BASE}/upload/validate`, { method: 'POST', body: formData });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error?.message || 'Validation failed');
      setValidationResult(data);
      setShowPreview(true);
    } catch (e) {
      setMessage(e.message);
      setMessageType('error');
    } finally {
      setValidating(false);
    }
  };

  const handleUpload = async () => {
    if (!file) {
      setMessage('Please select a CSV or Excel file first.');
      setMessageType('error');
      return;
    }

    setUploading(true);
    setMessage('');
    setMessageType('');

    try {
      const formData = new FormData();
      formData.append('file', file);
      const response = await uploadData(formData);
      setMessage(response.data.message || 'Data uploaded successfully!');
      setMessageType('success');
      setFile(null);
      setValidationResult(null);
      setShowPreview(false);
    } catch (error) {
      const errData = error.response?.data?.error;
      const errMsg =
        errData && typeof errData === 'object' && errData.message
          ? errData.message
          : typeof errData === 'string'
            ? errData
            : 'Upload failed. Please try again.';
      setMessage(errMsg);
      setMessageType('error');
    } finally {
      setUploading(false);
    }
  };

  const formatFileSize = (bytes) => {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / 1048576).toFixed(1) + ' MB';
  };

  return (
    <div className="row justify-content-center">
      <div className="col-lg-8">
        <div className="page-header fade-in">
          <h2>Upload E-Waste Data</h2>
          <p>Import your historical e-waste data to power analytics and predictions</p>
        </div>

        <div className="card p-4 fade-in">
          <div
            className={`upload-zone ${isDragActive ? 'drag-active' : ''}`}
            onDrop={handleDrop}
            onDragOver={handleDragOver}
            onDragLeave={handleDragLeave}
            onClick={() => document.getElementById('file-input').click()}
          >
            <UploadCloud className="upload-zone-icon" />
            <h5 style={{ fontWeight: 600, color: '#1e293b', marginBottom: '0.5rem' }}>
              {isDragActive ? 'Drop your file here' : 'Drag & drop your file here'}
            </h5>
            <p className="text-muted mb-3" style={{ fontSize: '0.9rem' }}>or click to browse files</p>
            <div className="d-flex justify-content-center gap-2">
              <span className="badge-custom badge-green">CSV</span>
              <span className="badge-custom badge-purple">Excel</span>
            </div>
          </div>

          <input
            id="file-input"
            type="file"
            accept=".csv,.xlsx,.xls"
            onChange={(e) => handleFileChange(e.target.files[0])}
            style={{ display: 'none' }}
          />

          {file && (
            <div className="mt-3 p-3" style={{ background: '#f0fdf4', borderRadius: '12px', border: '1px solid #d1fae5' }}>
              <div className="d-flex align-items-center gap-3">
                <div className="kpi-icon green" style={{ width: '44px', height: '44px', borderRadius: '10px' }}>
                  <FileText size={20} />
                </div>
                <div className="flex-grow-1">
                  <div style={{ fontWeight: 600, fontSize: '0.95rem', color: '#1e293b' }}>{file.name}</div>
                  <div style={{ fontSize: '0.8rem', color: '#64748b' }}>{formatFileSize(file.size)}</div>
                </div>
                <button
                  onClick={(e) => { e.stopPropagation(); setFile(null); setValidationResult(null); setShowPreview(false); }}
                  className="btn btn-sm"
                  style={{ color: '#94a3b8', padding: '4px' }}
                >
                  <XCircle size={20} />
                </button>
              </div>
            </div>
          )}

          {file && (
            <div className="d-flex gap-2 mt-3">
              <button
                className="btn btn-outline-primary flex-grow-1 d-flex align-items-center justify-content-center gap-2"
                onClick={handleValidate}
                disabled={validating || uploading}
              >
                {validating ? <Loader2 size={18} className="spinner" /> : <Eye size={18} />}
                {validating ? 'Validating...' : 'Validate & Preview'}
              </button>
              <button
                className="btn-primary-custom flex-grow-1 d-flex align-items-center justify-content-center gap-2"
                onClick={handleUpload}
                disabled={uploading || validating}
              >
                {uploading ? (
                  <>
                    <Loader2 size={18} className="spinner" />
                    Uploading...
                  </>
                ) : (
                  <>
                    <UploadCloud size={18} />
                    Upload Data
                  </>
                )}
              </button>
            </div>
          )}

          {message && (
            <div className={`mt-3 p-3 d-flex align-items-start gap-2 ${messageType === 'success' ? 'alert-success-custom' : 'alert-danger-custom'}`} role="alert">
              {messageType === 'success' ? <CheckCircle size={20} className="mt-1" style={{ flexShrink: 0 }} /> : <AlertCircle size={20} className="mt-1" style={{ flexShrink: 0 }} />}
              <span style={{ fontSize: '0.9rem' }}>{message}</span>
            </div>
          )}

          {showPreview && validationResult && (
            <div className="mt-4 fade-in">
              <div className="d-flex align-items-center gap-2 mb-3">
                <Table size={18} style={{ color: '#6366f1' }} />
                <h5 style={{ fontWeight: 700, fontSize: '1rem', marginBottom: 0 }}>Data Validation Report</h5>
              </div>

              <div className="row g-3 mb-3">
                <div className="col-md-3">
                  <div className="text-center p-2 rounded" style={{ background: validationResult.is_valid ? '#f0fdf4' : '#fef2f2' }}>
                    <div style={{ fontWeight: 700, fontSize: '1.3rem', color: validationResult.is_valid ? '#10b981' : '#ef4444' }}>
                      {validationResult.stats.valid_rows}/{validationResult.stats.total_rows}
                    </div>
                    <div className="text-muted small">Valid Rows</div>
                  </div>
                </div>
                <div className="col-md-3">
                  <div className="text-center p-2 rounded" style={{ background: '#f8fafc' }}>
                    <div style={{ fontWeight: 700, fontSize: '1.3rem' }}>{validationResult.stats.unique_regions.length}</div>
                    <div className="text-muted small">Unique Regions</div>
                  </div>
                </div>
                <div className="col-md-3">
                  <div className="text-center p-2 rounded" style={{ background: '#f8fafc' }}>
                    <div style={{ fontWeight: 700, fontSize: '1.3rem' }}>{validationResult.stats.year_range.min}-{validationResult.stats.year_range.max}</div>
                    <div className="text-muted small">Year Range</div>
                  </div>
                </div>
                <div className="col-md-3">
                  <div className="text-center p-2 rounded" style={{ background: '#f8fafc' }}>
                    <div style={{ fontWeight: 700, fontSize: '1.3rem' }}>{validationResult.stats.total_disposal}t</div>
                    <div className="text-muted small">Total Disposal</div>
                  </div>
                </div>
              </div>

              {validationResult.errors.length > 0 && (
                <div className="alert-danger-custom p-3 mb-3">
                  <strong className="d-flex align-items-center gap-2 mb-2" style={{ fontSize: '0.9rem' }}>
                    <AlertCircle size={16} />
                    {validationResult.errors.length} Error{validationResult.errors.length > 1 ? 's' : ''} Found
                  </strong>
                  <div style={{ maxHeight: '150px', overflowY: 'auto' }}>
                    {validationResult.errors.slice(0, 5).map((err, i) => (
                      <div key={i} className="small mb-1">
                        <strong>Row {err.row}:</strong> {err.errors.join('; ')}
                      </div>
                    ))}
                    {validationResult.errors.length > 5 && (
                      <div className="small text-muted">...and {validationResult.errors.length - 5} more errors</div>
                    )}
                  </div>
                </div>
              )}

              {validationResult.is_valid && (
                <div className="alert-success-custom p-3 mb-3">
                  <strong className="d-flex align-items-center gap-2" style={{ fontSize: '0.9rem' }}>
                    <CheckCircle size={16} />
                    All rows are valid! Ready to upload.
                  </strong>
                </div>
              )}

              {validationResult.preview && (
                <div className="card p-3">
                  <h6 style={{ fontWeight: 600, fontSize: '0.9rem', marginBottom: '0.5rem' }}>Preview (First 5 Rows)</h6>
                  <div className="table-responsive">
                    <table className="table table-sm" style={{ fontSize: '0.8rem' }}>
                      <thead>
                        <tr>
                          <th>Row</th>
                          <th>Region</th>
                          <th>Year</th>
                          <th>Sales (t)</th>
                          <th>Disposal (t)</th>
                        </tr>
                      </thead>
                      <tbody>
                        {validationResult.preview.map((p) => (
                          <tr key={p.row}>
                            <td>{p.row}</td>
                            <td>{p.region}</td>
                            <td>{p.year}</td>
                            <td>{p.sales}</td>
                            <td>{p.disposal}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </div>
          )}

          <div className="mt-4 p-3" style={{ background: '#f8fafc', borderRadius: '12px' }}>
            <h6 style={{ fontWeight: 600, fontSize: '0.85rem', color: '#64748b', marginBottom: '0.75rem', textTransform: 'uppercase', letterSpacing: '0.5px' }}>Required Columns</h6>
            <div className="d-flex flex-wrap gap-2">
              {['region', 'year', 'sales_import_tonnes', 'population_millions', 'disposal_amount_tonnes', 'device_category (optional)'].map((col) => (
                <code key={col} style={{ background: '#e2e8f0', padding: '4px 10px', borderRadius: '6px', fontSize: '0.8rem', color: '#475569' }}>{col}</code>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Upload;
