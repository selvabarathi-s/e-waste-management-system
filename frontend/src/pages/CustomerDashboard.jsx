import React, { useState, useEffect } from 'react';
import { Smartphone, MapPin, Truck, RefreshCw, Cpu, Award, Trash2 } from 'lucide-react';

const CustomerDashboard = () => {
  const [deviceType, setDeviceType] = useState('phone');
  const [age, setAge] = useState(3);
  const [condition, setCondition] = useState('good');
  const [suggestion, setSuggestion] = useState(null);
  const [devices, setDevices] = useState([]);

  // Mock currently logged-in user from localStorage 
  const currentUser = JSON.parse(localStorage.getItem('ewaste_user'))?.username || 'customer';

  useEffect(() => {
    fetchDevices();
  }, []);

  const fetchDevices = async () => {
    try {
      const resp = await fetch('http://localhost:5000/api/customer/devices');
      if (resp.ok) {
        const data = await resp.json();
        // Since we don't have true auth, just show devices for this user
        setDevices(data.filter(d => d.username === currentUser));
      }
    } catch (err) {
      console.error('Error fetching devices', err);
    }
  };

  const handlePredictAndSave = async (e) => {
    e.preventDefault();
    
    // Simulate AI prediction
    let action = 'Scrap';
    let desc = 'This device is too old. Send it to recycling to recover precious metals like Gold and Copper.';
    let color = 'warning';

    if (deviceType === 'phone' || age <= 4 || condition === 'good') {
      action = 'Refurbish';
      desc = 'Your device can be repaired or used as a CCTV! Connect with a local service center.';
      color = 'success';
    }

    setSuggestion({ action, desc, color });

    // Save to backend
    try {
      await fetch('http://localhost:5000/api/customer/devices', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          username: currentUser,
          device_type: deviceType,
          age_years: age,
          condition_status: condition,
          ai_suggestion: action
        })
      });
      fetchDevices(); // Refresh list
    } catch (err) {
      console.error('Failed to save device', err);
    }
  };

  const handleDelete = async (id) => {
    try {
      await fetch(`http://localhost:5000/api/customer/devices/${id}`, {
        method: 'DELETE'
      });
      fetchDevices();
    } catch (err) {
      console.error('Failed to delete device', err);
    }
  };

  return (
    <div className="container mt-4">
      <div className="d-flex justify-content-between align-items-center mb-4">
        <div>
          <h2>Customer Dashboard</h2>
          <p className="text-muted">Manage your e-waste smartly and earn rewards</p>
        </div>
        <div className="text-end">
          <span className="badge bg-primary fs-6 p-2"><Award size={18} className="me-1"/> 150 Eco Points</span>
        </div>
      </div>

      <div className="row g-4">
        <div className="col-lg-6">
          <div className="card h-100 shadow-sm border-0">
            <div className="card-header bg-white border-0 pt-4 pb-0">
              <h5 className="fw-bold"><Smartphone className="me-2" />Add New Device</h5>
            </div>
            <div className="card-body">
              <form onSubmit={handlePredictAndSave}>
                <div className="mb-3">
                  <label className="form-label">Device Type</label>
                  <select className="form-select" value={deviceType} onChange={(e) => setDeviceType(e.target.value)}>
                    <option value="phone">Smartphone</option>
                    <option value="laptop">Laptop / PC</option>
                    <option value="tv">Television</option>
                    <option value="accessories">Cables / Accessories</option>
                  </select>
                </div>
                <div className="mb-3">
                  <label className="form-label">Approximate Age (Years)</label>
                  <input type="number" className="form-control" value={age} onChange={e => setAge(e.target.value)} min="1" max="20" />
                </div>
                <div className="mb-3">
                  <label className="form-label">Physical Condition</label>
                  <select className="form-select" value={condition} onChange={e => setCondition(e.target.value)}>
                    <option value="good">Good (Working fine)</option>
                    <option value="fair">Fair (Minor issues)</option>
                    <option value="poor">Poor (Not turning on / Broken)</option>
                  </select>
                </div>
                <button type="submit" className="btn btn-primary w-100">Get AI Suggestion & Save</button>
              </form>

              {suggestion && (
                <div className={`alert alert-${suggestion.color} mt-4 d-flex align-items-start`}>
                  <Cpu className="me-3 mt-1" size={24} />
                  <div>
                    <strong>AI Recommendation: {suggestion.action}</strong>
                    <p className="mb-0 mt-1">{suggestion.desc}</p>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>

        <div className="col-lg-6">
          
          {/* List logged devices */}
          <div className="card mb-4 shadow-sm border-0">
            <div className="card-header bg-white border-0 pt-4 pb-0">
              <h5 className="fw-bold">My Logged Devices</h5>
            </div>
            <div className="card-body p-0">
              {devices.length === 0 ? (
                <p className="p-3 text-muted">No devices logged yet.</p>
              ) : (
                <ul className="list-group list-group-flush">
                  {devices.map(d => (
                    <li key={d.id} className="list-group-item d-flex justify-content-between align-items-center">
                      <div>
                        <strong>{d.device_type}</strong> ({d.age_years} yrs) - 
                        <span className={`ms-2 badge bg-${d.ai_suggestion === 'Scrap' ? 'danger' : 'success'}`}>{d.ai_suggestion}</span>
                      </div>
                      <button onClick={() => handleDelete(d.id)} className="btn btn-sm btn-outline-danger"><Trash2 size={14}/></button>
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>

          <div className="card shadow-sm border-0 mb-4">
            <div className="card-header bg-white border-0 pt-4 pb-0 d-flex justify-content-between align-items-center">
              <h5 className="fw-bold"><MapPin className="me-2" />Nearest Hubs & Centers</h5>
              <button className="btn btn-outline-secondary btn-sm">View Map</button>
            </div>
            <div className="card-body">
              <ul className="list-group list-group-flush">
                <li className="list-group-item px-0 d-flex justify-content-between align-items-center">
                  <div>
                    <h6 className="mb-0">Green Recycling Hub</h6>
                    <small className="text-muted">2.5 km away • Opens till 6 PM</small>
                  </div>
                  <button className="btn btn-sm btn-outline-primary">Directions</button>
                </li>
                <li className="list-group-item px-0 d-flex justify-content-between align-items-center">
                  <div>
                    <h6 className="mb-0">Tech Fixers Service Center</h6>
                    <small className="text-muted">3.8 km away • Repair & Refurbish</small>
                  </div>
                  <button className="btn btn-sm btn-outline-primary">Directions</button>
                </li>
              </ul>
            </div>
          </div>

          <div className="card shadow-sm border-0">
            <div className="card-header bg-white border-0 pt-4 pb-0">
              <h5 className="fw-bold"><Truck className="me-2" />Request Pickup</h5>
            </div>
            <div className="card-body">
              <p className="text-muted small mb-3">Schedule a door-step pickup. Our agent will collect your e-waste.</p>
              <button className="btn btn-dark w-100"><Truck size={18} className="me-2" /> Schedule Pickup Now</button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default CustomerDashboard;
