import React, { useState, useEffect } from 'react';
import { Package, Inbox, Truck, BarChart2, CheckCircle, Database } from 'lucide-react';

const HubDashboard = () => {
  const [activeTab, setActiveTab] = useState('inventory');
  const [items, setItems] = useState([]);
  
  // Form State
  const [source, setSource] = useState('Customer Drop-off');
  const [category, setCategory] = useState('Mixed Electronics');
  const [weight, setWeight] = useState(15);

  useEffect(() => {
    fetchInventory();
  }, []);

  const fetchInventory = async () => {
    try {
      const resp = await fetch('http://localhost:5000/api/hub/inventory');
      if (resp.ok) {
        setItems(await resp.json());
      }
    } catch (err) {
      console.error('Error fetching inventory', err);
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    
    // Simulate simple AI classification rules 
    let aiClass = 'Refurbish (Class B)';
    if (category.toLowerCase().includes('scrap') || weight < 5) {
      aiClass = 'Scrap (Class C)';
    }

    try {
      await fetch('http://localhost:5000/api/hub/inventory', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          batch_id: '#BATCH-' + Math.floor(Math.random() * 10000),
          source,
          category,
          weight_kg: weight,
          ai_classification: aiClass,
          destination: 'Pending Assignment'
        })
      });
      fetchInventory();
      setActiveTab('inventory'); // Switch to view
    } catch (err) {
      console.error('Failed to save inventory', err);
    }
  };

  const handleUpdateDestination = async (id, newDestination) => {
    try {
      await fetch(`http://localhost:5000/api/hub/inventory/${id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: 'Dispatched', destination: newDestination })
      });
      fetchInventory();
    } catch (err) {
      console.error('Failed to update destination', err);
    }
  };

  const handleDelete = async (id) => {
    try {
      await fetch(`http://localhost:5000/api/hub/inventory/${id}`, {
        method: 'DELETE'
      });
      fetchInventory();
    } catch (err) {
      console.error('Failed to delete item', err);
    }
  };

  return (
    <div className="container mt-4">
      <div className="d-flex justify-content-between align-items-center mb-4">
        <div>
          <h2>Hub / Warehouse Dashboard</h2>
          <p className="text-muted">Manage collection, classification, and logistics</p>
        </div>
      </div>

      <div className="row mb-4">
        <div className="col-md-3">
          <div className="card text-white bg-primary mb-3 shadow-sm border-0">
            <div className="card-body">
              <h6 className="card-title text-white-50">Items Received</h6>
              <h2 className="mb-0">{items.length}</h2>
            </div>
          </div>
        </div>
        <div className="col-md-3">
          <div className="card text-white bg-warning mb-3 shadow-sm border-0">
            <div className="card-body">
              <h6 className="card-title text-white-50">Pending Route</h6>
              <h2 className="mb-0">{items.filter(i => i.status === 'Pending Classification').length}</h2>
            </div>
          </div>
        </div>
        <div className="col-md-3">
          <div className="card text-white bg-success mb-3 shadow-sm border-0">
            <div className="card-body">
              <h6 className="card-title text-white-50">Total kg</h6>
              <h2 className="mb-0">{items.reduce((sum, i) => sum + Number(i.weight_kg), 0).toFixed(2)}</h2>
            </div>
          </div>
        </div>
        <div className="col-md-3">
          <div className="card text-white bg-danger mb-3 shadow-sm border-0">
            <div className="card-body">
              <h6 className="card-title text-white-50">Dispatched</h6>
              <h2 className="mb-0">{items.filter(i => i.status === 'Dispatched').length}</h2>
            </div>
          </div>
        </div>
      </div>

      <div className="premium-card mb-4">
        <div className="card-header bg-white border-bottom-0 pb-0 pt-3">
          <ul className="nav nav-tabs border-bottom-0">
            <li className="nav-item cursor-pointer" style={{ cursor: 'pointer' }}>
              <a className={`nav-link fw-bold ${activeTab === 'entry' ? 'active border-bottom border-primary border-3' : 'text-muted border-0'}`} onClick={() => setActiveTab('entry')}>
                <Inbox size={18} className="me-2"/> Log Incoming Collection
              </a>
            </li>
            <li className="nav-item cursor-pointer" style={{ cursor: 'pointer' }}>
              <a className={`nav-link fw-bold ${activeTab === 'inventory' ? 'active border-bottom border-primary border-3' : 'text-muted border-0'}`} onClick={() => setActiveTab('inventory')}>
                <Database size={18} className="me-2"/> Inventory & Routing
              </a>
            </li>
          </ul>
        </div>
        <div className="card-body bg-light">
          {activeTab === 'entry' && (
            <div className="bg-white p-4 rounded shadow-sm border">
              <h5>New Batch Entry</h5>
              <form className="row g-3 mt-2" onSubmit={handleSubmit}>
                <div className="col-md-4">
                  <label className="form-label">Source</label>
                  <select className="form-select" value={source} onChange={(e)=>setSource(e.target.value)}>
                    <option>Customer Drop-off</option>
                    <option>Scrap Dealer</option>
                    <option>Bulk Corporate Pickup</option>
                  </select>
                </div>
                <div className="col-md-4">
                  <label className="form-label">Category</label>
                  <select className="form-select" value={category} onChange={(e)=>setCategory(e.target.value)}>
                    <option>Mixed Electronics</option>
                    <option>Smartphones</option>
                    <option>Laptops / Computers</option>
                    <option>Home Appliances</option>
                  </select>
                </div>
                <div className="col-md-4">
                  <label className="form-label">Approx. Weight (kg)</label>
                  <input type="number" className="form-control" value={weight} onChange={(e)=>setWeight(e.target.value)} required />
                </div>
                <div className="col-12 mt-4">
                  <button type="submit" className="btn btn-primary"><CheckCircle size={18} className="me-2"/> Submit & Process using AI</button>
                </div>
              </form>
            </div>
          )}

          {activeTab === 'inventory' && (
            <div className="bg-white p-0 rounded shadow-sm border overflow-hidden table-responsive">
               <table className="table table-hover mb-0">
                <thead className="table-light">
                  <tr>
                    <th>Batch ID</th>
                    <th>Item Type</th>
                    <th>Weight</th>
                    <th>AI Classification</th>
                    <th>Destination Route</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {items.map(item => (
                    <tr key={item.id}>
                      <td>{item.batch_id}</td>
                      <td>{item.category}</td>
                      <td>{item.weight_kg} kg</td>
                      <td>
                        <span className={`badge ${item.ai_classification.includes('Scrap') ? 'bg-danger' : 'bg-success'}`}>
                          {item.ai_classification}
                        </span>
                      </td>
                      <td>
                        {item.status === 'Dispatched' ? <strong className="text-success">{item.destination}</strong> : item.destination}
                      </td>
                      <td>
                        {item.status !== 'Dispatched' && (
                          <div className="d-flex gap-2">
                             <button onClick={() => handleUpdateDestination(item.id, 'Main Service Center')} className="btn btn-sm btn-outline-primary whitespace-nowrap"><Truck size={14} className="me-1"/> Send to Service</button>
                             <button onClick={() => handleDelete(item.id)} className="btn btn-sm btn-outline-danger">Del</button>
                          </div>
                        )}
                      </td>
                    </tr>
                  ))}
                  {items.length === 0 && (
                    <tr><td colSpan="6" className="text-center p-3 text-muted">No inventory found</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default HubDashboard;
