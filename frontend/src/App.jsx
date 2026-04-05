import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import Navbar from './components/Navbar';
import Footer from './components/Footer';

// Admin Pages Component imports
import Dashboard from './pages/Dashboard';
import Upload from './pages/Upload';
import Forecast from './pages/Forecast';
import Simulation from './pages/Simulation';
import Sustainability from './pages/Sustainability';
import Recommendations from './pages/Recommendations';
import CV from './pages/CV';

// New Pages
import Login from './pages/Login';
import CustomerDashboard from './pages/CustomerDashboard';
import HubDashboard from './pages/HubDashboard';
import ServiceDashboard from './pages/ServiceDashboard';

import 'bootstrap/dist/css/bootstrap.min.css';

function App() {
  const [user, setUser] = useState(null);

  useEffect(() => {
    // Load from local storage if available
    const savedUser = localStorage.getItem('ewaste_user');
    if (savedUser) {
      setUser(JSON.parse(savedUser));
    }
  }, []);

  const handleLogin = (userData) => {
    setUser(userData);
    localStorage.setItem('ewaste_user', JSON.stringify(userData));
  };

  const handleLogout = () => {
    setUser(null);
    localStorage.removeItem('ewaste_user');
  };

  return (
    <Router>
      <Navbar user={user} onLogout={handleLogout} />
      <div className="container mt-4 mb-5" style={{ minHeight: '80vh' }}>
        <Routes>
          {!user ? (
            <>
              <Route path="*" element={<Login onLogin={handleLogin} />} />
            </>
          ) : (
            <>
              {user.role === 'admin' && (
                <>
                  <Route path="/" element={<Dashboard />} />
                  <Route path="/upload" element={<Upload />} />
                  <Route path="/forecast" element={<Forecast />} />
                  <Route path="/simulation" element={<Simulation />} />
                  <Route path="/sustainability" element={<Sustainability />} />
                  <Route path="/recommendations" element={<Recommendations />} />
                  <Route path="/cv" element={<CV />} />
                  <Route path="*" element={<Navigate to="/" replace />} />
                </>
              )}

              {user.role === 'hub' && (
                <>
                  <Route path="/hub" element={<HubDashboard />} />
                  <Route path="*" element={<Navigate to="/hub" replace />} />
                </>
              )}

              {user.role === 'customer' && (
                <>
                  <Route path="/customer" element={<CustomerDashboard />} />
                  <Route path="*" element={<Navigate to="/customer" replace />} />
                </>
              )}

              {user.role === 'service' && (
                <>
                  <Route path="/service" element={<ServiceDashboard />} />
                  <Route path="*" element={<Navigate to="/service" replace />} />
                </>
              )}
            </>
          )}
        </Routes>
      </div>
      <Footer />
    </Router>
  );
}

export default App;
