import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { LogIn, User, Building, Settings, Shield } from 'lucide-react';

const Login = ({ onLogin }) => {
  const navigate = useNavigate();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [selectedRole, setSelectedRole] = useState('admin');
  const [isRegister, setIsRegister] = useState(false);

  const roles = [
    { id: 'admin', name: 'Admin', icon: <Shield size={40} className="mb-2" />, desc: 'Government / Authority' },
    { id: 'hub', name: 'Hub', icon: <Building size={40} className="mb-2" />, desc: 'Collection & Inventory' },
    { id: 'customer', name: 'Customer', icon: <User size={40} className="mb-2" />, desc: 'Public User' },
    { id: 'service', name: 'Service', icon: <Settings size={40} className="mb-2" />, desc: 'Repair & Recycling' }
  ];

  const handleLogin = (e) => {
    e.preventDefault();
    setError('');

    // Hardcoded credentials for demonstration
    const validCredentials = {
      admin: { u: 'admin', p: 'admin123', path: '/' },
      hub: { u: 'hub', p: 'hub123', path: '/hub' },
      customer: { u: 'customer', p: 'customer123', path: '/customer' },
      service: { u: 'service', p: 'service123', path: '/service' }
    };

    if (isRegister && selectedRole !== 'admin') {
      if (!username || !password) {
        setError('Please enter username and password to register.');
        return;
      }
      onLogin({ role: selectedRole, username });
      const pathMap = {
        hub: '/hub',
        customer: '/customer',
        service: '/service'
      };
      navigate(pathMap[selectedRole]);
      return;
    }

    const creds = validCredentials[selectedRole];
    if (username === creds.u && password === creds.p) {
      onLogin({ role: selectedRole, username });
      navigate(creds.path);
    } else {
      setError('Invalid username or password. Please use the default credentials.');
    }
  };

  return (
    <div className="container mt-5">
      <div className="text-center mb-5">
        <h1 className="fw-bold">Welcome to E-Waste Platform</h1>
        <p className="text-muted">Select your module to sign in</p>
      </div>

      <div className="row justify-content-center mb-4">
        {roles.map(role => (
          <div key={role.id} className="col-6 col-md-3 mb-3">
            <div 
              className={`card text-center p-3 cursor-pointer transition ${selectedRole === role.id ? 'border-primary bg-primary text-white' : 'border-secondary'}`}
              onClick={() => { setSelectedRole(role.id); setError(''); }}
              style={{ cursor: 'pointer', transition: '0.3s' }}
            >
              <div className="card-body p-0">
                {role.icon}
                <h5 className="card-title fw-bold">{role.name}</h5>
                <small className={selectedRole === role.id ? 'text-white-50' : 'text-muted'}>{role.desc}</small>
              </div>
            </div>
          </div>
        ))}
      </div>

      <div className="row justify-content-center">
        <div className="col-md-6 col-lg-5">
          <div className="card shadow-sm border-0">
            <div className="card-body p-4">
              <h3 className="card-title text-center mb-4">
                {roles.find(r => r.id === selectedRole).name} {isRegister && selectedRole !== 'admin' ? 'Register' : 'Login'}
              </h3>
              
              {error && <div className="alert alert-danger">{error}</div>}

              <form onSubmit={handleLogin}>
                <div className="mb-3">
                  <label className="form-label">Username</label>
                  <input 
                    type="text" 
                    className="form-control" 
                    value={username}
                    onChange={(e) => setUsername(e.target.value)}
                    placeholder={`Enter ${selectedRole} username`}
                    required
                  />
                  <div className="form-text">Hint: {selectedRole}</div>
                </div>
                
                <div className="mb-4">
                  <label className="form-label">Password</label>
                  <input 
                    type="password" 
                    className="form-control" 
                    value={password}
                    onChange={(e) => setPassword(e.target.value)}
                    placeholder="Enter password"
                    required
                  />
                  <div className="form-text">Hint: {selectedRole}123</div>
                </div>

                <div className="d-grid gap-2">
                  <button type="submit" className="btn btn-primary btn-lg">
                    <LogIn size={20} className="me-2" />
                    {isRegister && selectedRole !== 'admin' ? 'Sign Up' : 'Sign In'}
                  </button>
                </div>
                {selectedRole !== 'admin' && (
                  <div className="text-center mt-3">
                    <button 
                      type="button" 
                      className="btn btn-link text-decoration-none" 
                      onClick={() => setIsRegister(!isRegister)}
                    >
                      {isRegister ? 'Already have an account? Sign In' : 'New user? Create an account'}
                    </button>
                  </div>
                )}
              </form>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Login;
