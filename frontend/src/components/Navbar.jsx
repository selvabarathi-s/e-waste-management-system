import React, { useState } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { BarChart3, Upload, TrendingUp, Menu, X, Zap, Shield, Lightbulb, Camera, LogOut, User, Building, Settings, Sun, Moon, Palette } from 'lucide-react';

const Navbar = ({ user, onLogout, theme, setTheme }) => {
  const location = useLocation();
  const [isOpen, setIsOpen] = useState(false);
  const isActive = (path) => location.pathname === path ? 'active' : '';

  return (
    <nav className="navbar navbar-expand-lg navbar-dark navbar-custom sticky-top">
      <div className="container">
        <Link className="navbar-brand" to="/">
          <BarChart3 size={24} />
          E-Waste Forecaster
        </Link>
        <button
          className="navbar-toggler border-0"
          type="button"
          onClick={() => setIsOpen(!isOpen)}
          aria-label="Toggle navigation"
        >
          {isOpen ? <X size={24} /> : <Menu size={24} />}
        </button>
        <div className={`collapse navbar-collapse ${isOpen ? 'show' : ''}`} id="navbarNav">
          <ul className="navbar-nav ms-auto align-items-center">
            {user && user.role === 'admin' && (
              <>
                <li className="nav-item">
                  <Link className={`nav-link ${isActive('/')}`} to="/" onClick={() => setIsOpen(false)}>
                    <BarChart3 size={16} className="me-1" />
                    Dashboard
                  </Link>
                </li>
                <li className="nav-item">
                  <Link className={`nav-link ${isActive('/forecast')}`} to="/forecast" onClick={() => setIsOpen(false)}>
                    <TrendingUp size={16} className="me-1" />
                    Predictions
                  </Link>
                </li>
                <li className="nav-item">
                  <Link className={`nav-link ${isActive('/simulation')}`} to="/simulation" onClick={() => setIsOpen(false)}>
                    <Zap size={16} className="me-1" />
                    Simulation
                  </Link>
                </li>
                <li className="nav-item">
                  <Link className={`nav-link ${isActive('/sustainability')}`} to="/sustainability" onClick={() => setIsOpen(false)}>
                    <Shield size={16} className="me-1" />
                    Sustainability
                  </Link>
                </li>
                <li className="nav-item">
                  <Link className={`nav-link ${isActive('/recommendations')}`} to="/recommendations" onClick={() => setIsOpen(false)}>
                    <Lightbulb size={16} className="me-1" />
                    Recommendations
                  </Link>
                </li>
                <li className="nav-item">
                  <Link className={`nav-link ${isActive('/cv')}`} to="/cv" onClick={() => setIsOpen(false)}>
                    <Camera size={16} className="me-1" />
                    CV Detection
                  </Link>
                </li>
                <li className="nav-item">
                  <Link className={`nav-link ${isActive('/upload')}`} to="/upload" onClick={() => setIsOpen(false)}>
                    <Upload size={16} className="me-1" />
                    Upload
                  </Link>
                </li>
              </>
            )}

            {user && user.role === 'customer' && (
              <>
                <li className="nav-item">
                    <Link className={`nav-link ${isActive('/customer')}`} to="/customer" onClick={() => setIsOpen(false)}>
                      <User size={16} className="me-1" /> Customer Area
                    </Link>
                </li>
                <li className="nav-item">
                  <Link className={`nav-link ${isActive('/cv')}`} to="/cv" onClick={() => setIsOpen(false)}>
                    <Camera size={16} className="me-1" />
                    CV Detection
                  </Link>
                </li>
              </>
            )}

            {user && user.role === 'hub' && (
              <li className="nav-item">
                  <Link className={`nav-link ${isActive('/hub')}`} to="/hub" onClick={() => setIsOpen(false)}>
                    <Building size={16} className="me-1" /> Hub Dashboard
                  </Link>
              </li>
            )}

            {user && user.role === 'service' && (
              <li className="nav-item">
                  <Link className={`nav-link ${isActive('/service')}`} to="/service" onClick={() => setIsOpen(false)}>
                    <Settings size={16} className="me-1" /> Service Panel
                  </Link>
              </li>
            )}

            {user && (
              <li className="nav-item ms-lg-3 mt-3 mt-lg-0 d-flex align-items-center gap-2">
                <button 
                  className="btn btn-outline-light btn-sm p-2 d-flex align-items-center" 
                  onClick={() => setTheme(theme === 'light' ? 'dark' : theme === 'dark' ? 'mix' : 'light')}
                  title={`Current theme: ${theme}`}
                >
                  {theme === 'light' && <Sun size={16} />}
                  {theme === 'dark' && <Moon size={16} />}
                  {theme === 'mix' && <Palette size={16} />}
                </button>
                <button className="btn btn-outline-light btn-sm d-flex align-items-center" onClick={() => { onLogout(); setIsOpen(false); }}>
                  <LogOut size={16} className="me-2" /> Logout ({user.username})
                </button>
              </li>
            )}
          </ul>
        </div>
      </div>
    </nav>
  );
};

export default Navbar;
