import React from 'react';
import { Recycle } from 'lucide-react';

const Footer = () => {
  return (
    <footer className="footer">
      <div className="container">
        <div className="row align-items-center">
          <div className="col-md-6">
            <div className="d-flex align-items-center gap-2 mb-2 mb-md-0">
              <Recycle size={20} style={{ color: '#10b981' }} />
              <span style={{ fontWeight: 600, color: '#1e293b' }}>E-Waste Forecaster</span>
              <span className="text-muted" style={{ fontSize: '0.85rem' }}>AI-powered e-waste quantification & forecasting</span>
            </div>
          </div>
          <div className="col-md-6 text-md-end">
            <span className="text-muted" style={{ fontSize: '0.85rem' }}>
              Built for Kumaraguru Hackathon 2026
            </span>
          </div>
        </div>
      </div>
    </footer>
  );
};

export default Footer;
