import React, { useEffect, useState } from 'react';
import { getEwasteData, getPredictions } from '../services/api';
import Chart from '../components/Chart';
import MapComponent from '../components/Map';
import { BarChart3, TrendingUp, MapPin, Loader2, UploadCloud } from 'lucide-react';
import { Link } from 'react-router-dom';

const Dashboard = () => {
    const [data, setData] = useState([]);
    const [forecasts, setForecasts] = useState([]);
    const [loading, setLoading] = useState(true);
    
    useEffect(() => {
        const fetchData = async () => {
            try {
                setLoading(true);
                const [resData, resForecasts] = await Promise.all([
                    getEwasteData(),
                    getPredictions()
                ]);
                setData((resData.data.data || []).map(d => ({
                    ...d,
                    sales_import_tonnes: parseFloat(d.sales_import_tonnes) || 0,
                    disposal_amount_tonnes: parseFloat(d.disposal_amount_tonnes) || 0,
                    year: parseInt(d.year, 10) || 0
                })));
                setForecasts(resForecasts.data.data || []);
            } catch (error) {
                console.error("Error fetching data", error);
            } finally {
                setLoading(false);
            }
        };
        fetchData();
    }, []);

    const totalSales = data.reduce((sum, d) => sum + (parseFloat(d.sales_import_tonnes) || 0), 0);
    const totalDisposal = data.reduce((sum, d) => sum + (parseFloat(d.disposal_amount_tonnes) || 0), 0);
    const totalPredicted = forecasts.reduce((sum, d) => sum + (parseFloat(d.predicted_tonnes) || 0), 0);
    const uniqueRegions = [...new Set(data.map(d => d.region))].length;
    const avgYear = data.length > 0 ? Math.round(data.reduce((sum, d) => sum + (parseFloat(d.year) || 0), 0) / data.length) : 'N/A';

    return (
        <div>
            <div className="page-header fade-in">
                <h2>E-Waste Overview</h2>
                <p>Monitor and analyze electronic waste disposal trends across regions</p>
            </div>

            <div className="row g-3 mb-4">
                <div className="col-6 col-lg-3">
                    <div className="kpi-card fade-in fade-in-delay-1">
                        <div className="kpi-icon green">
                            <BarChart3 size={24} />
                        </div>
                        <div>
                            <div className="kpi-value">{totalSales.toLocaleString()}</div>
                            <div className="kpi-label">Total Tonnes</div>
                        </div>
                    </div>
                </div>
                <div className="col-6 col-lg-3">
                    <div className="kpi-card fade-in fade-in-delay-2">
                        <div className="kpi-icon purple">
                            <TrendingUp size={24} />
                        </div>
                        <div>
                            <div className="kpi-value">{totalDisposal.toLocaleString()}</div>
                            <div className="kpi-label">Disposal</div>
                        </div>
                    </div>
                </div>
                <div className="col-6 col-lg-3">
                    <div className="kpi-card fade-in fade-in-delay-3">
                        <div className="kpi-icon amber">
                            <MapPin size={24} />
                        </div>
                        <div>
                            <div className="kpi-value">{uniqueRegions}</div>
                            <div className="kpi-label">Regions</div>
                        </div>
                    </div>
                </div>
                <div className="col-6 col-lg-3">
                    <div className="kpi-card fade-in fade-in-delay-4">
                        <div className="kpi-icon amber-shadow">
                            <TrendingUp size={24} />
                        </div>
                        <div>
                            <div className="kpi-value">{totalPredicted.toLocaleString()}</div>
                            <div className="kpi-label">Forecasted</div>
                        </div>
                    </div>
                </div>
            </div>

            {loading ? (
                <div className="spinner-container">
                    <Loader2 className="spinner" size={40} />
                    <p className="mt-3 mb-0">Loading dashboard data...</p>
                </div>
            ) : data.length > 0 ? (
                <div className="row g-4">
                    <div className="col-lg-6">
                        <div className="card p-3">
                            <div className="d-flex align-items-center justify-content-between mb-3">
                                <h4 className="card-title mb-0" style={{ fontSize: '1.1rem', fontWeight: 700 }}>Historical Data</h4>
                                <span className="badge-custom badge-green">{data.length} records</span>
                            </div>
                            <Chart data={data} xKey="year" yKeys={['sales_import_tonnes', 'disposal_amount_tonnes']} />
                        </div>
                    </div>
                    <div className="col-lg-6">
                        <div className="card p-3" style={{ minHeight: '400px' }}>
                            <div className="d-flex align-items-center justify-content-between mb-3">
                                <h4 className="card-title mb-0" style={{ fontSize: '1.1rem', fontWeight: 700 }}>Disposal Clusters</h4>
                                <span className="badge-custom badge-purple">{uniqueRegions} locations</span>
                            </div>
                            <MapComponent />
                        </div>
                    </div>
                </div>
            ) : (
                <div className="card p-5 text-center">
                    <div className="empty-state">
                        <UploadCloud className="empty-state-icon" />
                        <h5 style={{ color: '#1e293b', fontWeight: 600, marginBottom: '0.5rem' }}>No Data Available</h5>
                        <p className="text-muted mb-4">Upload your e-waste data to start analyzing trends and viewing insights.</p>
                        <Link to="/upload" className="btn-primary-custom" style={{ textDecoration: 'none', display: 'inline-flex', alignItems: 'center', gap: '0.5rem' }}>
                            <UploadCloud size={18} />
                            Upload Data
                        </Link>
                    </div>
                </div>
            )}
        </div>
    );
};

export default Dashboard;
