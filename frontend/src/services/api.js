import axios from 'axios';

const api = axios.create({
    baseURL: import.meta.env.VITE_API_BASE_URL || 'https://e-waste-backend-xbqk.onrender.com/api',
});

export const uploadData = (formData) => api.post('/upload', formData);
export const getEwasteData = (params) => api.get('/data', { params });
export const getMapData = () => api.get('/map-data');
export const getHeatmap = () => api.get('/map/heatmap');
export const getClusters = (method = 'dbscan') => api.get('/map/clusters', { params: { method } });
export const predictEwaste = (data) => api.post('/predict', data);
export const predictTimeseries = (data) => api.post('/predict/timeseries', data);
export const getPredictions = (params) => api.get('/predictions', { params });
export const getMlHealth = () => api.get('/ml/health');

export default api;
