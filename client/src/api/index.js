import axios from 'axios';

const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL || 'http://localhost:4000',
});

export const shortenUrl = (data) => api.post('/api/shorten', data);
export const getAnalytics = (shortCode) => api.get(`/api/analytics/${shortCode}`);
export const getSummary = () => api.get('/api/analytics/summary');
