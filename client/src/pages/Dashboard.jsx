import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import { getSummary } from '../api';
import StatCard from '../components/StatCard';
import ClicksChart from '../components/ClicksChart';

export default function Dashboard() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    getSummary()
      .then(({ data }) => setData(data))
      .catch(() => setError('Failed to load dashboard data'))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <p style={{ color: '#64748b' }}>Loading…</p>;
  if (error) return <p style={{ color: '#ef4444' }}>{error}</p>;

  const clicksToday = data.clicksLast7Days.at(-1)?.clicks ?? 0;

  return (
    <div>
      <h1 style={{ fontSize: '1.75rem', fontWeight: 700, marginBottom: '1.5rem' }}>Dashboard</h1>

      <div style={{ display: 'flex', gap: '1rem', marginBottom: '1.5rem', flexWrap: 'wrap' }}>
        <StatCard label="Total Links" value={data.totalLinks} />
        <StatCard label="Total Clicks" value={data.totalClicks} accent="#6366f1" />
        <StatCard label="Clicks Today" value={clicksToday} />
      </div>

      <div style={{ marginBottom: '1.5rem' }}>
        <ClicksChart data={data.clicksLast7Days} type="bar" title="Clicks — Last 14 Days" />
      </div>

      <h2 style={{ fontSize: '1.1rem', fontWeight: 600, marginBottom: '1rem', color: '#334155' }}>Top Links</h2>
      <div style={{ background: 'white', border: '1.5px solid #e2e8f0', borderRadius: 10, overflow: 'hidden' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr style={{ background: '#f8fafc' }}>
              <th style={thStyle}>Short Code</th>
              <th style={thStyle}>Original URL</th>
              <th style={{ ...thStyle, textAlign: 'right' }}>Clicks</th>
            </tr>
          </thead>
          <tbody>
            {data.topLinks.length === 0 ? (
              <tr>
                <td colSpan={3} style={{ ...tdStyle, textAlign: 'center', color: '#94a3b8' }}>
                  No links yet — shorten one on the home page
                </td>
              </tr>
            ) : (
              data.topLinks.map((link) => (
                <tr key={link.shortCode} style={{ borderTop: '1px solid #f1f5f9' }}>
                  <td style={tdStyle}>
                    <Link to={`/analytics/${link.shortCode}`} style={{ color: '#6366f1', textDecoration: 'none', fontWeight: 600 }}>
                      {link.shortCode}
                    </Link>
                  </td>
                  <td style={{ ...tdStyle, color: '#64748b', maxWidth: 340, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {link.originalUrl}
                  </td>
                  <td style={{ ...tdStyle, textAlign: 'right', fontWeight: 600 }}>
                    {link.clicks.toLocaleString()}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

const thStyle = {
  padding: '0.75rem 1rem',
  textAlign: 'left',
  fontSize: '0.8rem',
  color: '#64748b',
  fontWeight: 600,
  textTransform: 'uppercase',
  letterSpacing: '0.04em',
};

const tdStyle = {
  padding: '0.85rem 1rem',
  fontSize: '0.875rem',
  color: '#334155',
};
