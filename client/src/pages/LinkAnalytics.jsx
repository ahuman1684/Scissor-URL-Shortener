import React, { useEffect, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import { getAnalytics } from '../api';
import ClicksChart from '../components/ClicksChart';
import TopTable from '../components/TopTable';

const BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:4000';

export default function LinkAnalytics() {
  const { shortCode } = useParams();
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  useEffect(() => {
    setLoading(true);
    setError('');
    getAnalytics(shortCode)
      .then(({ data }) => setData(data))
      .catch(() => setError('Failed to load analytics for this link'))
      .finally(() => setLoading(false));
  }, [shortCode]);

  if (loading) return <p style={{ color: '#64748b' }}>Loading…</p>;
  if (error) return <p style={{ color: '#ef4444' }}>{error}</p>;

  return (
    <div>
      <Link to="/" style={{ color: '#94a3b8', fontSize: '0.875rem', textDecoration: 'none', display: 'inline-block', marginBottom: '1.25rem' }}>
        ← Back
      </Link>

      <h1 style={{ fontSize: '1.5rem', fontWeight: 700, marginBottom: '0.25rem', color: '#6366f1' }}>
        {BASE_URL}/{shortCode}
      </h1>
      <p style={{
        color: '#64748b',
        fontSize: '0.875rem',
        marginBottom: '0.25rem',
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
        maxWidth: 640,
      }}>
        → {data.originalUrl}
      </p>
      <p style={{ color: '#94a3b8', fontSize: '0.8rem', marginBottom: '2rem' }}>
        Created {new Date(data.createdAt).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' })}
        {' · '}
        <strong style={{ color: '#6366f1' }}>{data.totalClicks.toLocaleString()}</strong> total clicks
      </p>

      <div style={{ marginBottom: '1.5rem' }}>
        <ClicksChart data={data.clicksLast7Days} type="line" title="Clicks — Last 30 Days" />
      </div>

      <div style={{ display: 'flex', gap: '1rem', flexWrap: 'wrap' }}>
        <TopTable title="Top Referrers" data={data.topReferrers} labelKey="referrer" />
        <TopTable title="Top Countries" data={data.topCountries} labelKey="country" />
      </div>
    </div>
  );
}
