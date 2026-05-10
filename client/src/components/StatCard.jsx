import React from 'react';

export default function StatCard({ label, value, accent }) {
  return (
    <div style={{
      background: 'white',
      border: '1.5px solid #e2e8f0',
      borderRadius: 10,
      padding: '1.5rem',
      flex: 1,
      minWidth: 0,
    }}>
      <p style={{ margin: '0 0 0.4rem', color: '#64748b', fontSize: '0.8rem', textTransform: 'uppercase', letterSpacing: '0.05em', fontWeight: 600 }}>
        {label}
      </p>
      <p style={{ margin: 0, fontSize: '2rem', fontWeight: 700, color: accent || '#0f172a' }}>
        {value?.toLocaleString() ?? '—'}
      </p>
    </div>
  );
}
