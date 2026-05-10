import React from 'react';

export default function TopTable({ title, data, labelKey, countKey = 'count' }) {
  return (
    <div style={{ background: 'white', border: '1.5px solid #e2e8f0', borderRadius: 10, padding: '1.5rem', flex: 1, minWidth: 0 }}>
      <h3 style={{ margin: '0 0 1rem', fontWeight: 600, fontSize: '1rem', color: '#0f172a' }}>{title}</h3>
      {!data || data.length === 0 ? (
        <p style={{ color: '#94a3b8', fontSize: '0.875rem' }}>No data yet</p>
      ) : (
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <tbody>
            {data.map((row, i) => (
              <tr key={i} style={{ borderBottom: i < data.length - 1 ? '1px solid #f1f5f9' : 'none' }}>
                <td style={{
                  padding: '0.6rem 0',
                  color: '#334155',
                  fontSize: '0.85rem',
                  maxWidth: 220,
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }}>
                  {row[labelKey] || <span style={{ color: '#94a3b8' }}>Direct / unknown</span>}
                </td>
                <td style={{ padding: '0.6rem 0', textAlign: 'right', fontWeight: 600, color: '#6366f1', fontSize: '0.875rem' }}>
                  {row[countKey].toLocaleString()}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
