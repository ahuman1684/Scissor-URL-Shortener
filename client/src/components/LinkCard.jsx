import React, { useState } from 'react';
import { Link } from 'react-router-dom';

export default function LinkCard({ link }) {
  const [copied, setCopied] = useState(false);

  const copy = () => {
    navigator.clipboard.writeText(link.shortUrl);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div style={{
      background: 'white',
      border: '1.5px solid #e2e8f0',
      borderRadius: 10,
      padding: '1rem 1.25rem',
      display: 'flex',
      justifyContent: 'space-between',
      alignItems: 'center',
      gap: '1rem',
    }}>
      <div style={{ minWidth: 0 }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <a
            href={link.shortUrl}
            target="_blank"
            rel="noreferrer"
            style={{ color: '#6366f1', fontWeight: 600, fontSize: '1rem', textDecoration: 'none' }}
          >
            {link.shortUrl}
          </a>
          <Link
            to={`/analytics/${link.shortCode}`}
            style={{ color: '#94a3b8', fontSize: '0.75rem', textDecoration: 'none', flexShrink: 0 }}
          >
            Analytics →
          </Link>
        </div>
        <p style={{
          margin: '0.2rem 0 0',
          color: '#64748b',
          fontSize: '0.85rem',
          overflow: 'hidden',
          textOverflow: 'ellipsis',
          whiteSpace: 'nowrap',
        }}>
          {link.originalUrl}
        </p>
        {link.expiresAt && (
          <p style={{ margin: '0.2rem 0 0', color: '#f59e0b', fontSize: '0.75rem' }}>
            Expires {new Date(link.expiresAt).toLocaleDateString()}
          </p>
        )}
      </div>

      <button
        onClick={copy}
        style={{
          padding: '0.45rem 1rem',
          background: copied ? '#10b981' : '#6366f1',
          color: 'white',
          border: 'none',
          borderRadius: 6,
          fontWeight: 500,
          fontSize: '0.875rem',
          flexShrink: 0,
          transition: 'background 0.2s',
        }}
      >
        {copied ? 'Copied!' : 'Copy'}
      </button>
    </div>
  );
}
