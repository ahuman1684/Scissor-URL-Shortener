import React, { useState } from 'react';
import { shortenUrl } from '../api';

export default function ShortenForm({ onSuccess }) {
  const [url, setUrl] = useState('');
  const [customCode, setCustomCode] = useState('');
  const [expiresIn, setExpiresIn] = useState('');
  const [showOptions, setShowOptions] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);
    try {
      const payload = { originalUrl: url };
      if (customCode.trim()) payload.customCode = customCode.trim();
      if (expiresIn) payload.expiresIn = parseInt(expiresIn, 10);

      const { data } = await shortenUrl(payload);
      onSuccess(data);
      setUrl('');
      setCustomCode('');
      setExpiresIn('');
    } catch (err) {
      setError(err.response?.data?.error || 'Something went wrong');
    } finally {
      setLoading(false);
    }
  };

  return (
    <form onSubmit={handleSubmit} style={{ maxWidth: 640 }}>
      <div style={{ display: 'flex', gap: '0.5rem', marginBottom: '0.75rem' }}>
        <input
          type="url"
          value={url}
          onChange={(e) => setUrl(e.target.value)}
          placeholder="Paste a long URL here…"
          required
          style={inputStyle}
        />
        <button type="submit" disabled={loading} style={btnStyle}>
          {loading ? '…' : 'Shorten'}
        </button>
      </div>

      <button
        type="button"
        onClick={() => setShowOptions(!showOptions)}
        style={{ background: 'none', border: 'none', color: '#6366f1', fontSize: '0.875rem', cursor: 'pointer', padding: '0 0 0.75rem' }}
      >
        {showOptions ? '▲ Hide options' : '▼ Advanced options'}
      </button>

      {showOptions && (
        <div style={{ display: 'flex', gap: '0.75rem', marginBottom: '0.75rem' }}>
          <input
            type="text"
            value={customCode}
            onChange={(e) => setCustomCode(e.target.value)}
            placeholder="Custom alias (optional)"
            style={{ ...inputStyle, flex: 1 }}
          />
          <input
            type="number"
            value={expiresIn}
            onChange={(e) => setExpiresIn(e.target.value)}
            placeholder="Expires in days"
            min="1"
            style={{ ...inputStyle, width: 160 }}
          />
        </div>
      )}

      {error && (
        <p style={{ color: '#ef4444', fontSize: '0.875rem', marginTop: '0.5rem' }}>{error}</p>
      )}
    </form>
  );
}

const inputStyle = {
  flex: 1,
  padding: '0.7rem 1rem',
  border: '1.5px solid #e2e8f0',
  borderRadius: 8,
  fontSize: '1rem',
  background: 'white',
  color: '#0f172a',
};

const btnStyle = {
  padding: '0.7rem 1.5rem',
  background: '#6366f1',
  color: 'white',
  border: 'none',
  borderRadius: 8,
  fontSize: '1rem',
  fontWeight: 600,
  whiteSpace: 'nowrap',
  flexShrink: 0,
};
