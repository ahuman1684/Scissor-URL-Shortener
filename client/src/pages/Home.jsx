import React, { useState, useEffect } from 'react';
import ShortenForm from '../components/ShortenForm';
import LinkCard from '../components/LinkCard';

const STORAGE_KEY = 'scissor_recent_links';

export default function Home() {
  const [recentLinks, setRecentLinks] = useState([]);

  useEffect(() => {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) setRecentLinks(JSON.parse(stored));
    } catch {
      // ignore parse errors
    }
  }, []);

  const handleSuccess = (link) => {
    const updated = [link, ...recentLinks.filter((l) => l.shortCode !== link.shortCode)].slice(0, 10);
    setRecentLinks(updated);
    localStorage.setItem(STORAGE_KEY, JSON.stringify(updated));
  };

  return (
    <div>
      <h1 style={{ fontSize: '1.75rem', fontWeight: 700, marginBottom: '0.4rem' }}>Shorten a URL</h1>
      <p style={{ color: '#64748b', marginBottom: '2rem' }}>
        Paste any long URL to get a short, shareable link with click analytics.
      </p>

      <ShortenForm onSuccess={handleSuccess} />

      {recentLinks.length > 0 && (
        <div style={{ marginTop: '3rem' }}>
          <h2 style={{ fontSize: '1.1rem', fontWeight: 600, marginBottom: '1rem', color: '#334155' }}>
            Your recent links
          </h2>
          <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
            {recentLinks.map((link) => (
              <LinkCard key={link.shortCode} link={link} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
