import React from 'react';
import { Routes, Route, Link, useLocation } from 'react-router-dom';
import Home from './pages/Home';
import Dashboard from './pages/Dashboard';
import LinkAnalytics from './pages/LinkAnalytics';

export default function App() {
  const location = useLocation();

  return (
    <div style={{ display: 'flex', minHeight: '100vh' }}>
      <nav style={{
        width: 220,
        background: '#0f172a',
        padding: '2rem 1rem',
        display: 'flex',
        flexDirection: 'column',
        gap: '0.5rem',
        flexShrink: 0,
      }}>
        <div style={{ color: '#6366f1', fontWeight: 700, fontSize: '1.4rem', marginBottom: '2rem', paddingLeft: '0.75rem' }}>
          ✂ Scissor
        </div>
        <NavLink to="/" label="Shorten" active={location.pathname === '/'} />
        <NavLink to="/dashboard" label="Dashboard" active={location.pathname === '/dashboard'} />
      </nav>

      <main style={{ flex: 1, padding: '2.5rem', overflowY: 'auto' }}>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/dashboard" element={<Dashboard />} />
          <Route path="/analytics/:shortCode" element={<LinkAnalytics />} />
        </Routes>
      </main>
    </div>
  );
}

function NavLink({ to, label, active }) {
  return (
    <Link
      to={to}
      style={{
        color: active ? '#6366f1' : '#94a3b8',
        textDecoration: 'none',
        padding: '0.6rem 0.75rem',
        borderRadius: 6,
        background: active ? 'rgba(99,102,241,0.12)' : 'transparent',
        fontWeight: active ? 600 : 400,
        fontSize: '0.95rem',
        transition: 'color 0.15s',
      }}
    >
      {label}
    </Link>
  );
}
