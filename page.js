'use client';

import { useState } from 'react';

export default function Home() {
  const [status, setStatus] = useState(null);
  const [loading, setLoading] = useState(false);

  const checkStatus = async () => {
    const res = await fetch('/api/send-replies');
    setStatus(await res.json());
  };

  const triggerBatch = async () => {
    setLoading(true);
    const res = await fetch('/api/send-replies', { method: 'POST' });
    setStatus(await res.json());
    setLoading(false);
  };

  const reset = async () => {
    if (!confirm('Reset offset to 0?')) return;
    const res = await fetch('/api/send-replies', { method: 'DELETE' });
    setStatus(await res.json());
  };

  return (
    <div style={{ fontFamily: 'monospace', padding: 40, maxWidth: 500 }}>
      <h1>📨 Reply Sender</h1>

      <div style={{ display: 'flex', gap: 10, marginBottom: 20 }}>
        <button onClick={checkStatus}>Check Status</button>
        <button onClick={triggerBatch} disabled={loading}>
          {loading ? 'Sending...' : 'Send Batch'}
        </button>
        <button onClick={reset} style={{ color: 'red' }}>
          Reset
        </button>
      </div>

      {status && (
        <pre style={{ background: '#f0f0f0', padding: 16, borderRadius: 8 }}>
          {JSON.stringify(status, null, 2)}
        </pre>
      )}
    </div>
  );
}
