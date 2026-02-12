import { kv } from '@vercel/kv';

const WEBHOOK_URL = 'https://n8n.outboundhero.co/webhook/untracked';
const BATCH_SIZE = 25;
const DELAY_MS = 10000;

export const maxDuration = 300;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function send(reply) {
  try {
    const res = await fetch(WEBHOOK_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(reply),
    });
    return res.ok;
  } catch {
    return false;
  }
}

export async function POST() {
  const replies = (await import('@/data/replies.json')).default;
  const offset = (await kv.get('offset')) || 0;
  const failed = (await kv.get('failed')) || [];

  // All done — retry failed ones
  if (offset >= replies.length) {
    if (failed.length === 0) {
      return Response.json({ done: true, total: replies.length });
    }

    const retryBatch = failed.slice(0, BATCH_SIZE);
    const stillFailed = [];

    for (let i = 0; i < retryBatch.length; i++) {
      const ok = await send(retryBatch[i]);
      if (!ok) stillFailed.push(retryBatch[i]);
      if (i < retryBatch.length - 1) await sleep(DELAY_MS);
    }

    const remaining = [...stillFailed, ...failed.slice(BATCH_SIZE)];
    await kv.set('failed', remaining);

    return Response.json({ retrying: true, remaining: remaining.length });
  }

  // Normal batch
  const batch = replies.slice(offset, offset + BATCH_SIZE);
  const newFailed = [];

  for (let i = 0; i < batch.length; i++) {
    const ok = await send(batch[i]);
    if (!ok) newFailed.push(batch[i]);
    console.log(
      `${ok ? '✅' : '❌'} [${offset + i + 1}/${replies.length}]`
    );
    if (i < batch.length - 1) await sleep(DELAY_MS);
  }

  const newOffset = offset + BATCH_SIZE;
  await kv.set('offset', newOffset);
  if (newFailed.length) await kv.set('failed', [...failed, ...newFailed]);

  return Response.json({
    progress: `${Math.min(newOffset, replies.length)}/${replies.length}`,
    failed: failed.length + newFailed.length,
  });
}

// Reset everything
export async function DELETE() {
  await kv.set('offset', 0);
  await kv.set('failed', []);
  return Response.json({ reset: true });
}

// Check status
export async function GET() {
  const offset = (await kv.get('offset')) || 0;
  const failed = (await kv.get('failed')) || [];

  let total = 0;
  try {
    const replies = (await import('@/data/replies.json')).default;
    total = replies.length;
  } catch {
    total = 'unknown';
  }

  return Response.json({
    offset,
    total,
    failed: failed.length,
    done: offset >= total && failed.length === 0,
  });
}
