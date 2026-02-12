import { kv } from '@vercel/kv';

const WEBHOOK_URL = 'https://n8n.outboundhero.co/webhook/untracked';
const BATCH_SIZE = 25;
const DELAY_MS = 10000;
const CHUNK_SIZE = 2000; // must match split.js

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

async function loadReplies(offset) {
  const chunkIndex = Math.floor(offset / CHUNK_SIZE);
  const padded = String(chunkIndex).padStart(3, '0');
  const chunk = (await import(`@/data/chunks/chunk_${padded}.json`)).default;
  const startInChunk = offset % CHUNK_SIZE;
  return { replies: chunk.slice(startInChunk, startInChunk + BATCH_SIZE), chunkTotal: chunk.length };
}

async function getTotalReplies() {
  let total = await kv.get('total');
  if (total) return total;

  // Count once by checking all chunks
  let count = 0;
  let i = 0;
  while (true) {
    try {
      const padded = String(i).padStart(3, '0');
      const chunk = (await import(`@/data/chunks/chunk_${padded}.json`)).default;
      count += chunk.length;
      i++;
    } catch {
      break;
    }
  }
  await kv.set('total', count);
  return count;
}

export async function POST() {
  const offset = (await kv.get('offset')) || 0;
  const failed = (await kv.get('failed')) || [];
  const total = await getTotalReplies();

  // All done — retry failed ones
  if (offset >= total) {
    if (failed.length === 0) {
      return Response.json({ done: true, total });
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

  // Load only the chunk we need
  const { replies } = await loadReplies(offset);
  const newFailed = [];

  for (let i = 0; i < replies.length; i++) {
    const ok = await send(replies[i]);
    if (!ok) newFailed.push(replies[i]);
    console.log(`${ok ? '✅' : '❌'} [${offset + i + 1}/${total}]`);
    if (i < replies.length - 1) await sleep(DELAY_MS);
  }

  const newOffset = offset + replies.length;
  await kv.set('offset', newOffset);
  if (newFailed.length) await kv.set('failed', [...failed, ...newFailed]);

  return Response.json({
    progress: `${Math.min(newOffset, total)}/${total}`,
    failed: failed.length + newFailed.length,
  });
}

export async function DELETE() {
  await kv.set('offset', 0);
  await kv.set('failed', []);
  await kv.set('total', null);
  return Response.json({ reset: true });
}

export async function GET() {
  const offset = (await kv.get('offset')) || 0;
  const failed = (await kv.get('failed')) || [];
  const total = await getTotalReplies();

  return Response.json({
    offset,
    total,
    failed: failed.length,
    done: offset >= total && failed.length === 0,
  });
}
