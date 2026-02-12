import { kv } from '@vercel/kv';
import fs from 'fs';
import path from 'path';

const WEBHOOK_URL = 'https://n8n.outboundhero.co/webhook/untracked';
const BATCH_SIZE = 25;
const DELAY_MS = 10000;
const CHUNK_SIZE = 2000;

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

function loadChunk(chunkIndex) {
  const padded = String(chunkIndex).padStart(3, '0');
  const filePath = path.join(process.cwd(), 'data', 'chunks', `chunk_${padded}.json`);
  const raw = fs.readFileSync(filePath, 'utf8');
  return JSON.parse(raw);
}

function getTotalFromDisk() {
  const chunksDir = path.join(process.cwd(), 'data', 'chunks');
  const files = fs.readdirSync(chunksDir).filter(f => f.endsWith('.json')).sort();
  let total = 0;
  for (const file of files) {
    const raw = fs.readFileSync(path.join(chunksDir, file), 'utf8');
    total += JSON.parse(raw).length;
  }
  return total;
}

async function getTotalReplies() {
  let total = await kv.get('total');
  if (total) return total;
  total = getTotalFromDisk();
  await kv.set('total', total);
  return total;
}

export async function POST() {
  const offset = (await kv.get('offset')) || 0;
  const failed = (await kv.get('failed')) || [];
  const total = await getTotalReplies();

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

  const chunkIndex = Math.floor(offset / CHUNK_SIZE);
  const startInChunk = offset % CHUNK_SIZE;
  const chunk = loadChunk(chunkIndex);
  const batch = chunk.slice(startInChunk, startInChunk + BATCH_SIZE);
  const newFailed = [];

  for (let i = 0; i < batch.length; i++) {
    const ok = await send(batch[i]);
    if (!ok) newFailed.push(batch[i]);
    console.log(`${ok ? '✅' : '❌'} [${offset + i + 1}/${total}]`);
    if (i < batch.length - 1) await sleep(DELAY_MS);
  }

  const newOffset = offset + batch.length;
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
