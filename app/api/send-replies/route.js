import { kv } from '@vercel/kv';
import fs from 'fs';
import path from 'path';

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

function getChunkFiles() {
  const chunksDir = path.join(process.cwd(), 'data', 'chunks');
  return fs.readdirSync(chunksDir).filter(f => f.endsWith('.json')).sort();
}

function loadChunk(filename) {
  const filePath = path.join(process.cwd(), 'data', 'chunks', filename);
  return JSON.parse(fs.readFileSync(filePath, 'utf8'));
}

export async function POST() {
  let chunkIndex = (await kv.get('chunkIndex')) || 0;
  let pos = (await kv.get('pos')) || 0;
  let sent = (await kv.get('sent')) || 0;
  const failed = (await kv.get('failed')) || [];
  const chunkFiles = getChunkFiles();

  if (chunkIndex >= chunkFiles.length) {
    if (failed.length === 0) {
      return Response.json({ done: true, sent });
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

  const chunk = loadChunk(chunkFiles[chunkIndex]);
  const batch = chunk.slice(pos, pos + BATCH_SIZE);
  const newFailed = [];

  for (let i = 0; i < batch.length; i++) {
    const ok = await send(batch[i]);
    if (!ok) newFailed.push(batch[i]);
    console.log(`${ok ? '✅' : '❌'} chunk=${chunkIndex} pos=${pos + i + 1}/${chunk.length}`);
    if (i < batch.length - 1) await sleep(DELAY_MS);
  }

  const newPos = pos + batch.length;
  const newSent = sent + batch.length;

  if (newPos >= chunk.length) {
    await kv.set('chunkIndex', chunkIndex + 1);
    await kv.set('pos', 0);
  } else {
    await kv.set('pos', newPos);
  }

  await kv.set('sent', newSent);
  if (newFailed.length) await kv.set('failed', [...failed, ...newFailed]);

  return Response.json({
    chunk: `${chunkIndex + 1}/${chunkFiles.length} (${chunkFiles[chunkIndex]})`,
    progress: `${newPos}/${chunk.length} in current chunk`,
    totalSent: newSent,
    failed: failed.length + newFailed.length,
  });
}

export async function DELETE() {
  await kv.set('chunkIndex', 0);
  await kv.set('pos', 0);
  await kv.set('sent', 0);
  await kv.set('failed', []);
  return Response.json({ reset: true });
}

export async function GET() {
  const chunkIndex = (await kv.get('chunkIndex')) || 0;
  const pos = (await kv.get('pos')) || 0;
  const sent = (await kv.get('sent')) || 0;
  const failed = (await kv.get('failed')) || [];
  const chunkFiles = getChunkFiles();

  return Response.json({
    chunkIndex,
    totalChunks: chunkFiles.length,
    posInChunk: pos,
    totalSent: sent,
    failed: failed.length,
    done: chunkIndex >= chunkFiles.length && failed.length === 0,
  });
}
