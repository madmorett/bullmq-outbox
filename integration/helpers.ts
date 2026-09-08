import { Pool } from 'pg';
import Redis from 'ioredis';

export const REDIS = { host: '127.0.0.1', port: 6398 };
export const PG = 'postgres://outbox:outbox@127.0.0.1:54329/outbox';

/** The schema straight out of bullmq-outbox/examples/postgres-store.ts. */
export const SCHEMA = `
  create table if not exists bullmq_outbox (
    id           text primary key,
    queue_name   text        not null,
    job_name     text        not null,
    data         jsonb       not null,
    opts         jsonb,
    created_at   timestamptz not null,
    attempts     int         not null default 0,
    last_error   text,
    status       text        not null default 'pending'
  );
  create index if not exists bullmq_outbox_pending
    on bullmq_outbox (created_at) where status = 'pending';
`;

export async function freshDb(): Promise<Pool> {
  const pool = new Pool({ connectionString: PG, max: 4 });
  await pool.query(SCHEMA);
  await pool.query('truncate bullmq_outbox');
  return pool;
}

export function admin(): Redis {
  return new Redis({ ...REDIS, maxRetriesPerRequest: null });
}

/**
 * Force a real OOM.
 *
 * `maxmemory` is set just above what Redis is already using, so every
 * subsequent write is rejected with "OOM command not allowed when used memory
 * > 'maxmemory'" — the exact error an ElastiCache node throws when it fills
 * up, and the one this package exists to survive.
 *
 * Setting it to something tiny like 1 byte does NOT work: Redis cannot hold
 * its own structures and the server dies instead of rejecting writes, which
 * tests reconnection rather than OOM.
 */
export async function breakRedis(client: Redis): Promise<void> {
  const info = await client.info('memory');
  const used = Number(/used_memory:(\d+)/.exec(info)?.[1] ?? 0);
  await client.config('SET', 'maxmemory', String(used + 8 * 1024));

  // Confirm the server really is rejecting writes before the test proceeds,
  // rather than assuming the config took effect.
  await until(async () => {
    try {
      await client.set('__oom_probe__', 'x'.repeat(64 * 1024));
      return false;
    } catch (error) {
      return /OOM/.test(String(error));
    }
  }, 5000);
}

export async function healRedis(client: Redis): Promise<void> {
  await client.config('SET', 'maxmemory', '0');
}

export async function flushAll(client: Redis): Promise<void> {
  await client.flushall();
}

/** Wait until `check` passes, so tests never depend on a fixed sleep. */
export async function until(
  check: () => Promise<boolean> | boolean,
  timeoutMs = 10_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`condition not met within ${timeoutMs}ms`);
}
