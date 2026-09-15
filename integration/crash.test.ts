/**
 * What survives a crash?
 *
 * Every other test in this suite simulates failure — a closed connection, a
 * Redis at maxmemory. None of them kill the process. But the failure that
 * actually loses jobs in production is the one where the machine goes away
 * mid-drain: an OOM kill, a `docker stop` past its grace period, a spot
 * instance reclaimed.
 *
 * These tests SIGKILL a real drain process at a precise point and then check
 * the store and the queue. SIGKILL because it cannot be trapped — no cleanup
 * handler runs, which is exactly the point.
 */
import assert from 'node:assert/strict';
import { spawn, type ChildProcess } from 'node:child_process';
import { after, before, beforeEach, describe, it } from 'node:test';
import { Queue } from 'bullmq';
import type { Pool } from 'pg';
import type Redis from 'ioredis';
import { createOutbox } from '../src/index';
import { createPostgresOutboxStore } from './postgres-store';
import { REDIS, admin, flushAll, freshDb } from './helpers';

const connection = { ...REDIS, maxRetriesPerRequest: null };
const QUEUE = 'crash-recovery';

let pool: Pool;
let control: Redis;

before(async () => {
  control = admin();
  pool = await freshDb();
});

beforeEach(async () => {
  await flushAll(control);
  await pool.query('truncate bullmq_outbox');
});

after(async () => {
  await control.quit();
  await pool.end();
});

/**
 * Run the drain until it prints `untilLine`, then SIGKILL it.
 * Returns everything it printed before dying.
 */
function killDrainAt(killPoint: string, untilLine: string): Promise<string[]> {
  return new Promise((resolve, reject) => {
    const lines: string[] = [];
    let child: ChildProcess | undefined;

    const timer = setTimeout(() => {
      child?.kill('SIGKILL');
      reject(new Error(`worker never printed "${untilLine}"; saw: ${lines.join(' | ')}`));
    }, 30_000);

    child = spawn(
      'node',
      ['--import', 'tsx', 'crash-worker.ts', killPoint, QUEUE],
      { cwd: __dirname, stdio: ['ignore', 'pipe', 'pipe'] },
    );

    child.stdout?.on('data', (chunk: Buffer) => {
      for (const line of chunk.toString().split('\n').filter(Boolean)) {
        lines.push(line.trim());
        if (line.includes(untilLine)) {
          // no grace period, no cleanup handlers — a real crash
          child?.kill('SIGKILL');
          clearTimeout(timer);
          // let the kill land before the assertions read the store
          setTimeout(() => resolve(lines), 250);
        }
      }
    });

    child.stderr?.on('data', (chunk: Buffer) => {
      const text = chunk.toString();
      if (text.includes('WORKER-ERROR')) {
        clearTimeout(timer);
        child?.kill('SIGKILL');
        reject(new Error(text.slice(0, 400)));
      }
    });
  });
}

const pendingRows = async () =>
  (await pool.query("select * from bullmq_outbox where status = 'pending'")).rows;

describe('crash recovery', () => {
  it('a job killed between Redis and the store is replayed, never lost', async () => {
    // The dangerous window: the job reached Redis, the store still says
    // pending, and the process dies before it can record that.
    const store = createPostgresOutboxStore(pool);
    await store.save({
      id: 'crash-1',
      queueName: QUEUE,
      jobName: 'survive-me',
      data: { orderId: 42 },
      createdAt: new Date().toISOString(),
      attempts: 0,
    });

    const printed = await killDrainAt('after-add-before-mark', 'PAUSED after-add');

    assert.ok(
      printed.some((line) => line.startsWith('PAUSED after-add')),
      'the worker should have reached the gap before being killed',
    );
    assert.ok(
      !printed.some((line) => line.startsWith('MARKED')),
      'the store must NOT have recorded it — that is the window being tested',
    );

    // The entry is still pending, so the next drain picks it up. At-least-once:
    // the job may reach Redis twice, and that is the deliberate trade — a
    // duplicate is recoverable with an idempotent handler, a lost job is not.
    const pending = await pendingRows();
    assert.equal(pending.length, 1, 'entry survived the crash as pending');
    assert.equal(pending[0].id, 'crash-1');

    // Prove recovery actually happens rather than assuming it.
    const queue = new Queue(QUEUE, { connection });
    const outbox = createOutbox({ store: createPostgresOutboxStore(pool) });
    outbox.registerQueue(queue);

    const result = await outbox.flush();
    assert.equal(result.requeued, 1, 'the surviving entry was replayed');
    assert.equal((await pendingRows()).length, 0, 'and is now resolved');

    await queue.close();
  });

  it('a crash before anything reaches Redis leaves the entry untouched', async () => {
    const store = createPostgresOutboxStore(pool);
    await store.save({
      id: 'crash-2',
      queueName: QUEUE,
      jobName: 'never-started',
      data: { orderId: 7 },
      createdAt: new Date().toISOString(),
      attempts: 0,
    });

    await killDrainAt('before-add', 'PAUSED before-add');

    const pending = await pendingRows();
    assert.equal(pending.length, 1);
    // No attempt was consumed: the drain never got far enough to fail.
    assert.equal(pending[0].attempts, 0, 'a crash must not burn a retry');
    assert.equal(pending[0].last_error, null);
  });

  it('survives being killed repeatedly and still drains everything', async () => {
    // Three entries, and a process that dies on the first one every time.
    // Whatever the crash loop does, no entry may disappear.
    const store = createPostgresOutboxStore(pool);
    for (let index = 0; index < 3; index++) {
      await store.save({
        id: `crash-loop-${index}`,
        queueName: QUEUE,
        jobName: `job-${index}`,
        data: { index },
        createdAt: new Date(Date.now() + index).toISOString(),
        attempts: 0,
      });
    }

    for (let round = 0; round < 3; round++) {
      await killDrainAt('after-add-before-mark', 'PAUSED after-add');
    }

    // Some entries may have been marked processed by a round that got far
    // enough; none may have vanished.
    const all = (await pool.query('select id, status from bullmq_outbox')).rows;
    assert.equal(all.length, 3, 'no entry was lost across three crashes');

    const queue = new Queue(QUEUE, { connection });
    const outbox = createOutbox({ store: createPostgresOutboxStore(pool) });
    outbox.registerQueue(queue);
    await outbox.flush();

    assert.equal((await pendingRows()).length, 0, 'a clean drain finishes the job');
    await queue.close();
  });
});
