/**
 * bullmq-outbox against a real Redis and a real Postgres.
 *
 * The Redis is broken the way production breaks it: maxmemory dropped below
 * current usage, so every write is rejected with a genuine OOM error. No
 * mocks, no fake queues — the jobs really fail to enqueue, really land in
 * Postgres, and really come back.
 */
import assert from 'node:assert/strict';
import { after, before, beforeEach, describe, it } from 'node:test';
import { Queue, Worker } from 'bullmq';
import type { Pool } from 'pg';
import type Redis from 'ioredis';
import { createOutbox } from '../src/index';
import { createPostgresOutboxStore } from './postgres-store';
import { REDIS, admin, breakRedis, freshDb, flushAll, healRedis, until } from './helpers';

let pool: Pool;
let control: Redis;

before(async () => {
  control = admin();
  pool = await freshDb();
});

beforeEach(async () => {
  await healRedis(control);
  await flushAll(control);
  await pool.query('truncate bullmq_outbox');
});

after(async () => {
  await healRedis(control);
  await control.quit();
  await pool.end();
});

const pending = async () =>
  (await pool.query("select * from bullmq_outbox where status = 'pending' order by created_at"))
    .rows;

describe('outbox against real Redis', () => {
  it('survives a real OOM and replays the job when Redis recovers', async () => {
    const outbox = createOutbox({ store: createPostgresOutboxStore(pool) });
    const queue = outbox.wrapQueue(
      new Queue('emails', { connection: { ...REDIS, maxRetriesPerRequest: null } }),
    );

    // Healthy: the job goes straight through.
    await queue.add('welcome', { userId: 1 });
    assert.equal((await pending()).length, 0);

    // Redis fills up.
    await breakRedis(control);
    await assert.rejects(() => queue.add('welcome', { userId: 2 }), /OOM/);

    const stored = await pending();
    assert.equal(stored.length, 1, 'the rejected job is in Postgres');
    assert.equal(stored[0].queue_name, 'emails');
    assert.deepEqual(stored[0].data, { userId: 2 });
    assert.match(stored[0].last_error, /OOM/);

    // Redis recovers, the drain runs.
    await healRedis(control);
    const result = await outbox.flush();

    assert.equal(result.requeued, 1);
    assert.equal((await pending()).length, 0, 'no longer pending');

    // And a real worker actually receives it.
    const seen: number[] = [];
    const worker = new Worker(
      'emails',
      async (job) => {
        seen.push(job.data.userId);
      },
      { connection: { ...REDIS, maxRetriesPerRequest: null } },
    );
    await until(() => seen.includes(2));
    await worker.close();
    await queue.close();
  });

  it('does not multiply entries when the drain itself fails', async () => {
    // The regression that a fake queue hid: replaying through an instrumented
    // queue used to store a NEW entry on every failed drain — 1, 2, 4, 8.
    const outbox = createOutbox({ store: createPostgresOutboxStore(pool) });
    const queue = outbox.wrapQueue(
      new Queue('emails', { connection: { ...REDIS, maxRetriesPerRequest: null } }),
    );

    await breakRedis(control);
    await assert.rejects(() => queue.add('welcome', { userId: 1 }));
    assert.equal((await pending()).length, 1);

    // Still down. Three drains that cannot succeed.
    await outbox.flush();
    await outbox.flush();
    await outbox.flush();

    const stored = await pending();
    assert.equal(stored.length, 1, 'one job, one row');
    assert.equal(stored[0].attempts, 3, 'attempts counted instead');

    await healRedis(control);
    await queue.close();
  });

  it('covers every queue when the class is substituted (the NestJS path)', async () => {
    const outbox = createOutbox({ store: createPostgresOutboxStore(pool) });
    const QueueWithOutbox = outbox.queueClass(Queue);

    // Built exactly as @nestjs/bullmq builds them: new Q(name, options).
    const invoices = new QueueWithOutbox('invoices', {
      connection: { ...REDIS, maxRetriesPerRequest: null },
    });
    const emails = new QueueWithOutbox('emails', {
      connection: { ...REDIS, maxRetriesPerRequest: null },
    });

    assert.ok(invoices instanceof Queue, 'still a real BullMQ Queue');

    await breakRedis(control);
    await assert.rejects(() => invoices.add('issue', { orderId: 1 }));
    await assert.rejects(() => emails.add('receipt', { orderId: 1 }));

    assert.equal((await pending()).length, 2, 'no wrapQueue call anywhere');

    await healRedis(control);
    const result = await outbox.flush();
    assert.equal(result.requeued, 2, 'self-registered, so flush found them');

    await invoices.close();
    await emails.close();
  });

  it('replays job options, including a custom jobId', async () => {
    const outbox = createOutbox({ store: createPostgresOutboxStore(pool) });
    const queue = outbox.wrapQueue(
      new Queue('emails', { connection: { ...REDIS, maxRetriesPerRequest: null } }),
    );

    await breakRedis(control);
    await assert.rejects(() =>
      queue.add('welcome', { userId: 3 }, { jobId: 'order-42', attempts: 7 }),
    );
    await healRedis(control);
    await outbox.flush();

    const job = await queue.getJob('order-42');
    assert.ok(job, 'the custom jobId survived the round trip');
    assert.equal(job.opts.attempts, 7);
    await queue.close();
  });

  it('expires a job that never gets back in, and stops retrying it', async () => {
    const expired: string[] = [];
    const outbox = createOutbox({
      store: createPostgresOutboxStore(pool),
      maxAttempts: 2,
      onJobExpired: (event) => expired.push(event.jobName),
    });
    const queue = outbox.wrapQueue(
      new Queue('emails', { connection: { ...REDIS, maxRetriesPerRequest: null } }),
    );

    await breakRedis(control);
    await assert.rejects(() => queue.add('doomed', {}));

    await outbox.flush();
    await outbox.flush();

    assert.deepEqual(expired, ['doomed']);
    assert.equal((await pending()).length, 0, 'out of the drain rotation');

    const dead = await pool.query("select * from bullmq_outbox where status = 'expired'");
    assert.equal(dead.rows.length, 1, 'kept for inspection, not deleted');

    await healRedis(control);
    await queue.close();
  });

  it('leaves queues owned by other services alone', async () => {
    const outbox = createOutbox({ store: createPostgresOutboxStore(pool) });
    const queue = outbox.wrapQueue(
      new Queue('emails', { connection: { ...REDIS, maxRetriesPerRequest: null } }),
    );

    await breakRedis(control);
    await assert.rejects(() => queue.add('welcome', {}));
    await healRedis(control);

    // A second service, sharing the table, owning a different queue.
    const otherService = createOutbox({ store: createPostgresOutboxStore(pool) });
    const result = await otherService.flush();

    assert.equal(result.skipped, 1, 'not mine, left for the owner');
    assert.equal((await pending()).length, 1, 'still there');

    assert.equal((await outbox.flush()).requeued, 1, 'the owner drains it');
    await queue.close();
  });
});
