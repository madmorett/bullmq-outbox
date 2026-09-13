/**
 * bullmq-outbox against a real Redis and a real MongoDB, using the store from
 * examples/mongodb-store.ts verbatim.
 *
 * Same six scenarios the Postgres suite covers, so the two examples are held
 * to one standard.
 */
import assert from 'node:assert/strict';
import { after, before, beforeEach, describe, it } from 'node:test';
import { Queue, Worker } from 'bullmq';
import type { Db, MongoClient } from 'mongodb';
import type Redis from 'ioredis';
import { createOutbox } from '../src/index';
import { createMongoOutboxStore } from './mongodb-store';
import { REDIS, admin, breakRedis, flushAll, freshMongo, healRedis, until } from './helpers';

const connection = { ...REDIS, maxRetriesPerRequest: null };

let control: Redis;
let client: MongoClient;
let db: Db;

before(async () => {
  control = admin();
  ({ client, db } = await freshMongo());
});

beforeEach(async () => {
  await healRedis(control);
  await flushAll(control);
  await db.collection('bullmq_outbox').deleteMany({});
});

after(async () => {
  await healRedis(control);
  await control.quit();
  await client.close();
});

const pending = () =>
  db.collection('bullmq_outbox').find({ status: 'pending' }).sort({ createdAt: 1 }).toArray();

describe('outbox on MongoDB, against real Redis', () => {
  it('survives a real OOM and replays the job when Redis recovers', async () => {
    const outbox = createOutbox({ store: createMongoOutboxStore(db) });
    const queue = outbox.wrapQueue(new Queue('emails', { connection }));

    await queue.add('welcome', { userId: 1 });
    assert.equal((await pending()).length, 0, 'healthy adds go straight through');

    await breakRedis(control);
    await assert.rejects(() => queue.add('welcome', { userId: 2 }), /OOM/);

    const stored = await pending();
    assert.equal(stored.length, 1);
    assert.equal(stored[0]!.queueName, 'emails');
    assert.deepEqual(stored[0]!.data, { userId: 2 });
    assert.match(stored[0]!.lastError, /OOM/);
    assert.ok(stored[0]!.createdAt instanceof Date, 'stored as a real Date');

    await healRedis(control);
    assert.equal((await outbox.flush()).requeued, 1);
    assert.equal((await pending()).length, 0);

    // A real worker actually receives it.
    const seen: number[] = [];
    const worker = new Worker(
      'emails',
      async (job) => {
        seen.push(job.data.userId);
      },
      { connection },
    );
    await until(() => seen.includes(2));
    await worker.close();
    await queue.close();
  });

  it('does not multiply documents when the drain itself fails', async () => {
    const outbox = createOutbox({ store: createMongoOutboxStore(db) });
    const queue = outbox.wrapQueue(new Queue('emails', { connection }));

    await breakRedis(control);
    await assert.rejects(() => queue.add('welcome', { userId: 1 }));

    await outbox.flush();
    await outbox.flush();
    await outbox.flush();

    const stored = await pending();
    assert.equal(stored.length, 1, 'one job, one document');
    assert.equal(stored[0]!.attempts, 3, 'attempts counted instead');

    await healRedis(control);
    await queue.close();
  });

  it('covers every queue when the class is substituted (the NestJS path)', async () => {
    const outbox = createOutbox({ store: createMongoOutboxStore(db) });
    const QueueWithOutbox = outbox.queueClass(Queue);
    const invoices = new QueueWithOutbox('invoices', { connection });
    const emails = new QueueWithOutbox('emails', { connection });

    await breakRedis(control);
    await assert.rejects(() => invoices.add('issue', { orderId: 1 }));
    await assert.rejects(() => emails.add('receipt', { orderId: 1 }));
    assert.equal((await pending()).length, 2);

    await healRedis(control);
    assert.equal((await outbox.flush()).requeued, 2);

    await invoices.close();
    await emails.close();
  });

  it('replays job options, including a custom jobId', async () => {
    const outbox = createOutbox({ store: createMongoOutboxStore(db) });
    const queue = outbox.wrapQueue(new Queue('emails', { connection }));

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

  it('expires a job that never gets back in, and keeps it for inspection', async () => {
    const expired: string[] = [];
    const outbox = createOutbox({
      store: createMongoOutboxStore(db),
      maxAttempts: 2,
      onJobExpired: (event) => expired.push(event.jobName),
    });
    const queue = outbox.wrapQueue(new Queue('emails', { connection }));

    await breakRedis(control);
    await assert.rejects(() => queue.add('doomed', {}));

    await outbox.flush();
    await outbox.flush();

    assert.deepEqual(expired, ['doomed']);
    assert.equal((await pending()).length, 0, 'out of the drain rotation');

    const dead = await db.collection('bullmq_outbox').find({ status: 'expired' }).toArray();
    assert.equal(dead.length, 1, 'kept, not deleted');
    assert.ok(dead[0]!.resolvedAt instanceof Date, 'resolvedAt drives the TTL index');

    await healRedis(control);
    await queue.close();
  });

  it('leaves queues owned by other services alone', async () => {
    const outbox = createOutbox({ store: createMongoOutboxStore(db) });
    const queue = outbox.wrapQueue(new Queue('emails', { connection }));

    await breakRedis(control);
    await assert.rejects(() => queue.add('welcome', {}));
    await healRedis(control);

    const otherService = createOutbox({ store: createMongoOutboxStore(db) });
    assert.equal((await otherService.flush()).skipped, 1, 'not mine');
    assert.equal((await pending()).length, 1, 'still there');
    assert.equal((await outbox.flush()).requeued, 1, 'the owner drains it');

    await queue.close();
  });

  it('never lets the TTL index delete a job that is still pending', async () => {
    // The subtle one: a TTL on createdAt would reap entries during a long
    // outage, before they are ever replayed. resolvedAt is only set once an
    // entry leaves the rotation, so pending documents are never candidates.
    const outbox = createOutbox({ store: createMongoOutboxStore(db) });
    const rawQueue = new Queue('emails', { connection });
    const queue = outbox.wrapQueue(rawQueue);

    await breakRedis(control);
    await assert.rejects(() => queue.add('welcome', {}));
    await healRedis(control);

    const stored = await pending();
    assert.equal(stored[0]!.resolvedAt, undefined, 'pending has no resolvedAt');

    await outbox.flush();
    const processed = await db
      .collection('bullmq_outbox')
      .findOne({ status: 'processed' });
    assert.ok(processed?.resolvedAt instanceof Date, 'set only once resolved');

    await queue.close();
    await rawQueue.close();
  });
});
