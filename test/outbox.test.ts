import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { MemoryOutboxStore } from '../src/memory-store';
import { createOutbox } from '../src/outbox';
import type { MinimalQueue, OutboxStore } from '../src/types';

/** A queue that fails until told otherwise, standing in for a dead Redis. */
function fakeQueue(name: string) {
  const added: { name: string; data: unknown; opts?: unknown }[] = [];
  let failure: Error | null = null;

  const queue: MinimalQueue & {
    added: typeof added;
    breakWith(error: Error): void;
    heal(): void;
  } = {
    name,
    added,
    breakWith(error: Error) {
      failure = error;
    },
    heal() {
      failure = null;
    },
    async add(jobName, data, opts) {
      if (failure) throw failure;
      added.push({ name: jobName, data, opts });
      return { id: String(added.length) };
    },
    async addBulk(jobs) {
      if (failure) throw failure;
      added.push(...jobs);
      return jobs.map((_, index) => ({ id: String(index) }));
    },
  };

  return queue;
}

describe('Outbox.wrapQueue', () => {
  it('passes jobs straight through while Redis is healthy', async () => {
    const store = new MemoryOutboxStore();
    const queue = fakeQueue('emails');
    const wrapped = createOutbox({ store }).wrapQueue(queue);

    await wrapped.add('welcome', { userId: 1 });

    assert.deepEqual(queue.added, [
      { name: 'welcome', data: { userId: 1 }, opts: undefined },
    ]);
    assert.equal(store.pending().length, 0);
  });

  it('stores the job and re-throws when the enqueue fails', async () => {
    const store = new MemoryOutboxStore();
    const queue = fakeQueue('emails');
    const wrapped = createOutbox({ store }).wrapQueue(queue);
    queue.breakWith(new Error('OOM command not allowed'));

    await assert.rejects(
      () => wrapped.add('welcome', { userId: 1 }, { attempts: 3 }),
      /OOM command not allowed/,
    );

    const [entry] = store.pending();
    assert.equal(entry?.queueName, 'emails');
    assert.equal(entry?.jobName, 'welcome');
    assert.deepEqual(entry?.data, { userId: 1 });
    assert.deepEqual(entry?.opts, { attempts: 3 });
    assert.equal(entry?.lastError, 'OOM command not allowed');
  });

  it('drops the flow parent from stored opts so the replay is accepted', async () => {
    const store = new MemoryOutboxStore();
    const queue = fakeQueue('emails');
    const wrapped = createOutbox({ store }).wrapQueue(queue);
    queue.breakWith(new Error('down'));

    await assert.rejects(() =>
      wrapped.add(
        'welcome',
        {},
        { attempts: 2, parent: { id: 'p1', queue: 'parent' } },
      ),
    );

    assert.deepEqual(store.pending()[0]?.opts, { attempts: 2 });
  });

  it('stores one entry per job of a failed bulk', async () => {
    const store = new MemoryOutboxStore();
    const queue = fakeQueue('emails');
    const wrapped = createOutbox({ store }).wrapQueue(queue);
    queue.breakWith(new Error('down'));

    await assert.rejects(() =>
      wrapped.addBulk([
        { name: 'a', data: { i: 1 } },
        { name: 'b', data: { i: 2 } },
      ]),
    );

    assert.equal(store.pending().length, 2);
  });

  it('honours shouldCapture for real-time queues', async () => {
    const store = new MemoryOutboxStore();
    const queue = fakeQueue('live-chat');
    const wrapped = createOutbox({
      store,
      shouldCapture: ({ queueName }) => !queueName.startsWith('live-'),
    }).wrapQueue(queue);
    queue.breakWith(new Error('down'));

    await assert.rejects(() => wrapped.add('turn', {}));

    assert.equal(store.pending().length, 0);
  });

  it('forwards untouched methods to the real queue', async () => {
    const store = new MemoryOutboxStore();
    const queue = Object.assign(fakeQueue('emails'), {
      async pause() {
        return 'paused';
      },
    });
    const wrapped = createOutbox({ store }).wrapQueue(queue);

    assert.equal(wrapped.name, 'emails');
    assert.equal(await wrapped.pause(), 'paused');
  });

  it('never lets a broken store swallow the original error', async () => {
    const explodingStore: OutboxStore = {
      async save() {
        throw new Error('dynamo is down too');
      },
      async loadPending() {
        return [];
      },
      async markProcessed() {},
      async markFailed() {},
    };
    const saveFailures: string[] = [];
    const queue = fakeQueue('emails');
    const wrapped = createOutbox({
      store: explodingStore,
      onSaveFailed: (event) => saveFailures.push(event.storeError),
    }).wrapQueue(queue);
    queue.breakWith(new Error('OOM'));

    await assert.rejects(() => wrapped.add('welcome', {}), /OOM/);

    assert.deepEqual(saveFailures, ['dynamo is down too']);
  });
});

describe('Outbox.flush', () => {
  it('re-enqueues stored jobs once Redis recovers', async () => {
    const store = new MemoryOutboxStore();
    const queue = fakeQueue('emails');
    const outbox = createOutbox({ store });
    const wrapped = outbox.wrapQueue(queue);

    queue.breakWith(new Error('OOM'));
    await assert.rejects(() => wrapped.add('welcome', { userId: 7 }));
    queue.heal();

    const result = await outbox.flush();

    assert.equal(result.requeued, 1);
    assert.equal(store.pending().length, 0);
    assert.deepEqual(queue.added, [
      { name: 'welcome', data: { userId: 7 }, opts: undefined },
    ]);
  });

  it('skips queues this process does not own', async () => {
    const store = new MemoryOutboxStore();
    await store.save({
      id: 'x1',
      queueName: 'owned-by-another-service',
      jobName: 'job',
      data: {},
      createdAt: new Date().toISOString(),
      attempts: 0,
    });

    const result = await createOutbox({ store }).flush();

    assert.equal(result.skipped, 1);
    assert.equal(store.pending().length, 1, 'the owner still gets its turn');
  });

  it('counts attempts and expires a job that never recovers', async () => {
    const store = new MemoryOutboxStore();
    const queue = fakeQueue('emails');
    const expirations: string[] = [];
    const outbox = createOutbox({
      store,
      maxAttempts: 2,
      onJobExpired: (event) => expirations.push(event.jobName),
    });
    const wrapped = outbox.wrapQueue(queue);

    queue.breakWith(new Error('OOM'));
    await assert.rejects(() => wrapped.add('welcome', {}));

    const first = await outbox.flush();
    assert.equal(first.failed, 1);
    assert.equal(store.pending()[0]?.attempts, 1);

    const second = await outbox.flush();
    assert.equal(second.expired, 1);
    assert.equal(store.pending().length, 0);
    assert.equal(store.expired().length, 1);
    assert.deepEqual(expirations, ['welcome']);
  });

  it('reports what it did through hooks', async () => {
    const store = new MemoryOutboxStore();
    const queue = fakeQueue('emails');
    const requeued: number[] = [];
    const outbox = createOutbox({
      store,
      onJobRequeued: (event) => requeued.push(event.ageMs),
    });
    const wrapped = outbox.wrapQueue(queue);

    queue.breakWith(new Error('OOM'));
    await assert.rejects(() => wrapped.add('welcome', {}));
    queue.heal();
    await outbox.flush();

    assert.equal(requeued.length, 1);
    assert.ok(requeued[0]! >= 0);
  });

  it('drains queues registered without wrapping', async () => {
    const store = new MemoryOutboxStore();
    const queue = fakeQueue('emails');
    const outbox = createOutbox({ store });
    outbox.registerQueue(queue);

    await store.save({
      id: 'x1',
      queueName: 'emails',
      jobName: 'welcome',
      data: { userId: 3 },
      createdAt: new Date().toISOString(),
      attempts: 0,
    });

    const result = await outbox.flush();

    assert.equal(result.requeued, 1);
    assert.equal(queue.added.length, 1);
  });
});
