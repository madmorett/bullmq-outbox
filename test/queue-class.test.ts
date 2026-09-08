import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { MemoryOutboxStore } from '../src/memory-store';
import { createOutbox } from '../src/outbox';

/**
 * Stands in for `Queue` from bullmq: constructed with (name, options), which
 * is exactly how NestJS builds it via `BullModule.queueClass`.
 */
class FakeQueue {
  readonly name: string;
  readonly options: unknown;
  readonly added: { name: string; data: unknown; opts?: unknown }[] = [];
  failure: Error | null = null;

  constructor(name: string, options?: unknown) {
    this.name = name;
    this.options = options;
  }

  async add(name: string, data: unknown, opts?: unknown): Promise<unknown> {
    if (this.failure) throw this.failure;
    this.added.push({ name, data, opts });
    return { id: String(this.added.length) };
  }

  async addBulk(
    jobs: { name: string; data: unknown; opts?: unknown }[],
  ): Promise<unknown> {
    if (this.failure) throw this.failure;
    this.added.push(...jobs);
    return jobs.map((_, index) => ({ id: String(index) }));
  }

  async pause(): Promise<string> {
    return 'paused';
  }
}

describe('Outbox.queueClass', () => {
  it('builds a queue that behaves like the base class', async () => {
    const store = new MemoryOutboxStore();
    const OutboxQueue = createOutbox({ store }).queueClass(FakeQueue);

    const queue = new OutboxQueue('emails', { connection: {} }) as FakeQueue;

    assert.equal(queue.name, 'emails');
    assert.deepEqual(queue.options, { connection: {} });
    await queue.add('welcome', { userId: 1 });
    assert.equal(queue.added.length, 1);
    assert.equal(store.pending().length, 0);
  });

  it('is a real subclass, so instanceof and other methods survive', async () => {
    const store = new MemoryOutboxStore();
    const OutboxQueue = createOutbox({ store }).queueClass(FakeQueue);

    const queue = new OutboxQueue('emails') as FakeQueue;

    assert.ok(queue instanceof FakeQueue, 'frameworks check instanceof');
    assert.equal(await queue.pause(), 'paused');
  });

  it('captures a failed add without any wrapQueue call', async () => {
    const store = new MemoryOutboxStore();
    const OutboxQueue = createOutbox({ store }).queueClass(FakeQueue);
    const queue = new OutboxQueue('emails') as FakeQueue;
    queue.failure = new Error('OOM command not allowed');

    await assert.rejects(() => queue.add('welcome', { userId: 1 }), /OOM/);

    const [entry] = store.pending();
    assert.equal(entry?.queueName, 'emails');
    assert.equal(entry?.jobName, 'welcome');
  });

  it('captures every job of a failed bulk', async () => {
    const store = new MemoryOutboxStore();
    const OutboxQueue = createOutbox({ store }).queueClass(FakeQueue);
    const queue = new OutboxQueue('emails') as FakeQueue;
    queue.failure = new Error('down');

    await assert.rejects(() =>
      queue.addBulk([
        { name: 'a', data: {} },
        { name: 'b', data: {} },
      ]),
    );

    assert.equal(store.pending().length, 2);
  });

  it('self-registers so flush finds the queue with no extra wiring', async () => {
    const store = new MemoryOutboxStore();
    const outbox = createOutbox({ store });
    const OutboxQueue = outbox.queueClass(FakeQueue);
    const queue = new OutboxQueue('emails') as FakeQueue;

    queue.failure = new Error('OOM');
    await assert.rejects(() => queue.add('welcome', { userId: 9 }));
    queue.failure = null;

    const result = await outbox.flush();

    assert.equal(result.requeued, 1, 'no registerQueue() was ever called');
    assert.deepEqual(queue.added[0]?.data, { userId: 9 });
  });

  it('covers every queue the framework builds, not just remembered ones', async () => {
    const store = new MemoryOutboxStore();
    const outbox = createOutbox({ store });
    const OutboxQueue = outbox.queueClass(FakeQueue);

    // Three modules registering three queues, as a Nest app would.
    const queues = ['emails', 'invoices', 'analytics'].map(
      (name) => new OutboxQueue(name) as FakeQueue,
    );
    for (const queue of queues) queue.failure = new Error('OOM');

    for (const queue of queues) {
      await assert.rejects(() => queue.add('job', { q: queue.name }));
    }

    assert.equal(store.pending().length, 3);
  });

  it('honours shouldCapture', async () => {
    const store = new MemoryOutboxStore();
    const OutboxQueue = createOutbox({
      store,
      shouldCapture: ({ queueName }) => !queueName.startsWith('live-'),
    }).queueClass(FakeQueue);
    const queue = new OutboxQueue('live-chat') as FakeQueue;
    queue.failure = new Error('down');

    await assert.rejects(() => queue.add('turn', {}));

    assert.equal(store.pending().length, 0);
  });
});
