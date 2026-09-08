import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { MemoryOutboxStore } from '../src/memory-store';
import { createOutbox } from '../src/outbox';
import type { MinimalQueue } from '../src/types';

class FakeQueue {
  readonly name: string;
  readonly added: { name: string; data: unknown; opts?: unknown }[] = [];
  failure: Error | null = null;

  constructor(name: string) {
    this.name = name;
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
    return jobs.map(() => ({}));
  }
}

/**
 * The replay goes through the same `add` the application calls. On an
 * instrumented queue that is the one that captures failures, so a drain that
 * fails must NOT store a second entry for the job it is already replaying —
 * otherwise every drain during an outage doubles the backlog: 1, 2, 4, 8.
 */
describe('a failed drain does not duplicate entries', () => {
  it('holds at one entry through repeated failed drains (queueClass)', async () => {
    const store = new MemoryOutboxStore();
    const outbox = createOutbox({ store });
    const QueueWithOutbox = outbox.queueClass(FakeQueue);
    const queue = new QueueWithOutbox('emails') as FakeQueue;

    queue.failure = new Error('OOM');
    await assert.rejects(() => queue.add('welcome', { userId: 1 }));
    assert.equal(store.pending().length, 1);

    // Redis is still down. Drain, fail, drain again.
    await outbox.flush();
    await outbox.flush();
    await outbox.flush();

    assert.equal(store.pending().length, 1, 'one job, one entry');
    assert.equal(store.pending()[0]?.attempts, 3, 'attempts counted, not entries');
  });

  it('holds at one entry through repeated failed drains (wrapQueue)', async () => {
    const store = new MemoryOutboxStore();
    const outbox = createOutbox({ store });
    const raw = new FakeQueue('emails');
    const wrapped = outbox.wrapQueue(raw);

    raw.failure = new Error('OOM');
    await assert.rejects(() => wrapped.add('welcome', { userId: 1 }));

    await outbox.flush();
    await outbox.flush();

    assert.equal(store.pending().length, 1);
    assert.equal(store.pending()[0]?.attempts, 2);
  });

  it('does not duplicate on a failed bulk replay either', async () => {
    const store = new MemoryOutboxStore();
    const outbox = createOutbox({ store });
    const QueueWithOutbox = outbox.queueClass(FakeQueue);
    const queue = new QueueWithOutbox('emails') as FakeQueue;

    queue.failure = new Error('OOM');
    await assert.rejects(() =>
      queue.addBulk([
        { name: 'a', data: {} },
        { name: 'b', data: {} },
      ]),
    );
    assert.equal(store.pending().length, 2);

    await outbox.flush();
    await outbox.flush();

    assert.equal(store.pending().length, 2, 'still two, not eight');
  });

  it('resumes capturing after the drain finishes', async () => {
    const store = new MemoryOutboxStore();
    const outbox = createOutbox({ store });
    const QueueWithOutbox = outbox.queueClass(FakeQueue);
    const queue = new QueueWithOutbox('emails') as FakeQueue;

    queue.failure = new Error('OOM');
    await assert.rejects(() => queue.add('first', {}));
    await outbox.flush();

    // A new application enqueue after the drain must still be captured.
    await assert.rejects(() => queue.add('second', {}));

    assert.equal(store.pending().length, 2);
    assert.deepEqual(
      store.pending().map((entry) => entry.jobName).sort(),
      ['first', 'second'],
    );
  });

  it('restores capturing when loadPending throws', async () => {
    const outbox = createOutbox({
      store: {
        async save() {},
        async loadPending() {
          throw new Error('store died');
        },
        async markProcessed() {},
        async markFailed() {},
      },
    });

    await assert.rejects(() => outbox.flush(), /store died/);

    // The flag must not be stuck on, or the outbox silently stops capturing
    // every job from here on — a far worse failure than the one it replaced.
    const captured: string[] = [];
    const outbox2 = createOutbox({
      store: {
        async save(entry) {
          captured.push(entry.jobName);
        },
        async loadPending() {
          return [];
        },
        async markProcessed() {},
        async markFailed() {},
      },
    });
    await outbox2.capture('emails', 'job', {}, undefined, new Error('OOM'));
    assert.deepEqual(captured, ['job']);
  });

  it('counts a delivered job as requeued even if the store write fails', async () => {
    // markProcessed throwing means the job IS in Redis. Reporting that as a
    // failed replay would burn an attempt and eventually expire a job that
    // actually succeeded — and page someone about it. It is a bookkeeping
    // problem, so it surfaces through onSaveFailed instead.
    const saveFailures: string[] = [];
    const entry = {
      id: 'x1',
      queueName: 'emails',
      jobName: 'welcome',
      data: {},
      createdAt: new Date().toISOString(),
      attempts: 0,
    };
    let markFailedCalls = 0;

    const outbox = createOutbox({
      store: {
        async save() {},
        async loadPending() {
          return [{ ...entry }];
        },
        async markProcessed() {
          throw new Error('store died');
        },
        async markFailed() {
          markFailedCalls++;
        },
      },
      onSaveFailed: (event) => saveFailures.push(event.storeError),
    });
    const queue = new FakeQueue('emails');
    outbox.registerQueue(queue);

    const result = await outbox.flush();

    assert.equal(result.requeued, 1, 'the job did reach Redis');
    assert.equal(result.failed, 0);
    assert.equal(markFailedCalls, 0, 'no attempt burned for a delivered job');
    assert.deepEqual(saveFailures, ['store died'], 'reported, not silent');
  });

  it('never captures a duplicate when the replay itself fails', async () => {
    // The core invariant: the drain re-enqueues through the raw queue, so a
    // failed replay can never re-enter capture. Asserted directly rather than
    // through entry counts, so a future refactor cannot regress it silently.
    const saved: string[] = [];
    const outbox = createOutbox({
      store: {
        async save(saveEntry) {
          saved.push(saveEntry.jobName);
        },
        async loadPending() {
          return [
            {
              id: 'x1',
              queueName: 'emails',
              jobName: 'welcome',
              data: {},
              createdAt: new Date().toISOString(),
              attempts: 0,
            },
          ];
        },
        async markProcessed() {},
        async markFailed() {},
      },
    });

    const QueueWithOutbox = outbox.queueClass(FakeQueue);
    const queue = new QueueWithOutbox('emails') as FakeQueue;
    queue.failure = new Error('still down');

    await outbox.flush();

    assert.deepEqual(saved, [], 'the replay path never calls save');
  });

  it('keeps capturing application jobs that fail during a drain', async () => {
    // A flag flipped for the duration of flush() would drop these — live
    // traffic failing mid-drain is exactly the traffic worth keeping.
    const saved: string[] = [];
    let releaseReplay: () => void = () => {};
    const replayStarted = new Promise<void>((resolve) => {
      releaseReplay = resolve;
    });

    const outbox = createOutbox({
      store: {
        async save(entry) {
          saved.push(entry.jobName);
        },
        async loadPending() {
          return [
            {
              id: 'x1',
              queueName: 'slow',
              jobName: 'old',
              data: {},
              createdAt: new Date().toISOString(),
              attempts: 0,
            },
          ];
        },
        async markProcessed() {},
        async markFailed() {},
      },
    });

    // A queue whose replay hangs until we let it go, so the drain is
    // provably still in flight when the application enqueue fails.
    let liveAddFailed: Promise<unknown> = Promise.resolve();
    outbox.registerQueue({
      name: 'slow',
      async add() {
        releaseReplay();
        await liveAddFailed;
        return {};
      },
      async addBulk() {
        return [];
      },
    });

    const QueueWithOutbox = outbox.queueClass(FakeQueue);
    const live = new QueueWithOutbox('emails') as FakeQueue;
    live.failure = new Error('OOM');

    const draining = outbox.flush();
    await replayStarted;

    // Mid-drain: a real request tries to enqueue and Redis rejects it.
    liveAddFailed = assert.rejects(() => live.add('welcome', { userId: 1 }));
    await liveAddFailed;
    await draining;

    assert.deepEqual(saved, ['welcome'], 'the live job was not dropped');
  });
});
