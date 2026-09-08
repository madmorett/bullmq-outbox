import { randomUUID } from 'node:crypto';
import type {
  FlushResult,
  MinimalQueue,
  OutboxEntry,
  OutboxHooks,
  OutboxOptions,
  OutboxStore,
} from './types';

const DEFAULT_MAX_ATTEMPTS = 10;

/** A hook must never take a job down with it. */
function safely(run: () => void): void {
  try {
    run();
  } catch {
    // observability is best-effort by construction
  }
}

/**
 * Job options are persisted as JSON, so anything that cannot survive a round
 * trip is dropped rather than silently corrupting the replay.
 *
 * `parent` is the notable one: a flow parent references a job that may no
 * longer exist by the time the outbox drains, and BullMQ would reject the
 * whole re-enqueue. A flow child is better replayed standalone than not at all.
 */
function serializableOpts(opts: unknown): unknown {
  if (opts === undefined || opts === null) return undefined;
  if (typeof opts !== 'object') return opts;
  const { parent: _parent, ...rest } = opts as Record<string, unknown>;
  return rest;
}

export class Outbox {
  private readonly store: OutboxStore;
  private readonly maxAttempts: number;
  private readonly hooks: OutboxHooks;
  private readonly generateId: () => string;
  private readonly shouldCapture: NonNullable<OutboxOptions['shouldCapture']>;

  /** Queues registered via `wrapQueue`, so `flush()` can find them again. */
  private readonly queues = new Map<string, MinimalQueue>();

  constructor(options: OutboxOptions) {
    this.store = options.store;
    this.maxAttempts = options.maxAttempts ?? DEFAULT_MAX_ATTEMPTS;
    this.generateId = options.generateId ?? (() => randomUUID());
    this.shouldCapture = options.shouldCapture ?? (() => true);
    this.hooks = {
      onJobSaved: options.onJobSaved,
      onJobRequeued: options.onJobRequeued,
      onJobExpired: options.onJobExpired,
      onSaveFailed: options.onSaveFailed,
      onFlush: options.onFlush,
    };
  }

  /**
   * Return a queue that writes to the outbox when Redis rejects an enqueue.
   *
   * This is a Proxy, not a subclass: the package never imports `bullmq`, so
   * the same wrapper works with v5, v6 and BullMQ Pro, and every method it
   * does not override (`getJob`, `pause`, `upsertJobScheduler`, Pro's group
   * APIs) passes straight through to the real queue.
   *
   * The original error is always re-thrown. The outbox buys you a replay, not
   * a lie — the caller still learns the enqueue failed and decides what to
   * tell the user.
   */
  wrapQueue<Q extends MinimalQueue>(queue: Q): Q {
    this.queues.set(queue.name, queue);

    const outbox = this;

    return new Proxy(queue, {
      get(target, property, receiver) {
        if (property === 'add') {
          return function add(name: string, data: unknown, opts?: unknown) {
            return Promise.resolve(target.add(name, data, opts)).catch(
              async (error: Error) => {
                await outbox.capture(target.name, name, data, opts, error);
                throw error;
              },
            );
          };
        }

        if (property === 'addBulk') {
          return function addBulk(
            jobs: { name: string; data: unknown; opts?: unknown }[],
          ) {
            return Promise.resolve(target.addBulk(jobs)).catch(
              async (error: Error) => {
                // One entry per job: a partial bulk failure is indistinguishable
                // from a total one at this layer, and a job replayed twice is
                // cheaper than a job lost. Give jobs a `jobId` if the replay
                // needs to dedupe.
                for (const job of jobs) {
                  await outbox.capture(
                    target.name,
                    job.name,
                    job.data,
                    job.opts,
                    error,
                  );
                }
                throw error;
              },
            );
          };
        }

        const value = Reflect.get(target, property, receiver);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    }) as Q;
  }

  /**
   * Register a queue that was not created through `wrapQueue` — for instance
   * when the process draining the outbox is not the one that enqueues.
   */
  registerQueue(queue: MinimalQueue): void {
    this.queues.set(queue.name, queue);
  }

  /** Persist a failed enqueue by hand. `wrapQueue` calls this for you. */
  async capture(
    queueName: string,
    jobName: string,
    data: unknown,
    opts: unknown,
    error: Error,
  ): Promise<void> {
    if (!this.shouldCapture({ queueName, jobName, error })) return;

    const entry: OutboxEntry = {
      id: this.generateId(),
      queueName,
      jobName,
      data,
      opts: serializableOpts(opts),
      createdAt: new Date().toISOString(),
      attempts: 0,
      lastError: error.message,
    };

    try {
      await this.store.save(entry);
      safely(() =>
        this.hooks.onJobSaved?.({
          queueName,
          jobName,
          id: entry.id,
          error: error.message,
        }),
      );
    } catch (storeError) {
      // The fallback's fallback. Swallowed on purpose: the caller is already
      // getting the Redis error, and throwing here would replace a meaningful
      // message with a confusing one.
      safely(() =>
        this.hooks.onSaveFailed?.({
          queueName,
          jobName,
          originalError: error.message,
          storeError:
            storeError instanceof Error
              ? storeError.message
              : String(storeError),
        }),
      );
    }
  }

  /**
   * Re-enqueue pending jobs. Call it from a cron, a repeatable job or a plain
   * `setInterval`.
   *
   * Run the scheduler on infrastructure that does not share the failure you
   * are recovering from. A drain loop living in the Redis that just died
   * never fires.
   */
  async flush(limit = 50): Promise<FlushResult> {
    const pending = await this.store.loadPending(limit);

    const result: FlushResult = {
      requeued: 0,
      failed: 0,
      skipped: 0,
      expired: 0,
      details: [],
    };

    for (const entry of pending) {
      const detail = await this.replay(entry);
      result.details.push(detail);
      if (detail.status === 'requeued') result.requeued++;
      else if (detail.status === 'skipped') result.skipped++;
      else if (detail.status === 'expired') result.expired++;
      else result.failed++;
    }

    safely(() =>
      this.hooks.onFlush?.({
        requeued: result.requeued,
        failed: result.failed,
        skipped: result.skipped,
        expired: result.expired,
      }),
    );

    return result;
  }

  private async replay(
    entry: OutboxEntry,
  ): Promise<FlushResult['details'][number]> {
    const queue = this.queues.get(entry.queueName);

    // Not an error: with several services against one store, each drains the
    // queues it owns and leaves the rest to whoever registered them.
    if (!queue) {
      return {
        id: entry.id,
        queueName: entry.queueName,
        jobName: entry.jobName,
        status: 'skipped',
      };
    }

    try {
      await queue.add(entry.jobName, entry.data, entry.opts);
      await this.store.markProcessed(entry.id);

      safely(() =>
        this.hooks.onJobRequeued?.({
          queueName: entry.queueName,
          jobName: entry.jobName,
          id: entry.id,
          ageMs: Date.now() - new Date(entry.createdAt).getTime(),
          attempts: entry.attempts,
        }),
      );

      return {
        id: entry.id,
        queueName: entry.queueName,
        jobName: entry.jobName,
        status: 'requeued',
      };
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const attempts = entry.attempts + 1;
      const expired = attempts >= this.maxAttempts;

      await this.store.markFailed(entry.id, message, attempts, expired);

      if (expired) {
        safely(() =>
          this.hooks.onJobExpired?.({
            queueName: entry.queueName,
            jobName: entry.jobName,
            id: entry.id,
            attempts,
            lastError: message,
          }),
        );
      }

      return {
        id: entry.id,
        queueName: entry.queueName,
        jobName: entry.jobName,
        status: expired ? 'expired' : 'failed',
        error: message,
      };
    }
  }
}

export function createOutbox(options: OutboxOptions): Outbox {
  return new Outbox(options);
}
