import { randomUUID } from 'node:crypto';
import type {
  FlushResult,
  MinimalQueue,
  OutboxEntry,
  OutboxHooks,
  OutboxOptions,
  OutboxStore,
  QueueConstructor,
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

/** Rejections are not guaranteed to be Errors. */
function messageOf(error: unknown): string {
  if (error instanceof Error) return error.message;
  if (typeof error === 'string') return error;
  return String(error);
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

/**
 * Snapshot the payload so a later mutation cannot change what gets replayed,
 * and surface an unserializable payload at capture time — where the caller
 * still has the real error for context — instead of inside the user's store.
 */
function snapshot(data: unknown): unknown {
  if (data === undefined) return undefined;
  return JSON.parse(JSON.stringify(data));
}

/**
 * How the replay reaches Redis.
 *
 * The application's `add` is instrumented: it captures failures. The replay
 * must NOT go through it, or a drain that fails would store a second entry
 * for the job it is currently replaying, and every subsequent drain would
 * double the backlog again — 1, 2, 4, 8.
 *
 * So the raw queue is kept alongside the instrumented one and the drain uses
 * it directly. Scoping this to the replay path, rather than to a window of
 * time, is deliberate: a flag flipped for the duration of `flush()` would
 * silently drop live application jobs that fail during the drain, which is
 * precisely the traffic this package exists to protect and precisely when it
 * is failing.
 */
type RegisteredQueue = {
  /** The queue as the application sees it. */
  instrumented: MinimalQueue;
  /** The same queue, without capture. Used only by `replay`. */
  raw: MinimalQueue;
};

export class Outbox {
  private readonly store: OutboxStore;
  private readonly maxAttempts: number;
  private readonly hooks: OutboxHooks;
  private readonly generateId: () => string;
  private readonly shouldCapture: NonNullable<OutboxOptions['shouldCapture']>;

  /** Queues known to this outbox, so `flush()` can find them again. */
  private readonly queues = new Map<string, RegisteredQueue>();

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
    const outbox = this;

    const proxy = new Proxy(queue, {
      get(target, property, receiver) {
        if (property === 'add') {
          return async function add(
            name: string,
            data: unknown,
            opts?: unknown,
          ) {
            try {
              // Inside try, not `Promise.resolve(...).catch`: a queue that
              // throws synchronously (a validation guard in some future
              // version) must be captured too.
              return await target.add(name, data, opts);
            } catch (error) {
              await outbox.capture(target.name, name, data, opts, error);
              throw error;
            }
          };
        }

        if (property === 'addBulk') {
          return async function addBulk(
            jobs: { name: string; data: unknown; opts?: unknown }[],
          ) {
            try {
              return await target.addBulk(jobs);
            } catch (error) {
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
            }
          };
        }

        // `target` as the receiver, not the proxy: a prototype getter reading
        // a #private field would throw if `this` were the proxy.
        const value = Reflect.get(target, property, target);

        if (typeof value === 'function') {
          const bound = value.bind(target);
          return function forward(this: unknown, ...args: unknown[]) {
            const result = bound(...args);
            // Chainable methods (`on`, `once`, `off`) return the queue itself.
            // Handing back the raw queue would silently drop the fallback for
            // anyone who writes `wrapQueue(q).on('error', log)`.
            return result === target ? receiver : result;
          };
        }

        return value;
      },
    }) as Q;

    this.queues.set(queue.name, { instrumented: proxy, raw: queue });

    return proxy;
  }

  /**
   * Return a queue *class* with the fallback built in, for frameworks that
   * construct queues for you.
   *
   * NestJS is the case that needs this: with `@nestjs/bullmq` you never call
   * `new Queue()` yourself, so there is no instance to wrap. Both
   * `@nestjs/bullmq` and `@taskforcesh/nestjs-bullmq-pro` expose a
   * `BullModule.queueClass` setter for exactly this kind of substitution:
   *
   *     BullModule.queueClass = outbox.queueClass(Queue);
   *
   * Set it before any module is registered and every queue in the app is
   * covered — including the ones added next year by someone who never read
   * this README. That is the argument for doing it here rather than wrapping
   * each injected queue: coverage stops depending on anyone remembering.
   *
   * Instances register themselves, so `flush()` finds them without a
   * `registerQueue` call.
   */
  queueClass<Q extends MinimalQueue>(
    BaseQueue: QueueConstructor<Q>,
  ): QueueConstructor<Q> {
    const outbox = this;

    // Subclassing the caller's class, not a class from this package: the
    // result is a real Queue, so `instanceof`, private fields and everything
    // the framework does to the instance afterwards keep working.
    return class OutboxQueue extends (BaseQueue as QueueConstructor<MinimalQueue>) {
      constructor(...args: ConstructorParameters<QueueConstructor<Q>>) {
        super(...args);

        // `raw` bypasses this subclass's overrides, so the drain re-enqueues
        // without going through capture. Bound to `super` explicitly rather
        // than reaching for the prototype, which is what the original
        // production implementation had to do.
        outbox.queues.set(this.name, {
          instrumented: this,
          raw: {
            name: this.name,
            add: (name, data, opts) => super.add(name, data, opts),
            addBulk: (jobs) => super.addBulk(jobs),
          },
        });
      }

      override async add(
        name: string,
        data: unknown,
        opts?: unknown,
      ): Promise<unknown> {
        try {
          return await super.add(name, data, opts);
        } catch (error) {
          await outbox.capture(this.name, name, data, opts, error);
          throw error;
        }
      }

      override async addBulk(
        jobs: { name: string; data: unknown; opts?: unknown }[],
      ): Promise<unknown> {
        try {
          return await super.addBulk(jobs);
        } catch (error) {
          for (const job of jobs) {
            await outbox.capture(
              this.name,
              job.name,
              job.data,
              job.opts,
              error,
            );
          }
          throw error;
        }
      }
    } as unknown as QueueConstructor<Q>;
  }

  /**
   * Register a queue that was not created through `wrapQueue` — for instance
   * when the process draining the outbox is not the one that enqueues.
   *
   * The queue is used for replay only, so pass the plain queue.
   */
  registerQueue(queue: MinimalQueue): void {
    this.queues.set(queue.name, { instrumented: queue, raw: queue });
  }

  /** Persist a failed enqueue by hand. `wrapQueue` calls this for you. */
  async capture(
    queueName: string,
    jobName: string,
    data: unknown,
    opts: unknown,
    error: unknown,
  ): Promise<void> {
    if (!this.shouldCapture({ queueName, jobName, error })) return;

    const message = messageOf(error);

    let entry: OutboxEntry;
    try {
      entry = {
        id: this.generateId(),
        queueName,
        jobName,
        data: snapshot(data),
        opts: serializableOpts(opts),
        createdAt: new Date().toISOString(),
        attempts: 0,
        lastError: message,
      };
    } catch (serializationError) {
      // A payload that cannot be JSON'd could never have been replayed. Say so
      // through the hook rather than throwing over the caller's real error.
      safely(() =>
        this.hooks.onSaveFailed?.({
          queueName,
          jobName,
          originalError: message,
          storeError: `payload is not serializable: ${messageOf(serializationError)}`,
        }),
      );
      return;
    }

    try {
      await this.store.save(entry);
      safely(() =>
        this.hooks.onJobSaved?.({
          queueName,
          jobName,
          id: entry.id,
          error: message,
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
          originalError: message,
          storeError: messageOf(storeError),
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
    const registered = this.queues.get(entry.queueName);

    // Not an error: with several services against one store, each drains the
    // queues it owns and leaves the rest to whoever registered them.
    if (!registered) {
      return {
        id: entry.id,
        queueName: entry.queueName,
        jobName: entry.jobName,
        status: 'skipped',
      };
    }

    try {
      // The raw queue: a failed replay must not capture a duplicate of the
      // entry it is replaying.
      await registered.raw.add(entry.jobName, entry.data, entry.opts);
    } catch (error) {
      return this.replayFailed(entry, messageOf(error));
    }

    // Past this point the job IS in Redis. A store that fails to record that
    // is a bookkeeping problem, not a delivery one: reporting it as a failed
    // replay would re-enqueue the job on the next drain and eventually expire
    // a job that actually succeeded.
    try {
      await this.store.markProcessed(entry.id);
    } catch (error) {
      safely(() =>
        this.hooks.onSaveFailed?.({
          queueName: entry.queueName,
          jobName: entry.jobName,
          originalError: 'replayed, but the store did not record it',
          storeError: messageOf(error),
        }),
      );
    }

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
  }

  private async replayFailed(
    entry: OutboxEntry,
    message: string,
  ): Promise<FlushResult['details'][number]> {
    const attempts = entry.attempts + 1;
    const expired = attempts >= this.maxAttempts;

    await this.store.markFailed({
      id: entry.id,
      error: message,
      attempts,
      expired,
    });

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

export function createOutbox(options: OutboxOptions): Outbox {
  return new Outbox(options);
}
