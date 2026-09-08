# bullmq-outbox

When Redis runs out of memory or goes away, `queue.add()` rejects and the job
is gone. Not delayed — gone. There is nothing to retry, because nothing was
ever written down.

This package writes it down. A failed enqueue lands in a store you own, and a
scheduler you control puts it back into Redis when Redis comes back.

```ts
import { createOutbox } from 'bullmq-outbox';
import { Queue } from 'bullmq';

const outbox = createOutbox({ store: myStore });
const emails = outbox.wrapQueue(new Queue('emails'));

// Behaves exactly like the queue you passed in, until Redis says no.
await emails.add('welcome', { userId: 1 });
```

- **No dependencies.** Not even `bullmq`.
- **Works with BullMQ v5, v6 and [BullMQ Pro](https://taskforce.sh/).** The
  wrapper is structural, so it does not care which one you have.
- **Any store.** Four functions against whatever database you already run —
  Postgres, Redis, DynamoDB, Mongo, SQLite.
- **Not a broker.** It is a fallback. Redis stays the queue.

---

## Install

```bash
npm install bullmq-outbox
```

## The store

The package ships no adapters, because your outbox belongs in a database you
already operate. You write four functions:

```ts
import type { OutboxStore } from 'bullmq-outbox';

const myStore: OutboxStore = {
  save(entry)                                 { /* persist it */ },
  loadPending(limit)                          { /* oldest first */ },
  markProcessed(id)                           { /* it made it back */ },
  markFailed(id, error, attempts, expired)    { /* it did not */ },
};
```

Working implementations to copy, not install:

| Store | File | Notes |
|---|---|---|
| **Postgres** | [`examples/postgres-store.ts`](examples/postgres-store.ts) | The simplest road. A table and one partial index. |
| **Redis** | [`examples/redis-store.ts`](examples/redis-store.ts) | Must be a *different* Redis from your queues. See the caveat in the file. |
| **DynamoDB** | [`examples/dynamodb-store.ts`](examples/dynamodb-store.ts) | The shape running in production at Monest. |

There is also a `MemoryOutboxStore` for tests. It is not durable and it is not
for production.

## Draining

`flush()` re-enqueues what it finds. Call it however you schedule things:

```ts
const result = await outbox.flush(50);
// { requeued: 12, failed: 0, skipped: 3, expired: 0, details: [...] }
```

**Run the scheduler somewhere that does not share the failure you are
recovering from.** This is the part people get wrong. A drain loop that lives
in the Redis that just died never fires. Use a cron, a Lambda, a separate
small Redis — anything with an independent failure domain.

A job that keeps failing to re-enqueue is marked expired after `maxAttempts`
(default 10) and your store decides what that means: a dead letter table, an
alert, a row someone looks at on Monday.

## NestJS

With `@nestjs/bullmq` you never call `new Queue()` — the module builds it and
you get it through `@InjectQueue()`. There is no instance to wrap, so swap the
class instead:

```ts
// main.ts, before NestFactory.create()
BullModule.queueClass = outbox.queueClass(Queue);
```

Every queue in the app is now covered, including ones added later by someone
who never read this. Coverage stops depending on anyone remembering.

Both `@nestjs/bullmq` and `@taskforcesh/nestjs-bullmq-pro` expose that setter —
it is the documented way to substitute `QueuePro`, and it takes any subclass.
It must run before the app bootstraps: queue providers read it at construction,
so a module's `onModuleInit` is too late.

Full working example, including the drain processor:
[**`examples/nestjs/`**](examples/nestjs/)

## Full example

```ts
import { createOutbox } from 'bullmq-outbox';
import { Queue } from 'bullmq';
import { createPostgresOutboxStore } from './outbox-store'; // copied from examples/

const outbox = createOutbox({
  store: createPostgresOutboxStore(pool),
  maxAttempts: 10,

  // Real-time queues are better off dropping a job than replaying it later.
  // A chat turn delivered fifteen minutes late is worse than one never sent.
  shouldCapture: ({ queueName }) => !queueName.startsWith('live-'),

  onJobSaved:    (e) => metrics.increment('outbox.saved', { queue: e.queueName }),
  onJobRequeued: (e) => metrics.timing('outbox.recovery_ms', e.ageMs),
  onJobExpired:  (e) => alerts.page('outbox job expired', e),
});

export const emails = outbox.wrapQueue(new Queue('emails', { connection }));

// somewhere with its own failure domain
setInterval(() => outbox.flush(50), 60_000);
```

If the process that drains is not the process that enqueues, register the
queues there instead of wrapping them:

```ts
outbox.registerQueue(new Queue('emails', { connection }));
await outbox.flush();
```

Entries for queues this process does not know about are reported as `skipped`
and left alone, so several services can share one outbox table and each drains
only what it owns.

## API

### `createOutbox(options)`

| Option | Default | |
|---|---|---|
| `store` | *required* | Your four functions. |
| `maxAttempts` | `10` | Re-enqueue attempts before an entry is expired. |
| `shouldCapture` | capture everything | Return `false` to let a failure through unstored. |
| `generateId` | `randomUUID()` | Entry ids. |
| `onJobSaved` · `onJobRequeued` · `onJobExpired` · `onSaveFailed` · `onFlush` | — | Optional. Exceptions inside a hook are swallowed. |

### `outbox.wrapQueue(queue)`

Returns a Proxy over your queue. `add` and `addBulk` gain the fallback;
everything else — `getJob`, `pause`, `upsertJobScheduler`, Pro's group and
batch APIs — passes straight through.

### `outbox.queueClass(BaseQueue)`

Returns a subclass of `BaseQueue` with the fallback built in, for frameworks
that construct queues for you. Instances self-register, so `flush()` finds them
without a `registerQueue` call. See [NestJS](#nestjs).

### `outbox.flush(limit?)`

Re-enqueues pending entries. Returns counts plus a per-entry breakdown.

### `outbox.capture(queueName, jobName, data, opts, error)`

Store a failure by hand, if you would rather not wrap the queue.

## Things worth knowing

**The original error is always re-thrown.** The outbox buys you a replay, not
a lie. Your caller still finds out the enqueue failed and still decides what
to tell the user.

**A failed `addBulk` stores every job in the batch.** At this layer a partial
failure is indistinguishable from a total one, and a job replayed twice is
cheaper than a job lost. Set a `jobId` if you need the replay to dedupe.

**`parent` is stripped from stored options.** A flow parent may not exist by
the time the outbox drains, and BullMQ would reject the whole re-enqueue.
Flow children come back as standalone jobs.

**Replay is at-least-once.** If the store write succeeds and the process dies
before the error propagates, you may get the job twice. Idempotent handlers,
or a `jobId`.

**A broken store never breaks a job.** If `save` throws, `onSaveFailed` fires
and the Redis error propagates unchanged — you do not want the fallback's
failure hiding the real one.

## Where this came from

Built at [Monest](https://monest.com.br) after losing jobs to an ElastiCache
OOM. Three services, three Redis instances, one outbox table.

Two things we learned that are not in the code:

- Set `reserved-memory-percent` on your Redis parameter group. A Redis at 100%
  of `maxmemory` does not fail cleanly — it hangs, and BullMQ hangs with it
  (`maxRetriesPerRequest: null` is required by BullMQ, so ioredis retries
  forever). Reserving a slice makes the OOM arrive as an error you can catch.
- Watch the age of what you replay, not the count. `onJobRequeued` gives you
  `ageMs`; that number is your real recovery time, and it is the one that
  tells you whether the fallback is working or quietly filling up.

Seeing that second point is easier with a dashboard.
[**Bullpane**](https://bullpane.com) is a self-hosted BullMQ dashboard — free
edition, no login, does everything bull-board does.

## See also

[**bullmq-fanout**](https://github.com/madmorett/bullmq-fanout) — publish one
domain event to many queues, each with its own retries and failure domain.
Wrap your queues with this package first and the fan-out inherits the
durability.

## License

MIT © [Matheus Morett](https://matheusmorett.com/)
