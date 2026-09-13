# bullmq-outbox — integration guide

Written for a coding agent wiring this into an existing codebase. Dense on
purpose: contracts, decisions, and the mistakes that cost jobs.

Human-facing overview: [README.md](README.md).

## What it does, precisely

When `queue.add()` rejects because Redis is unreachable or out of memory, the
job is written to a store you own. A drain you schedule re-enqueues it later.

**It does not**: keep workers running while Redis is down (nothing processes),
swallow the error (it always re-throws), or replace Redis as the queue.

If the user's expectation is "my app keeps working with Redis down", correct
it: what they get is "no job is lost, and everything catches up when Redis
returns".

## Install

```bash
npm install bullmq-outbox
```

Zero dependencies. Peer-compatible with BullMQ v5, v6 and `@taskforcesh/bullmq-pro`
by structural typing — the package never imports `bullmq`.

## Step 1 — create the storage

The package ships **no** adapters. The user writes four functions against a
database they already run. Copy a store from
[`examples/`](examples/) in the repo (they are not in the npm tarball):

| Store | Store file | Schema |
|---|---|---|
| Postgres | [`examples/postgres-store.ts`](examples/postgres-store.ts) | [`examples/schema/postgres.sql`](examples/schema/postgres.sql) |
| MongoDB | [`examples/mongodb-store.ts`](examples/mongodb-store.ts) | [`examples/schema/mongodb.js`](examples/schema/mongodb.js) |
| Redis | [`examples/redis-store.ts`](examples/redis-store.ts) | — |
| DynamoDB | [`examples/dynamodb-store.ts`](examples/dynamodb-store.ts) | in the file header |

Postgres and MongoDB are covered by end-to-end tests against real servers, so
prefer them unless the user's stack says otherwise.

**Run the schema before the app starts.** `examples/schema/postgres.sql` is
idempotent — add it to the user's migration tool rather than executing it at
boot if they have one.

**Do not drop or "simplify" the partial index.** All four store queries run on
an index as shipped: the drain uses the partial index, and save/markProcessed/
markFailed are by `id`, covered by the primary key. Nothing else needs adding.
Replacing the partial index with a plain one on `(status, created_at)` gives
the same speed but 19 MB instead of 16 kB at 500k rows, growing forever.
Removing it turns the drain into a sequential scan.

**Redis as the store only works if it is a different Redis** from the one
running the queues. Same instance means the fallback dies with what it backs
up. Say so if the user asks for it.

### Writing a store from scratch

```ts
import type { OutboxStore } from 'bullmq-outbox';

const store: OutboxStore = {
  async save(entry) { /* insert; entry.id is your primary key */ },
  async loadPending(limit) { /* oldest first, at most `limit` */ },
  async markProcessed(id) { /* it is back in Redis */ },
  async markFailed({ id, error, attempts, expired }) { /* it is not */ },
};
```

Hard requirements:

- `loadPending` must return **oldest first** and must never return an entry
  that `markProcessed` accepted, or one marked `expired`.
- `save` gets an `id` that is already unique; use it as the primary key.
- `entry.data` and `entry.opts` are JSON-safe by the time they reach you.
- Any of these throwing is survivable — the package reports it through
  `onSaveFailed` and never masks the original Redis error.

## Step 2 — instrument the queues

Two ways. Pick by whether the code constructs its own queues.

### Plain BullMQ — wrap the instance

```ts
import { createOutbox } from 'bullmq-outbox';
import { Queue } from 'bullmq';

const outbox = createOutbox({ store });
export const emails = outbox.wrapQueue(new Queue('emails', { connection }));
```

Export and use the **wrapped** queue. The unwrapped one has no fallback.

### NestJS — substitute the class

With `@nestjs/bullmq` the framework constructs queues, so there is no instance
to wrap:

```ts
// main.ts — BEFORE NestFactory.create()
BullModule.queueClass = outbox.queueClass(Queue);
```

This must run before the app bootstraps; queue providers read it at
construction, so a module's `onModuleInit` is too late. One line covers every
queue in the app, including ones added later.

BullMQ Pro: import `BullModule` from `@taskforcesh/nestjs-bullmq-pro` and
`QueuePro` from `@taskforcesh/bullmq-pro`. Same line.

Full example: [`examples/nestjs/`](examples/nestjs/).

## Step 3 — schedule the drain

```ts
setInterval(() => void outbox.flush(50), 60_000);
```

**Where this runs matters more than how often.** It must not depend on the
Redis it recovers from — a drain scheduled as a BullMQ repeatable job in the
dead Redis never fires. Valid: a separate small Redis, `@nestjs/schedule`, a
system cron, a Lambda.

If several replicas drain concurrently, either make the store claim rows
(`FOR UPDATE SKIP LOCKED` in Postgres, `findOneAndUpdate` in Mongo — both
documented at the bottom of the example stores) or elect a leader. Otherwise
two drains replay the same job.

If the process that drains is not the one that enqueues:

```ts
outbox.registerQueue(new Queue('emails', { connection }));
```

Entries for unregistered queues are reported `skipped` and left alone, so
several services can share one table and each drains what it owns.

## Configuration

| Option | Default | Notes |
|---|---|---|
| `store` | required | The four functions. |
| `maxAttempts` | `10` | Replay attempts before an entry is expired. **Global, not per entry** — changing it also changes the threshold for entries already stored. |
| `shouldCapture` | capture all | Return `false` to let a failure through unstored. |
| `generateId` | `randomUUID()` | Must be unique; a collision drops a job. |
| `onJobSaved` / `onJobRequeued` / `onJobExpired` / `onSaveFailed` / `onFlush` | — | Hook exceptions are swallowed. |

**Exclude real-time queues.** A chat turn replayed fifteen minutes later is
worse than one never sent:

```ts
shouldCapture: ({ queueName }) => !queueName.startsWith('live-'),
```

**The metric worth alerting on is `ageMs` from `onJobRequeued`** — actual
recovery time — and any `onJobExpired`, which means a job was given up on.

## Behaviour an integration must account for

- **The error is always re-thrown.** Callers still see the failure. HTTP
  handlers should still return 5xx; the job being safe is not the same as the
  request succeeding.
- **Replay is at-least-once.** A job can be delivered twice if the process
  dies between the store write and the error propagating. Handlers should be
  idempotent, or set a `jobId`.
- **A failed `addBulk` stores every job in the batch**, since a partial
  failure is indistinguishable from a total one at this layer.
- **`parent` is stripped from stored options.** Flow children are replayed as
  standalone jobs, because the parent may no longer exist at drain time. Do
  not rely on the outbox to preserve flows.
- **Payloads are snapshotted through JSON at capture.** A `Date` in a payload
  comes back as a string after a replay. Unserializable payloads are reported
  via `onSaveFailed` and not stored.
- **The drain never captures its own failures**, so a failed drain counts an
  attempt rather than storing a duplicate.

## Verification

Do not declare it working because it compiles. Prove the failure path:

1. Start the app with the store reachable.
2. Break Redis. In a container: `redis-cli config set maxmemory 1mb` after
   filling it, or stop the container. **Do not** set `maxmemory 1` — Redis
   dies rather than rejecting writes, which tests reconnection instead of OOM.
3. Trigger an enqueue. It should throw, and a row should appear in the store
   with `status = 'pending'`.
4. Restore Redis. After the drain interval the row should be gone and the job
   should be in the queue.

The repo's [`integration/`](integration/) suite does exactly this against
real containers, and is worth reading before writing your own.

## Common mistakes

| Symptom | Cause |
|---|---|
| Nothing is ever stored | Using the unwrapped queue, or `BullModule.queueClass` set after bootstrap |
| Entries pile up, never drain | Drain scheduled on the Redis that is down; or queues not registered in the draining process |
| Jobs delivered twice | Concurrent drains without row claiming |
| Store grows forever | Nothing deletes resolved rows — Postgres needs a cron; Mongo needs the TTL index |
| Drain slows down over months | Index created without the partial predicate, or not created at all |
| Real-time jobs arrive late | Missing `shouldCapture` exclusion |
