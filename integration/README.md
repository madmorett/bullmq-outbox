# Integration tests

Unit tests with a fake queue prove the logic. They cannot prove the package
survives a Redis that is actually out of memory — and that is the only thing
this package is for.

So these run against a real Redis, a real Postgres and a real MongoDB, and
break the Redis for real.

```bash
pnpm install
pnpm run verify     # up + test + down
```

Or keep the containers between runs:

```bash
pnpm run up
pnpm test
pnpm run scale      # the scalability measurement below
pnpm run down
```

## How the outage is simulated

`maxmemory` is set just above what Redis is already using, so every write is
rejected with:

```
OOM command not allowed when used memory > 'maxmemory'
```

That is the exact error an ElastiCache node throws when it fills up.

Setting `maxmemory` to something tiny like 1 byte does **not** work: Redis
cannot hold its own structures and the server dies, which tests reconnection
rather than OOM. `helpers.ts` also probes with a real write before letting a
test proceed, so a test never runs against a Redis it only assumes is broken.

## What is covered

Both the Postgres and the MongoDB example stores are tested verbatim, against
the same six scenarios:

- a real OOM stores the job, and a real BullMQ `Worker` receives it after the drain
- repeated failed drains do **not** multiply entries
- `queueClass` covers queues built the way NestJS builds them, with no `wrapQueue` call
- job options, including a custom `jobId`, survive the round trip
- a job that never gets back in expires and leaves the drain rotation
- entries for another service's queues are skipped, not stolen

MongoDB adds one more: the TTL index must never reap an entry that is still
pending. It keys on `resolvedAt`, which is set only when an entry leaves the
rotation — a TTL on `createdAt` would delete jobs mid-outage, before they were
ever replayed.

## What survives a crash?

Every other test here simulates failure — a closed connection, a Redis at
`maxmemory`. None of them kill the process. But the failure that actually
loses jobs in production is the one where the machine goes away mid-drain: an
OOM kill, a `docker stop` past its grace period, a spot instance reclaimed.

`crash.test.ts` spawns a real drain in its own process and `SIGKILL`s it at a
precise point — SIGKILL because it cannot be trapped, so no cleanup handler
runs. That is the point.

The worker prints where it is (`READY`, `REPLAYED <id>`, `MARKED <id>`,
`PAUSED <id>`) so the test kills at an exact moment instead of racing a timer.

Three cases:

- **killed between Redis and the store** — the dangerous window. The job is in
  Redis, the store still says pending. The entry survives as pending and the
  next drain replays it. At-least-once: the job may arrive twice, which is the
  deliberate trade — a duplicate is recoverable with an idempotent handler, a
  lost job is not.
- **killed before anything reaches Redis** — the entry is untouched, and
  crucially no retry is burned.
- **killed three times in a row** — no entry disappears, and a clean drain
  afterwards finishes the work.

The tests were validated by breaking the package on purpose: reordering
`markProcessed` before `add` (the classic outbox bug that loses jobs) makes two
of the three fail immediately.

## Does it scale?

`pnpm run scale` answers this with numbers rather than assurances.

The worry: after months in production the store holds hundreds of thousands of
already-processed jobs. Does finding the few still pending get slower?

**Every row below has exactly 50 pending entries to fetch.** What grows is the
pile of already-processed rows around them.

```
=== Postgres ===
  processed rows in table | time to fetch the 50 pending | plan
                        0 |                      1.01 ms | index
                    50000 |                      0.88 ms | index
                   200000 |                      0.66 ms | index
                   500000 |                      0.94 ms | index
table 74 MB, partial index 16 kB

=== MongoDB ===
  processed docs in coll. | time to fetch the 50 pending | plan
                        0 |                      0.87 ms | index (read 50 docs to return 50)
                    50000 |                      1.29 ms | index (read 50 docs to return 50)
                   200000 |                      0.69 ms | index (read 50 docs to return 50)
                   500000 |                      0.71 ms | index (read 50 docs to return 50)
collection 81.4 MB, pending index 12 kB

=== Postgres, same 500k table, WITHOUT the partial index ===
  13.70 ms | SEQ SCAN
```

Flat on both. The row-to-row differences are noise — they do not move in one
direction — and that is exactly the point: history costs nothing.

The control case is what makes this falsifiable. Drop the partial index and
the same query on the same table takes 15× longer, on a sequential scan that
degrades with every job you ever process.

That property — cost proportional to the pending backlog, not to total
history — is the one that makes the production DynamoDB deployment scale,
where the `status-createdAt-index` GSI reads only PENDING items without
touching the table.

Index size says it from another angle: a 74 MB table with a 16 kB index.

Measured on Docker containers on a laptop, so the absolute milliseconds are
indicative. The shape — constant rather than growing — is what matters and is
what survives a move to real infrastructure.

## Ports

Redis **6398**, Postgres **54329**, MongoDB **27019** — chosen so they cannot
collide with anything you already run.
