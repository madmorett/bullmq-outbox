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

## Does it scale?

`pnpm run scale` answers this with numbers rather than assurances.

The property that makes the production DynamoDB deployment scale is that its
`status-createdAt-index` GSI reads only PENDING items, oldest first, without
touching the table — cost is proportional to what is pending, not to
everything ever stored.

Postgres and Mongo get the same property from a **partial index**. The script
fills the store with resolved entries, keeps 50 pending, and measures
`loadPending(50)` as history grows:

```
=== Postgres ===
resolved rows |  loadPending(50)  | plan
            0 |           1.14 ms | index
        50000 |           0.79 ms | index
       200000 |           0.89 ms | index
       500000 |           0.83 ms | index
table 74 MB, partial index 16 kB

=== MongoDB ===
resolved docs |  loadPending(50)  | plan
            0 |           0.92 ms | index (examined 50 docs for 50)
        50000 |           0.69 ms | index (examined 50 docs for 50)
       200000 |           1.09 ms | index (examined 50 docs for 50)
       500000 |           0.65 ms | index (examined 50 docs for 50)
collection 81.4 MB, pending index 12 kB
```

Flat, on both. Never a `Seq Scan` or a `COLLSCAN`. Mongo examines exactly 50
documents to return 50, with half a million in the collection.

The index sizes are the same story from another angle: a 74 MB table with a
16 kB index. The partial index only covers what is pending, so it tracks the
size of your backlog rather than the size of your history.

**If you drop the partial predicate, you lose this.** A plain index on
`(status, createdAt)` still works but grows with every job you ever stored;
the drain stays fast, the storage bill does not. And with no index at all the
query degrades to a full scan, which is an outbox that gets slower every
month — precisely when you need it.

## Ports

Redis **6398**, Postgres **54329**, MongoDB **27019** — chosen so they cannot
collide with anything you already run.
