/**
 * Does the drain stay cheap as the store fills up?
 *
 * This is the property that makes the DynamoDB deployment at Monest scale: the
 * `status-createdAt-index` GSI reads only PENDING items, oldest first, without
 * ever touching the table. Cost is proportional to what is still pending — not
 * to everything ever stored.
 *
 * Postgres and Mongo get the same property from a PARTIAL index. This script
 * checks that claim instead of assuming it: fill the store with resolved
 * entries, keep a handful pending, and measure `loadPending` as the backlog of
 * history grows. Flat is the answer we want. Growth means a sequential scan,
 * and an outbox that gets slower every month.
 */
import { MongoClient } from 'mongodb';
import { Pool } from 'pg';
import { MONGO, PG, SCHEMA } from './helpers';

const STEPS = [0, 50_000, 200_000, 500_000];
const PENDING = 50;
const RUNS = 20;

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)]!;
}

async function timed(run: () => Promise<unknown>, times = RUNS): Promise<number> {
  await run(); // warm the plan cache
  const samples: number[] = [];
  for (let i = 0; i < times; i++) {
    const started = process.hrtime.bigint();
    await run();
    samples.push(Number(process.hrtime.bigint() - started) / 1e6);
  }
  return median(samples);
}

async function postgres() {
  const pool = new Pool({ connectionString: PG, max: 4 });
  await pool.query(SCHEMA);
  await pool.query('truncate bullmq_outbox');

  console.log('\n=== Postgres ===');
  console.log('Always 50 pending rows. Only the already-processed history grows.');
  console.log('');
  console.log('  processed rows in table | time to fetch the 50 pending | plan');

  // The pending entries stay constant; only history grows.
  await pool.query(`
    insert into bullmq_outbox (id, queue_name, job_name, data, created_at, attempts, status)
    select 'p' || g, 'emails', 'welcome', '{"userId":1}'::jsonb, now(), 0, 'pending'
    from generate_series(1, $1::bigint) g
  `, [PENDING]);

  let inserted = 0;
  for (const target of STEPS) {
    if (target > inserted) {
      await pool.query(`
        insert into bullmq_outbox (id, queue_name, job_name, data, created_at, attempts, status)
        select 'r' || g, 'emails', 'welcome', '{"userId":1}'::jsonb,
               now() - (g || ' seconds')::interval, 0, 'processed'
        from generate_series($1::bigint, $2::bigint) g
      `, [inserted + 1, target]);
      inserted = target;
      await pool.query('analyze bullmq_outbox');
    }

    const ms = await timed(() =>
      pool.query(
        `select id, queue_name, job_name, data, opts, created_at, attempts, last_error
           from bullmq_outbox where status = 'pending' order by created_at limit $1`,
        [PENDING],
      ),
    );

    const plan = await pool.query(
      `explain (format json) select id from bullmq_outbox
        where status = 'pending' order by created_at limit ${PENDING}`,
    );
    const node = plan.rows[0]['QUERY PLAN'][0]['Plan'];
    const scan = JSON.stringify(node).includes('Seq Scan') ? 'SEQ SCAN ⚠' : 'index';

    console.log(
      `  ${String(inserted).padStart(22)} | ${ms.toFixed(2).padStart(24)} ms | ${scan}`,
    );
  }

  const size = await pool.query(
    "select pg_size_pretty(pg_total_relation_size('bullmq_outbox')) s, " +
    "pg_size_pretty(pg_relation_size('bullmq_outbox_pending')) i",
  );
  console.log(`table ${size.rows[0].s}, partial index ${size.rows[0].i}`);
  await pool.end();
}

async function mongo() {
  const client = new MongoClient(MONGO);
  await client.connect();
  const db = client.db('outbox_test');
  const collection = db.collection('bullmq_outbox');

  await collection.deleteMany({});
  await collection.createIndex(
    { status: 1, createdAt: 1 },
    { name: 'pending_by_age', partialFilterExpression: { status: 'pending' } },
  );

  console.log('\n=== MongoDB ===');
  console.log('Always 50 pending docs. Only the already-processed history grows.');
  console.log('');
  console.log('  processed docs in coll. | time to fetch the 50 pending | plan');

  await collection.insertMany(
    Array.from({ length: PENDING }, (_, i) => ({
      _id: `p${i}`,
      queueName: 'emails',
      jobName: 'welcome',
      data: { userId: 1 },
      createdAt: new Date(),
      attempts: 0,
      status: 'pending',
    })) as never,
  );

  let inserted = 0;
  for (const target of STEPS) {
    while (inserted < target) {
      const batch = Math.min(50_000, target - inserted);
      await collection.insertMany(
        Array.from({ length: batch }, (_, i) => ({
          _id: `r${inserted + i}`,
          queueName: 'emails',
          jobName: 'welcome',
          data: { userId: 1 },
          createdAt: new Date(Date.now() - (inserted + i) * 1000),
          attempts: 0,
          status: 'processed',
          resolvedAt: new Date(),
        })) as never,
        { ordered: false },
      );
      inserted += batch;
    }

    const query = () =>
      collection.find({ status: 'pending' }).sort({ createdAt: 1 }).limit(PENDING).toArray();

    const ms = await timed(query);

    const explain = await collection
      .find({ status: 'pending' })
      .sort({ createdAt: 1 })
      .limit(PENDING)
      .explain('executionStats');
    const stats = explain.executionStats;
    const scan = JSON.stringify(explain.queryPlanner.winningPlan).includes('COLLSCAN')
      ? 'COLLSCAN ⚠'
      : 'index';

    console.log(
      `  ${String(inserted).padStart(22)} | ${ms.toFixed(2).padStart(24)} ms | ${scan}` +
      ` (read ${stats.totalDocsExamined} docs to return ${stats.nReturned})`,
    );
  }

  const stats = await db.command({ collStats: 'bullmq_outbox' });
  console.log(
    `collection ${(stats.size / 1e6).toFixed(1)} MB, ` +
    `pending index ${(stats.indexSizes.pending_by_age / 1e3).toFixed(0)} kB`,
  );
  await client.close();
}

/**
 * The control case. Everything above holds *because* of the partial index;
 * this is what the same query costs without it, so the claim is falsifiable
 * rather than decorative.
 */
async function withoutTheIndex() {
  const pool = new Pool({ connectionString: PG, max: 4 });

  console.log('\n=== Postgres, same 500k table, WITHOUT the partial index ===');
  await pool.query('drop index if exists bullmq_outbox_pending');
  await pool.query('analyze bullmq_outbox');

  const ms = await timed(() =>
    pool.query(
      `select id from bullmq_outbox where status = 'pending' order by created_at limit $1`,
      [PENDING],
    ),
    5,
  );
  const plan = await pool.query(
    `explain (format json) select id from bullmq_outbox
      where status = 'pending' order by created_at limit ${PENDING}`,
  );
  const scan = JSON.stringify(plan.rows[0]['QUERY PLAN'][0]['Plan']).includes('Seq Scan')
    ? 'SEQ SCAN ⚠'
    : 'index';
  console.log(`  ${ms.toFixed(2)} ms | ${scan}`);

  // Put it back so the test suites still run against the documented schema.
  await pool.query(
    `create index if not exists bullmq_outbox_pending
       on bullmq_outbox (created_at) where status = 'pending'`,
  );
  await pool.end();
}

async function main() {
  console.log(
    'The question: after months in production, with hundreds of thousands of\n' +
    'jobs already processed sitting in the table, does finding the few still\n' +
    `pending get slower?\n\n` +
    `Every row below has exactly ${PENDING} PENDING entries to fetch. What grows\n` +
    'is the pile of already-processed rows around them.\n\n' +
    'Flat timings = no. Which is the same property the DynamoDB GSI gives.',
  );
  await postgres();
  await mongo();
  await withoutTheIndex();
}

void main();
