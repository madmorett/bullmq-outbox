/**
 * A drain that gets killed mid-flight.
 *
 * Runs as its own process so the parent can SIGKILL it — there is no way to
 * simulate a real crash in-process, and SIGKILL specifically because it is
 * the one signal a process cannot trap. That is what a container OOM kill,
 * a `docker stop` past its grace period, or a node losing power looks like.
 *
 * Coordination with the parent is through stdout lines, so the parent can
 * kill at an exact point instead of guessing with a timer:
 *
 *   READY                      — connected, about to flush
 *   REPLAYED <id>              — this entry reached Redis
 *   MARKED <id>                — the store recorded it as processed
 *   PAUSED <id>                — sitting in the window the parent wants to kill
 */
import { Queue } from 'bullmq';
import { Pool } from 'pg';
import { createOutbox } from '../src/index';
import { createPostgresOutboxStore } from './postgres-store';
import { PG, REDIS } from './helpers';

/** Where to stop and wait to be killed. */
type KillPoint =
  /** after the job is in Redis, before the store knows — the dangerous gap */
  | 'after-add-before-mark'
  /** before anything reaches Redis */
  | 'before-add';

const killPoint = process.argv[2] as KillPoint;
const queueName = process.argv[3];

async function main() {
  const pool = new Pool({ connectionString: PG, max: 2 });
  const baseStore = createPostgresOutboxStore(pool);
  const queue = new Queue(queueName, {
    connection: { ...REDIS, maxRetriesPerRequest: null },
  });

  const outbox = createOutbox({
    store: {
      ...baseStore,
      async markProcessed(id) {
        await baseStore.markProcessed(id);
        console.log(`MARKED ${id}`);
      },
    },
    onJobRequeued: (event) => {
      console.log(`REPLAYED ${event.id}`);
    },
  });

  // A queue whose `add` pauses at the chosen point, so the parent kills the
  // process inside a precise window rather than racing a setTimeout.
  outbox.registerQueue({
    name: queueName,
    async add(jobName, data, opts) {
      if (killPoint === 'before-add') {
        console.log(`PAUSED before-add`);
        await new Promise(() => {}); // never resolves; parent kills us here
      }
      const job = await queue.add(jobName, data as never, opts as never);
      if (killPoint === 'after-add-before-mark') {
        // The job IS in Redis now. The store still says pending. This is the
        // window where a crash decides between losing a job and duplicating one.
        console.log(`PAUSED after-add ${job.id}`);
        await new Promise(() => {});
      }
      return job;
    },
    async addBulk() {
      return [];
    },
  });

  console.log('READY');
  await outbox.flush(50);

  await queue.close();
  await pool.end();
  console.log('DONE');
}

main().catch((error) => {
  console.error('WORKER-ERROR', error);
  process.exit(1);
});
