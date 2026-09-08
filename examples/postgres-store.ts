/**
 * A Postgres store, using `pg`. Copy this file, do not install it.
 *
 *   create table bullmq_outbox (
 *     id           text primary key,
 *     queue_name   text        not null,
 *     job_name     text        not null,
 *     data         jsonb       not null,
 *     opts         jsonb,
 *     created_at   timestamptz not null,
 *     attempts     int         not null default 0,
 *     last_error   text,
 *     status       text        not null default 'pending'
 *   );
 *
 *   -- the only query on the hot path: oldest pending first
 *   create index bullmq_outbox_pending
 *     on bullmq_outbox (created_at)
 *     where status = 'pending';
 *
 * The partial index is the point. Once the incident is over the table is
 * almost entirely 'processed' rows, and a partial index keeps the drain query
 * proportional to what is still pending rather than to everything you ever
 * stored.
 *
 * Deleting on success instead of setting status = 'processed' is also
 * reasonable — you keep the table small at the cost of losing the audit
 * trail. Pick one; do not do neither.
 */
import type { Pool } from 'pg';
import type { OutboxEntry, OutboxStore } from 'bullmq-outbox';

export function createPostgresOutboxStore(pool: Pool): OutboxStore {
  return {
    async save(entry: OutboxEntry) {
      await pool.query(
        `insert into bullmq_outbox
           (id, queue_name, job_name, data, opts, created_at, attempts, last_error, status)
         values ($1, $2, $3, $4, $5, $6, $7, $8, 'pending')
         on conflict (id) do nothing`,
        [
          entry.id,
          entry.queueName,
          entry.jobName,
          JSON.stringify(entry.data),
          entry.opts === undefined ? null : JSON.stringify(entry.opts),
          entry.createdAt,
          entry.attempts,
          entry.lastError ?? null,
        ],
      );
    },

    async loadPending(limit: number) {
      const { rows } = await pool.query(
        `select id, queue_name, job_name, data, opts, created_at, attempts, last_error
           from bullmq_outbox
          where status = 'pending'
          order by created_at
          limit $1`,
        [limit],
      );

      return rows.map((row): OutboxEntry => ({
        id: row.id,
        queueName: row.queue_name,
        jobName: row.job_name,
        data: row.data,
        opts: row.opts ?? undefined,
        createdAt: new Date(row.created_at).toISOString(),
        attempts: row.attempts,
        lastError: row.last_error ?? undefined,
      }));
    },

    async markProcessed(id: string) {
      await pool.query(
        `update bullmq_outbox
            set status = 'processed', last_error = null
          where id = $1`,
        [id],
      );
    },

    async markFailed(
      id: string,
      error: string,
      attempts: number,
      expired: boolean,
    ) {
      await pool.query(
        `update bullmq_outbox
            set attempts = $2, last_error = $3, status = $4
          where id = $1`,
        [id, attempts, error, expired ? 'expired' : 'pending'],
      );
    },
  };
}

/**
 * Running more than one drain process? Claim rows instead of reading them,
 * so two schedulers never replay the same job:
 *
 *   update bullmq_outbox
 *      set status = 'claimed'
 *    where id in (
 *      select id from bullmq_outbox
 *       where status = 'pending'
 *       order by created_at
 *       limit $1
 *       for update skip locked
 *    )
 *   returning ...
 *
 * and treat 'claimed' rows older than a few minutes as pending again, in case
 * the claimer died mid-flush.
 */
