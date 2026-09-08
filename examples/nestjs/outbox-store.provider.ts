/**
 * Building the store from config, if you would rather not construct it by
 * hand in `main.ts`.
 *
 * The catch: `main.ts` needs the Outbox before Nest's DI container exists, so
 * this pattern only works if you create the store yourself there too, or if
 * you accept that queues registered before `onModuleInit` are uncovered.
 *
 * In practice, build the store in `main.ts` from `process.env` — the outbox
 * config is three values and it needs to be ready earliest.
 */
import { Pool } from 'pg';
import { createPostgresOutboxStore } from '../postgres-store';
import type { OutboxStore } from 'bullmq-outbox';

export function outboxStoreFromEnv(): OutboxStore {
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    // The outbox writes only while your primary datastore for jobs is failing.
    // A small dedicated pool keeps that path from competing with request
    // traffic for connections at the exact moment things are going wrong.
    max: 2,
  });

  return createPostgresOutboxStore(pool);
}
