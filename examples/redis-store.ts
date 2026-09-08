/**
 * A Redis store, using `ioredis`. Copy this file, do not install it.
 *
 * Read the caveat before using it: storing the fallback in Redis only helps
 * if it is a DIFFERENT Redis from the one running your queues. A separate
 * instance, ideally a separate failure domain — a small managed node with
 * `noeviction` is enough, since the outbox is empty except during incidents.
 *
 * Pointing this at the same Redis that just ran out of memory gives you a
 * fallback that fails at the same moment as what it is backing up.
 *
 * Layout:
 *   bullmq-outbox:entry:<id>  hash, the entry
 *   bullmq-outbox:pending     sorted set, score = createdAt, member = id
 */
import type { Redis } from 'ioredis';
import type { OutboxEntry, OutboxStore } from 'bullmq-outbox';

const ENTRY_PREFIX = 'bullmq-outbox:entry:';
const PENDING_KEY = 'bullmq-outbox:pending';

/** Keep expired entries around for a week so someone can look at them. */
const EXPIRED_TTL_SECONDS = 7 * 24 * 60 * 60;

export function createRedisOutboxStore(redis: Redis): OutboxStore {
  return {
    async save(entry: OutboxEntry) {
      // One round trip: the entry and its index slot are written together, so
      // a pending id can never point at a hash that does not exist.
      await redis
        .multi()
        .hset(`${ENTRY_PREFIX}${entry.id}`, {
          queueName: entry.queueName,
          jobName: entry.jobName,
          data: JSON.stringify(entry.data),
          opts: entry.opts === undefined ? '' : JSON.stringify(entry.opts),
          createdAt: entry.createdAt,
          attempts: String(entry.attempts),
          lastError: entry.lastError ?? '',
        })
        .zadd(PENDING_KEY, String(new Date(entry.createdAt).getTime()), entry.id)
        .exec();
    },

    async loadPending(limit: number) {
      const ids = await redis.zrange(PENDING_KEY, 0, String(limit - 1));
      if (ids.length === 0) return [];

      const pipeline = redis.pipeline();
      for (const id of ids) pipeline.hgetall(`${ENTRY_PREFIX}${id}`);
      const results = await pipeline.exec();

      const entries: OutboxEntry[] = [];
      /** Index slots whose hash is gone; dropped below rather than returned. */
      const dangling: string[] = [];

      results?.forEach(([error, value], index) => {
        const id = ids[index]!;
        const hash = value as Record<string, string> | null;

        // The hash expired or was deleted out from under the index. Drop the
        // dangling id rather than returning a half-entry forever.
        if (error || !hash || !hash.queueName) {
          dangling.push(id);
          return;
        }

        entries.push({
          id,
          queueName: hash.queueName,
          jobName: hash.jobName!,
          data: JSON.parse(hash.data!),
          opts: hash.opts ? JSON.parse(hash.opts) : undefined,
          createdAt: hash.createdAt!,
          attempts: Number(hash.attempts ?? 0),
          lastError: hash.lastError || undefined,
        });
      });

      if (dangling.length > 0) await redis.zrem(PENDING_KEY, ...dangling);

      return entries;
    },

    async markProcessed(id: string) {
      await redis
        .multi()
        .zrem(PENDING_KEY, id)
        .del(`${ENTRY_PREFIX}${id}`)
        .exec();
    },

    async markFailed(failure) {
      const key = `${ENTRY_PREFIX}${failure.id}`;
      const multi = redis.multi().hset(key, {
        attempts: String(failure.attempts),
        lastError: failure.error,
      });

      if (failure.expired) {
        // Out of the drain rotation, but kept long enough to be inspected.
        multi.zrem(PENDING_KEY, failure.id).expire(key, EXPIRED_TTL_SECONDS);
      }

      await multi.exec();
    },
  };
}
