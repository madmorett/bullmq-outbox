/**
 * A MongoDB store, using the official `mongodb` driver. Copy this file, do
 * not install it.
 *
 * Typechecked against driver v7 and exercised against v6 in `integration/`,
 * so it works on both.
 *
 * Create the collection and its one index at boot:
 *
 *   const outbox = db.collection('bullmq_outbox');
 *
 *   // The only query on the hot path: pending entries, oldest first.
 *   await outbox.createIndex(
 *     { status: 1, createdAt: 1 },
 *     { name: 'pending_by_age', partialFilterExpression: { status: 'pending' } },
 *   );
 *
 * The partial index is the point. Once the incident is over the collection is
 * almost entirely 'processed' documents, and a partial index stays
 * proportional to what is still pending rather than to everything you ever
 * stored.
 *
 * Optional, and recommended — let Mongo reap resolved documents for you:
 *
 *   await outbox.createIndex(
 *     { resolvedAt: 1 },
 *     { name: 'ttl', expireAfterSeconds: 7 * 24 * 60 * 60 },
 *   );
 *
 * `resolvedAt` is only set once an entry is processed or expired, so pending
 * entries are never touched by the TTL monitor no matter how long the outage
 * lasts.
 */
import type { Collection, Db } from 'mongodb';
import type { OutboxEntry, OutboxStore } from 'bullmq-outbox';

type OutboxDoc = {
  /** The entry id is the document id: one upsert, no duplicate on retry. */
  _id: string;
  queueName: string;
  jobName: string;
  data: unknown;
  opts?: unknown;
  /** A real Date, so the index sorts chronologically and TTL can use it. */
  createdAt: Date;
  attempts: number;
  lastError?: string;
  status: 'pending' | 'processed' | 'expired';
  /** Set when the entry leaves the drain rotation. Drives the TTL index. */
  resolvedAt?: Date;
};

export function createMongoOutboxStore(db: Db): OutboxStore {
  const collection: Collection<OutboxDoc> = db.collection('bullmq_outbox');

  return {
    async save(entry: OutboxEntry) {
      await collection.insertOne({
        _id: entry.id,
        queueName: entry.queueName,
        jobName: entry.jobName,
        data: entry.data,
        opts: entry.opts,
        // Stored as a Date rather than the ISO string: Mongo sorts dates
        // correctly and the TTL monitor only understands real dates.
        createdAt: new Date(entry.createdAt),
        attempts: entry.attempts,
        lastError: entry.lastError,
        status: 'pending',
      });
    },

    async loadPending(limit: number) {
      const docs = await collection
        .find({ status: 'pending' })
        .sort({ createdAt: 1 })
        .limit(limit)
        .toArray();

      return docs.map((doc): OutboxEntry => ({
        id: doc._id,
        queueName: doc.queueName,
        jobName: doc.jobName,
        data: doc.data,
        opts: doc.opts ?? undefined,
        createdAt: doc.createdAt.toISOString(),
        attempts: doc.attempts,
        lastError: doc.lastError ?? undefined,
      }));
    },

    async markProcessed(id: string) {
      await collection.updateOne(
        { _id: id },
        {
          $set: { status: 'processed', resolvedAt: new Date() },
          $unset: { lastError: '' },
        },
      );
    },

    async markFailed(failure) {
      await collection.updateOne(
        { _id: failure.id },
        {
          $set: {
            attempts: failure.attempts,
            lastError: failure.error,
            // An expired entry must stop coming back from loadPending.
            // Keeping the document (rather than deleting it) is what lets
            // someone see what was lost on Monday morning.
            status: failure.expired ? 'expired' : 'pending',
            ...(failure.expired && { resolvedAt: new Date() }),
          },
        },
      );
    },
  };
}

/**
 * Running more than one drain process? Claim documents instead of reading
 * them, so two schedulers never replay the same job. `findOneAndUpdate` is
 * atomic, so each claim is exclusive:
 *
 *     const claimed: OutboxDoc[] = [];
 *     for (let i = 0; i < limit; i++) {
 *       const doc = await collection.findOneAndUpdate(
 *         { status: 'pending' },
 *         { $set: { status: 'claimed', claimedAt: new Date() } },
 *         { sort: { createdAt: 1 }, returnDocument: 'after' },
 *       );
 *       if (!doc) break;
 *       claimed.push(doc);
 *     }
 *
 * Then treat 'claimed' documents older than a few minutes as pending again,
 * in case the claimer died mid-flush.
 *
 * A note on payloads: Mongo stores `data` as BSON, so a key containing a dot
 * or starting with `$` is rejected on older server versions. The package
 * snapshots payloads through JSON before they reach you, which keeps types
 * simple, but it does not rename keys — if your jobs carry arbitrary
 * user-supplied keys, store `data` as a JSON string instead and parse it
 * back in `loadPending`.
 */
