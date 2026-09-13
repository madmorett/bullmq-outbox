/**
 * bullmq-outbox — MongoDB setup.
 * Run once at boot, or from `mongosh`. Safe to re-run.
 *
 * Pair it with examples/mongodb-store.ts.
 *
 *   node examples/schema/mongodb.js "mongodb://localhost:27017" mydb
 */
const { MongoClient } = require('mongodb');

async function setup(db) {
  const collection = db.collection('bullmq_outbox');

  // The only query on the hot path: pending entries, oldest first.
  //
  // partialFilterExpression is the important part. After an incident the
  // collection is almost entirely 'processed' documents, and a partial index
  // stays proportional to what is pending. Measured at 500k resolved docs:
  // 0.65 ms to read 50 pending, examining exactly 50 documents, with a 12 kB
  // index on an 81 MB collection.
  await collection.createIndex(
    { status: 1, createdAt: 1 },
    {
      name: 'pending_by_age',
      partialFilterExpression: { status: 'pending' },
    },
  );

  // Optional: let Mongo reap resolved documents after 7 days.
  //
  // This keys on `resolvedAt`, NOT `createdAt`, and the difference matters.
  // `resolvedAt` is set only when an entry leaves the drain rotation, so a
  // pending entry is never a TTL candidate no matter how long the outage
  // lasts. A TTL on `createdAt` would delete jobs mid-incident, before they
  // were ever replayed — silently losing exactly what you were protecting.
  await collection.createIndex(
    { resolvedAt: 1 },
    { name: 'resolved_ttl', expireAfterSeconds: 7 * 24 * 60 * 60 },
  );

  return collection;
}

module.exports = { setup };

if (require.main === module) {
  const [uri = 'mongodb://localhost:27017', dbName = 'app'] = process.argv.slice(2);
  const client = new MongoClient(uri);
  client
    .connect()
    .then(() => setup(client.db(dbName)))
    .then(() => console.log(`bullmq_outbox indexes ready on ${dbName}`))
    .catch((error) => {
      console.error(error);
      process.exitCode = 1;
    })
    .finally(() => client.close());
}
