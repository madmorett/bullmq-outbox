/**
 * A DynamoDB store, using `@aws-sdk/client-dynamodb`. Copy this file, do not
 * install it.
 *
 * This is the shape that has been running in production at Monest since April
 * 2026, backing three services across three Redis instances. It is here as
 * evidence, not as the recommended default — if you are not already on AWS,
 * the Postgres example is the simpler road.
 *
 * Table:
 *   PK              string, hash key  — QUEUE#<queueName>
 *   SK              string, range key — <createdAt>#<id>
 *   GSI status-createdAt-index: hash `status`, range `createdAt`
 *   TTL attribute:  ttl
 *   Billing:        PAY_PER_REQUEST
 *
 * The GSI is what makes the drain cheap: it reads only PENDING items, in age
 * order, without ever scanning the table. The main table stays partitioned by
 * queue so a single hot queue cannot throttle the others.
 */
import {
  DynamoDB,
  type QueryCommandInput,
} from '@aws-sdk/client-dynamodb';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import type { OutboxEntry, OutboxStore } from 'bullmq-outbox';

const STATUS_INDEX = 'status-createdAt-index';
/** How long a resolved item sticks around before DynamoDB reaps it. */
const TTL_SECONDS = 7 * 24 * 60 * 60;

type Row = {
  PK: string;
  SK: string;
  id: string;
  status: 'PENDING' | 'PROCESSED' | 'EXPIRED';
  queueName: string;
  jobName: string;
  data: string;
  opts?: string;
  createdAt: string;
  attempts: number;
  lastError?: string;
  ttl?: number;
};

export function createDynamoOutboxStore(
  client: DynamoDB,
  tableName: string,
): OutboxStore {
  /**
   * `flush` addresses entries by id, but the table is keyed by (PK, SK). The
   * keys are derivable from the entry, so they are cached on read instead of
   * costing an extra query per update.
   */
  const keysById = new Map<string, { PK: string; SK: string }>();

  const keysFor = (id: string) => {
    const keys = keysById.get(id);
    if (!keys) {
      throw new Error(
        `Unknown outbox entry '${id}' — markProcessed/markFailed must follow a loadPending in the same process`,
      );
    }
    return keys;
  };

  return {
    async save(entry: OutboxEntry) {
      const row: Row = {
        PK: `QUEUE#${entry.queueName}`,
        SK: `${entry.createdAt}#${entry.id}`,
        id: entry.id,
        status: 'PENDING',
        queueName: entry.queueName,
        jobName: entry.jobName,
        data: JSON.stringify(entry.data),
        opts: entry.opts === undefined ? undefined : JSON.stringify(entry.opts),
        createdAt: entry.createdAt,
        attempts: entry.attempts,
        lastError: entry.lastError,
      };

      await client.putItem({
        TableName: tableName,
        Item: marshall(row, { removeUndefinedValues: true }),
      });
    },

    async loadPending(limit: number) {
      const params: QueryCommandInput = {
        TableName: tableName,
        IndexName: STATUS_INDEX,
        KeyConditionExpression: '#status = :status',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: marshall({ ':status': 'PENDING' }),
        Limit: limit,
        ScanIndexForward: true,
      };

      const result = await client.query(params);

      return (result.Items ?? []).map((item) => {
        const row = unmarshall(item) as Row;
        keysById.set(row.id, { PK: row.PK, SK: row.SK });

        return {
          id: row.id,
          queueName: row.queueName,
          jobName: row.jobName,
          data: JSON.parse(row.data),
          opts: row.opts ? JSON.parse(row.opts) : undefined,
          createdAt: row.createdAt,
          attempts: row.attempts,
          lastError: row.lastError,
        } satisfies OutboxEntry;
      });
    },

    async markProcessed(id: string) {
      await client.updateItem({
        TableName: tableName,
        Key: marshall(keysFor(id)),
        UpdateExpression: 'SET #status = :status, #ttl = :ttl',
        ExpressionAttributeNames: { '#status': 'status', '#ttl': 'ttl' },
        ExpressionAttributeValues: marshall({
          ':status': 'PROCESSED',
          ':ttl': Math.floor(Date.now() / 1000) + TTL_SECONDS,
        }),
      });
      keysById.delete(id);
    },

    async markFailed(
      id: string,
      error: string,
      attempts: number,
      expired: boolean,
    ) {
      const keys = keysFor(id);

      if (!expired) {
        // Still pending — only the counter and the error move.
        await client.updateItem({
          TableName: tableName,
          Key: marshall(keys),
          UpdateExpression: 'SET attempts = :attempts, lastError = :lastError',
          ExpressionAttributeValues: marshall({
            ':attempts': attempts,
            ':lastError': error,
          }),
        });
        return;
      }

      await client.updateItem({
        TableName: tableName,
        Key: marshall(keys),
        UpdateExpression:
          'SET #status = :status, attempts = :attempts, lastError = :lastError, #ttl = :ttl',
        ExpressionAttributeNames: { '#status': 'status', '#ttl': 'ttl' },
        ExpressionAttributeValues: marshall({
          ':status': 'EXPIRED',
          ':attempts': attempts,
          ':lastError': error,
          ':ttl': Math.floor(Date.now() / 1000) + TTL_SECONDS,
        }),
      });
      keysById.delete(id);
    },
  };
}
