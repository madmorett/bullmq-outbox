/**
 * The drain, as a BullMQ repeatable job.
 *
 * The queue holding this scheduler must live on a DIFFERENT Redis from the
 * queues you are recovering — a small dedicated instance is enough. A drain
 * loop scheduled in the Redis that just died never fires, which is the failure
 * mode this whole package exists to avoid.
 *
 * Register it against a named connection:
 *
 *   BullModule.forRoot('outbox', { connection: { host: OUTBOX_REDIS_HOST } }),
 *   BullModule.registerQueue({ name: 'outbox-drain', configKey: 'outbox' }),
 */
import { Inject, Injectable, Logger, type OnModuleInit } from '@nestjs/common';
import { InjectQueue, Processor, WorkerHost } from '@nestjs/bullmq';
import type { Queue } from 'bullmq';
import type { Outbox } from 'bullmq-outbox';
import { OUTBOX } from './outbox.module';

const DRAIN_QUEUE = 'outbox-drain';
const BATCH_SIZE = 50;

@Injectable()
@Processor(DRAIN_QUEUE)
export class OutboxDrainProcessor extends WorkerHost implements OnModuleInit {
  private readonly logger = new Logger(OutboxDrainProcessor.name);

  constructor(
    @Inject(OUTBOX) private readonly outbox: Outbox,
    @InjectQueue(DRAIN_QUEUE) private readonly queue: Queue,
  ) {
    super();
  }

  async onModuleInit(): Promise<void> {
    // Idempotent: re-running this on every boot and every replica leaves one
    // scheduler, not one per pod.
    await this.queue.upsertJobScheduler(
      'drain',
      { every: 60_000 },
      { name: 'drain' },
    );
  }

  async process(): Promise<void> {
    const result = await this.outbox.flush(BATCH_SIZE);

    if (result.requeued === 0 && result.failed === 0) return;

    this.logger.log(
      `outbox drained: requeued=${result.requeued} failed=${result.failed} `
        + `skipped=${result.skipped} expired=${result.expired}`,
    );
  }
}

/**
 * `skipped` is not an error. Entries belong to queues this process does not
 * have registered — another service owns them and drains them itself. Several
 * services can share one outbox table.
 *
 * The number to watch is not `requeued`, it is the `ageMs` from
 * `onJobRequeued`: that is your actual recovery time.
 */
