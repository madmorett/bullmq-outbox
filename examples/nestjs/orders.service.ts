/**
 * An ordinary service. Read it and notice that nothing here knows the outbox
 * exists — no decorator, no injected helper, no try/catch.
 *
 * That is the whole benefit of substituting the queue class: the fallback is
 * a property of the infrastructure, not something every author has to opt into
 * correctly. Code written a year from now by someone who never heard of this
 * package is covered too.
 */
import { Injectable, Logger } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import type { Queue } from 'bullmq';

@Injectable()
export class OrdersService {
  private readonly logger = new Logger(OrdersService.name);

  constructor(@InjectQueue('invoices') private readonly invoices: Queue) {}

  async markPaid(orderId: number): Promise<void> {
    try {
      await this.invoices.add('issue-invoice', { orderId });
    } catch (error) {
      // The error still reaches you — the outbox buys a replay, not a lie.
      // By the time you are here the job is already persisted and will be
      // re-enqueued, so this is about what to tell the caller, not about
      // recovering the job.
      this.logger.error(`invoice enqueue failed for ${orderId}, will retry`, error);
      throw error;
    }
  }
}
