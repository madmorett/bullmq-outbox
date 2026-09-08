/**
 * A global module that hands the `Outbox` to anything that wants it.
 *
 * The instance is created in `main.ts` (it has to exist before the app does)
 * and passed in here, so the object capturing failed jobs is the same one that
 * drains them.
 */
import { Global, Module, type DynamicModule } from '@nestjs/common';
import type { Outbox } from 'bullmq-outbox';

export const OUTBOX = Symbol('OUTBOX');

@Global()
@Module({})
export class OutboxModule {
  static forRoot(outbox: Outbox): DynamicModule {
    return {
      module: OutboxModule,
      providers: [{ provide: OUTBOX, useValue: outbox }],
      exports: [OUTBOX],
    };
  }
}

/**
 * Inject it with `@Inject(OUTBOX) private readonly outbox: Outbox`.
 *
 * Most code never needs to: the queues are already instrumented by
 * `BullModule.queueClass`. This is for the drain processor and for anything
 * that wants to report on what is pending.
 */
