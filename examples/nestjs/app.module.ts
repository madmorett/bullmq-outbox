import { Module, type DynamicModule } from '@nestjs/common';
import { BullModule } from '@nestjs/bullmq';
import type { Outbox } from 'bullmq-outbox';
import { OutboxModule } from './outbox.module';
import { OutboxDrainProcessor } from './outbox-drain.processor';
import { OrdersService } from './orders.service';

const connection = {
  host: process.env.REDIS_HOST ?? 'localhost',
  port: 6379,
};

@Module({})
export class AppModule {
  static forRoot(outbox: Outbox): DynamicModule {
    return {
      module: AppModule,
      imports: [
        OutboxModule.forRoot(outbox),

        // The queues your app actually uses. Nothing here mentions the outbox
        // — they are instrumented because `main.ts` set `BullModule.queueClass`
        // before any of this was constructed.
        BullModule.forRoot({ connection }),
        BullModule.registerQueue({ name: 'invoices' }, { name: 'emails' }),

        // The scheduler lives on its own Redis, so it survives the failure it
        // is there to recover from.
        BullModule.forRoot('outbox', {
          connection: {
            host: process.env.OUTBOX_REDIS_HOST ?? 'localhost',
            port: 6380,
          },
        }),
        BullModule.registerQueue({ name: 'outbox-drain', configKey: 'outbox' }),
      ],
      providers: [OrdersService, OutboxDrainProcessor],
    };
  }
}
