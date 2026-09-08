/**
 * The one line that matters, and where it has to go.
 */
import { NestFactory } from '@nestjs/core';
import { BullModule } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { createOutbox } from 'bullmq-outbox';
import { AppModule } from './app.module';
import { createPostgresOutboxStore } from '../postgres-store';

async function bootstrap() {
  const outbox = createOutbox({
    store: createPostgresOutboxStore(pool),
    maxAttempts: 10,

    // Real-time queues are better off dropping a job than replaying it later.
    shouldCapture: ({ queueName }) => !queueName.startsWith('live-'),

    onJobSaved: (event) =>
      console.warn(`[outbox] stored ${event.jobName} from ${event.queueName}`),
    onJobRequeued: (event) =>
      console.log(`[outbox] recovered ${event.jobName} after ${event.ageMs}ms`),
  });

  // Must run BEFORE NestFactory.create(): queue providers read this when they
  // are constructed, so setting it from a module's onModuleInit is too late.
  //
  // Using BullMQ Pro? Import BullModule from '@taskforcesh/nestjs-bullmq-pro'
  // and QueuePro from '@taskforcesh/bullmq-pro' — same line, same behaviour.
  BullModule.queueClass = outbox.queueClass(Queue);

  const app = await NestFactory.create(AppModule.forRoot(outbox));
  await app.listen(3000);
}

void bootstrap();

declare const pool: import('pg').Pool;
