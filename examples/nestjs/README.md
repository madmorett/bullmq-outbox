# NestJS

With `@nestjs/bullmq` you never write `new Queue()` — `BullModule.registerQueue()`
builds it and you get it through `@InjectQueue()`. So there is no instance to
hand to `wrapQueue`.

Use `outbox.queueClass()` instead. Both `@nestjs/bullmq` and
`@taskforcesh/nestjs-bullmq-pro` expose a `BullModule.queueClass` setter for
substituting the queue class — it is the documented way to swap in `QueuePro`,
and it takes any subclass:

```ts
BullModule.queueClass = outbox.queueClass(Queue);
```

**Set it in `main.ts`, before the app bootstraps.** Every queue in the app is
then covered — including the ones a teammate adds next year without reading
this. That is the real argument for doing it here instead of wrapping each
injected queue: coverage stops depending on anyone remembering.

## Files

| | |
|---|---|
| [`main.ts`](main.ts) | Where `queueClass` is set. The one line that matters. |
| [`outbox.module.ts`](outbox.module.ts) | Global module providing the `Outbox` and your store. |
| [`outbox-store.provider.ts`](outbox-store.provider.ts) | The store, built from `ConfigService`. |
| [`outbox-drain.processor.ts`](outbox-drain.processor.ts) | A repeatable job that calls `flush()`. |
| [`orders.service.ts`](orders.service.ts) | An ordinary service. Unchanged — that is the point. |

## Order of operations

`BullModule.queueClass` is read when queue providers are created, so it must
be set before `NestFactory.create()`. Setting it inside a module's
`onModuleInit` is too late — the queues already exist.

The `Outbox` instance that `queueClass()` closes over is created in `main.ts`
and handed to the module with `forRoot()`, so there is exactly one, and the
one that captures jobs is the one that drains them.

## Draining

The drain must not depend on the Redis it is recovering from. Two options:

- **A second Redis** — a small instance holding only the scheduler. This is
  what [`outbox-drain.processor.ts`](outbox-drain.processor.ts) shows, and it
  is what runs in production at Monest.
- **`@nestjs/schedule`** — a plain `@Cron()`, no Redis at all. Simpler, but it
  runs on every replica, so either accept concurrent drains (make your store
  claim rows — see the note at the bottom of `postgres-store.ts`) or use a
  leader lock.
