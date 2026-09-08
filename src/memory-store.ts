import type { OutboxEntry, OutboxStore } from './types';

/**
 * An in-process store, for tests and for seeing the pattern work before you
 * write the real one.
 *
 * It is not durable — the process that holds the jobs is the process that
 * loses them. Never ship this.
 */
export class MemoryOutboxStore implements OutboxStore {
  private readonly entries = new Map<string, OutboxEntry>();
  private readonly dead = new Map<string, OutboxEntry>();

  async save(entry: OutboxEntry): Promise<void> {
    this.entries.set(entry.id, { ...entry });
  }

  async loadPending(limit: number): Promise<OutboxEntry[]> {
    return [...this.entries.values()]
      .sort((a, b) => a.createdAt.localeCompare(b.createdAt))
      .slice(0, limit)
      .map((entry) => ({ ...entry }));
  }

  async markProcessed(id: string): Promise<void> {
    this.entries.delete(id);
  }

  async markFailed(failure: {
    id: string;
    error: string;
    attempts: number;
    expired: boolean;
  }): Promise<void> {
    const entry = this.entries.get(failure.id);
    if (!entry) return;

    const updated = {
      ...entry,
      attempts: failure.attempts,
      lastError: failure.error,
    };

    if (failure.expired) {
      this.entries.delete(failure.id);
      this.dead.set(failure.id, updated);
      return;
    }

    this.entries.set(failure.id, updated);
  }

  /** Entries still awaiting replay. Test helper. */
  pending(): OutboxEntry[] {
    return [...this.entries.values()];
  }

  /** Entries that exhausted `maxAttempts`. Test helper. */
  expired(): OutboxEntry[] {
    return [...this.dead.values()];
  }

  clear(): void {
    this.entries.clear();
    this.dead.clear();
  }
}
