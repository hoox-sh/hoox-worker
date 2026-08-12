/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

import { DurableObject } from "cloudflare:workers";

const DEFAULT_TTL_MS = 300_000; // 5 minutes

/** Entry status: pending = reserved in-flight; committed = execution accepted. */
export type IdempotencyStatus = "pending" | "committed";

interface StoredEntry {
  storedAt: number;
  status: IdempotencyStatus;
  /** Absolute expiry time (ms since epoch). Alarm and reserve use this for TTL. */
  expiresAt: number;
}

export type ReserveResult =
  | { ok: true; status: "new" }
  | { ok: false; status: "duplicate" };

/**
 * IdempotencyStore — Durable Object for exactly-once trade execution.
 *
 * Two-phase protocol:
 *   reserve → (execute) → commit on success | release on hard failure
 *
 * Pending keys block concurrent in-flight retries; committed keys block
 * re-execution within the TTL. Release deletes the key so retries can proceed
 * when the first attempt never successfully queued/executed.
 */
export class IdempotencyStore extends DurableObject {
  /**
   * Reserve an idempotency key (phase 1).
   *
   * - committed within TTL → duplicate
   * - pending within TTL → duplicate (in-flight)
   * - else put { status: "pending", expiresAt }, schedule alarm
   */
  async reserve(
    key: string,
    ttlMs: number = DEFAULT_TTL_MS
  ): Promise<ReserveResult> {
    const run = async (): Promise<ReserveResult> => {
      const now = Date.now();
      const existing = await this.ctx.storage.get<StoredEntry>(key);

      if (existing && now < entryExpiresAt(existing)) {
        return { ok: false, status: "duplicate" };
      }

      const expiresAt = now + ttlMs;
      await this.ctx.storage.put(key, {
        storedAt: now,
        status: "pending" as const,
        expiresAt,
      } satisfies StoredEntry);

      await this.scheduleAlarm(expiresAt);
      return { ok: true, status: "new" };
    };

    return this.withConcurrencyBlock(run);
  }

  /**
   * Commit a previously reserved key (phase 2 success).
   * Marks the entry committed and refreshes storedAt; keeps expiresAt.
   */
  async commit(key: string): Promise<void> {
    const run = async (): Promise<void> => {
      const existing = await this.ctx.storage.get<StoredEntry>(key);
      if (!existing) return;

      const now = Date.now();
      // If already past TTL, drop instead of extending a stale reservation.
      if (now >= entryExpiresAt(existing)) {
        await this.ctx.storage.delete(key);
        return;
      }

      await this.ctx.storage.put(key, {
        storedAt: now,
        status: "committed" as const,
        expiresAt: entryExpiresAt(existing),
      } satisfies StoredEntry);
    };

    return this.withConcurrencyBlock(run);
  }

  /**
   * Release a reserved key (phase 2 hard failure).
   * Deletes the key so retries can reserve again.
   */
  async release(key: string): Promise<void> {
    const run = async (): Promise<void> => {
      await this.ctx.storage.delete(key);
    };
    return this.withConcurrencyBlock(run);
  }

  /**
   * Legacy single-phase API: reserve + commit atomically.
   * Kept for tests and any callers that have not migrated.
   *
   * @returns `true` if the key was stored (new request), `false` if duplicate
   */
  async checkAndStore(
    key: string,
    ttlMs: number = DEFAULT_TTL_MS
  ): Promise<boolean> {
    const run = async (): Promise<boolean> => {
      const now = Date.now();
      const existing = await this.ctx.storage.get<StoredEntry>(key);
      if (existing && now < entryExpiresAt(existing)) {
        return false;
      }

      const expiresAt = now + ttlMs;
      await this.ctx.storage.put(key, {
        storedAt: now,
        status: "committed" as const,
        expiresAt,
      } satisfies StoredEntry);

      await this.scheduleAlarm(expiresAt);
      return true;
    };

    return this.withConcurrencyBlock(run);
  }

  /**
   * Check whether a previously stored key has expired (or is missing).
   */
  async expired(key: string): Promise<boolean> {
    const entry = await this.ctx.storage.get<StoredEntry>(key);
    if (!entry) return true;
    return Date.now() >= entryExpiresAt(entry);
  }

  /**
   * Alarm handler — cleans up expired pending and committed entries.
   * Uses each entry's expiresAt so custom TTLs are respected.
   */
  async alarm(): Promise<void> {
    const all = await this.ctx.storage.list<StoredEntry>();
    const now = Date.now();
    let earliestRemaining = Infinity;
    const expiredKeys: string[] = [];

    for (const [key, entry] of all) {
      const expiresAt = entryExpiresAt(entry);
      if (now >= expiresAt) {
        expiredKeys.push(key);
      } else {
        const remaining = expiresAt - now;
        if (remaining < earliestRemaining) {
          earliestRemaining = remaining;
        }
      }
    }

    if (expiredKeys.length > 0) {
      await this.ctx.storage.delete(expiredKeys);
    }

    if (earliestRemaining < Infinity) {
      await this.ctx.storage.setAlarm(Date.now() + earliestRemaining);
    }
  }

  /**
   * Remove all stored keys (for testing/admin).
   * Prefer deleteAll() so internal metadata and alarms are cleared as well
   * (compat date ≥ 2026-02-24 deletes alarms with storage). Falls back to
   * multi-key delete when deleteAll is unavailable (unit-test mocks).
   */
  async clear(): Promise<void> {
    const storage = this.ctx.storage as DurableObjectStorage & {
      deleteAll?: () => Promise<void>;
    };
    if (typeof storage.deleteAll === "function") {
      await storage.deleteAll();
      return;
    }
    const all = await storage.list();
    const keys = [...all.keys()];
    if (keys.length > 0) {
      await storage.delete(keys);
    }
  }

  /** Schedule alarm at expiresAt if sooner than the current alarm. */
  private async scheduleAlarm(expiresAt: number): Promise<void> {
    const currentAlarm = await this.ctx.storage.getAlarm();
    if (!currentAlarm || expiresAt < currentAlarm) {
      await this.ctx.storage.setAlarm(expiresAt);
    }
  }

  /**
   * Run storage mutations under blockConcurrencyWhile when available.
   * Unit-test mocks may omit the method; then the callback runs immediately.
   */
  private async withConcurrencyBlock<T>(fn: () => Promise<T>): Promise<T> {
    const block = (
      this.ctx as DurableObjectState & {
        blockConcurrencyWhile?: <U>(cb: () => Promise<U>) => Promise<U>;
      }
    ).blockConcurrencyWhile;

    if (typeof block === "function") {
      // Bind preserves `this`; avoid `.call` which widens Promise<T> → unknown under tsc.
      const runBlocked = block.bind(this.ctx) as <U>(
        cb: () => Promise<U>
      ) => Promise<U>;
      return runBlocked(fn);
    }
    return fn();
  }
}

/** Resolve expiresAt with fallback for legacy entries that only had storedAt. */
function entryExpiresAt(entry: StoredEntry | { storedAt: number }): number {
  if ("expiresAt" in entry && typeof entry.expiresAt === "number") {
    return entry.expiresAt;
  }
  return entry.storedAt + DEFAULT_TTL_MS;
}
