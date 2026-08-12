/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, expect, test, beforeEach } from "bun:test";
import { IdempotencyStore } from "../src/idempotencyStore";

type StoredEntry = {
  storedAt: number;
  status: "pending" | "committed";
  expiresAt: number;
};

function createMockCtx(options?: {
  withBlockConcurrency?: boolean;
  onSetAlarm?: (t: number) => void;
  seed?: Map<string, StoredEntry>;
}) {
  const mockStorage = options?.seed ?? new Map<string, StoredEntry>();
  const mockCtx: {
    storage: Record<string, unknown>;
    id: { toString: () => string; name: string };
    waitUntil: (p: Promise<unknown>) => void;
    blockConcurrencyWhile?: <T>(fn: () => Promise<T>) => Promise<T>;
  } = {
    storage: {
      get: async <T>(key: string): Promise<T | undefined> =>
        mockStorage.get(key) as T | undefined,
      put: async <T>(key: string, value: T): Promise<void> => {
        mockStorage.set(key, value as StoredEntry);
      },
      delete: async (key: string | string[]): Promise<boolean | number> => {
        if (Array.isArray(key)) {
          let count = 0;
          for (const k of key) {
            if (mockStorage.delete(k)) count++;
          }
          return count;
        }
        return mockStorage.delete(key);
      },
      list: async <T>(opts?: {
        prefix?: string;
      }): Promise<Map<string, T>> => {
        const result = new Map<string, T>();
        for (const [k, v] of mockStorage) {
          if (!opts?.prefix || k.startsWith(opts.prefix)) {
            result.set(k, v as T);
          }
        }
        return result;
      },
      getAlarm: async (): Promise<number | null> => null,
      setAlarm: async (scheduledTime: number): Promise<void> => {
        options?.onSetAlarm?.(scheduledTime);
      },
    },
    id: { toString: () => "test-do-id", name: "test-do" },
    waitUntil: (_promise: Promise<unknown>) => {},
  };

  // Mock blockConcurrencyWhile to run the callback immediately when present
  if (options?.withBlockConcurrency !== false) {
    mockCtx.blockConcurrencyWhile = async <T>(
      fn: () => Promise<T>
    ): Promise<T> => fn();
  }

  return { mockCtx, mockStorage };
}

describe("IdempotencyStore", () => {
  let store: IdempotencyStore;
  let mockStorage: Map<string, StoredEntry>;

  beforeEach(() => {
    const { mockCtx, mockStorage: storage } = createMockCtx();
    mockStorage = storage;
    store = new IdempotencyStore(mockCtx as any);
  });

  // --------------------------------------------------------------------------
  // reserve / commit / release (two-phase API)
  // --------------------------------------------------------------------------

  test("reserve() returns ok for new key and stores pending", async () => {
    const result = await store.reserve("new-key");
    expect(result).toEqual({ ok: true, status: "new" });
    const entry = mockStorage.get("new-key");
    expect(entry?.status).toBe("pending");
    expect(entry?.expiresAt).toBeGreaterThan(Date.now());
  });

  test("reserve() returns duplicate for pending within TTL", async () => {
    expect(await store.reserve("dup-pending", 60_000)).toEqual({
      ok: true,
      status: "new",
    });
    expect(await store.reserve("dup-pending", 60_000)).toEqual({
      ok: false,
      status: "duplicate",
    });
  });

  test("reserve() returns duplicate for committed within TTL", async () => {
    expect(await store.reserve("dup-committed", 60_000)).toEqual({
      ok: true,
      status: "new",
    });
    await store.commit("dup-committed");
    expect(await store.reserve("dup-committed", 60_000)).toEqual({
      ok: false,
      status: "duplicate",
    });
  });

  test("reserve() allows re-reserve after release", async () => {
    expect(await store.reserve("rel-key", 60_000)).toEqual({
      ok: true,
      status: "new",
    });
    await store.release("rel-key");
    expect(await store.reserve("rel-key", 60_000)).toEqual({
      ok: true,
      status: "new",
    });
  });

  test("reserve() allows re-reserve after TTL expires", async () => {
    // ttlMs=0 → expiresAt <= now on next check
    expect(await store.reserve("ttl-key", 0)).toEqual({
      ok: true,
      status: "new",
    });
    expect(await store.reserve("ttl-key", 0)).toEqual({
      ok: true,
      status: "new",
    });
  });

  test("commit() marks entry committed and refreshes storedAt", async () => {
    await store.reserve("commit-key", 60_000);
    const before = mockStorage.get("commit-key")!;
    await store.commit("commit-key");
    const after = mockStorage.get("commit-key")!;
    expect(after.status).toBe("committed");
    expect(after.expiresAt).toBe(before.expiresAt);
    expect(after.storedAt).toBeGreaterThanOrEqual(before.storedAt);
  });

  test("commit() is a no-op for missing key", async () => {
    await expect(store.commit("missing")).resolves.toBeUndefined();
  });

  test("release() deletes the key", async () => {
    await store.reserve("del-key", 60_000);
    await store.release("del-key");
    expect(mockStorage.has("del-key")).toBe(false);
  });

  test("reserve() respects custom ttlMs via expiresAt", async () => {
    const before = Date.now();
    await store.reserve("ttl-custom", 10_000);
    const entry = mockStorage.get("ttl-custom")!;
    expect(entry.expiresAt).toBeGreaterThanOrEqual(before + 10_000 - 50);
    expect(entry.expiresAt).toBeLessThanOrEqual(Date.now() + 10_000 + 50);
  });

  test("reserve() works when blockConcurrencyWhile is missing", async () => {
    const { mockCtx } = createMockCtx({ withBlockConcurrency: false });
    // Explicitly omit blockConcurrencyWhile
    delete mockCtx.blockConcurrencyWhile;
    const s = new IdempotencyStore(mockCtx as any);
    const result = await s.reserve("no-block");
    expect(result).toEqual({ ok: true, status: "new" });
  });

  // --------------------------------------------------------------------------
  // Legacy checkAndStore
  // --------------------------------------------------------------------------

  test("checkAndStore() returns true for new key", async () => {
    const result = await store.checkAndStore("new-key-legacy");
    expect(result).toBe(true);
    expect(mockStorage.get("new-key-legacy")?.status).toBe("committed");
  });

  test("checkAndStore() respects ttlMs parameter", async () => {
    const result = await store.checkAndStore("key-ttl", 7200);
    expect(result).toBe(true);
  });

  test("checkAndStore() returns true for different keys", async () => {
    await store.checkAndStore("first-key");
    const result = await store.checkAndStore("second-key");
    expect(result).toBe(true);
  });

  test("checkAndStore() returns false for duplicate within TTL", async () => {
    expect(await store.checkAndStore("dup-key", 60_000)).toBe(true);
    expect(await store.checkAndStore("dup-key", 60_000)).toBe(false);
  });

  test("checkAndStore() allows re-store after TTL expires", async () => {
    expect(await store.checkAndStore("ttl-legacy", 0)).toBe(true);
    expect(await store.checkAndStore("ttl-legacy", 0)).toBe(true);
  });

  // --------------------------------------------------------------------------
  // expired / clear / alarm
  // --------------------------------------------------------------------------

  test("expired() returns true for unknown key", async () => {
    const result = await store.expired("nonexistent-key");
    expect(result).toBe(true);
  });

  test("expired() returns false for recently reserved key", async () => {
    await store.reserve("fresh-key");
    const result = await store.expired("fresh-key");
    expect(result).toBe(false);
  });

  test("clear() removes all stored keys", async () => {
    await store.reserve("key-a");
    await store.reserve("key-b");
    await store.clear();
    const expiredA = await store.expired("key-a");
    const expiredB = await store.expired("key-b");
    expect(expiredA).toBe(true);
    expect(expiredB).toBe(true);
  });

  test("alarm() deletes expired entries and keeps fresh ones", async () => {
    await store.reserve("fresh-alarm", 300_000);
    const storage = (store as any).ctx.storage;
    await storage.put("stale-alarm", {
      storedAt: Date.now() - 400_000,
      status: "committed",
      expiresAt: Date.now() - 100_000,
    });

    await store.alarm();

    expect(await store.expired("stale-alarm")).toBe(true);
    expect(await store.expired("fresh-alarm")).toBe(false);
  });

  test("alarm() expires pending entries by expiresAt", async () => {
    const storage = (store as any).ctx.storage;
    await storage.put("stale-pending", {
      storedAt: Date.now() - 10_000,
      status: "pending",
      expiresAt: Date.now() - 1,
    });
    await store.alarm();
    expect(mockStorage.has("stale-pending")).toBe(false);
  });

  test("alarm() schedules next cleanup when entries remain", async () => {
    let alarmSetTo: number | null = null;
    const seed = new Map<string, StoredEntry>();
    seed.set("keep-me", {
      storedAt: Date.now(),
      status: "committed",
      expiresAt: Date.now() + 300_000,
    });
    const { mockCtx } = createMockCtx({
      seed,
      onSetAlarm: (t) => {
        alarmSetTo = t;
      },
    });
    const s = new IdempotencyStore(mockCtx as any);
    await s.alarm();
    expect(alarmSetTo).not.toBeNull();
    expect(alarmSetTo!).toBeGreaterThan(Date.now() - 1000);
  });

  test("clear() is a no-op when empty", async () => {
    await expect(store.clear()).resolves.toBeUndefined();
  });
});
