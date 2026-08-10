/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, expect, test, beforeEach } from "bun:test";
import { IdempotencyStore } from "../src/idempotencyStore";

describe("IdempotencyStore", () => {
  let store: IdempotencyStore;

  beforeEach(() => {
    // DurableObject constructor expects a ctx object with storage
    const mockStorage = new Map<string, { storedAt: number }>();
    const mockCtx = {
      storage: {
        get: async <T>(key: string): Promise<T | undefined> =>
          mockStorage.get(key) as T | undefined,
        put: async <T>(key: string, value: T): Promise<void> => {
          mockStorage.set(key, value as { storedAt: number });
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
        setAlarm: async (_scheduledTime: number): Promise<void> => {},
      },
      id: { toString: () => "test-do-id", name: "test-do" },
      waitUntil: (_promise: Promise<unknown>) => {},
    };
    store = new IdempotencyStore(mockCtx as any);
  });

  test("checkAndStore() returns true for new key", async () => {
    const result = await store.checkAndStore("new-key");
    expect(result).toBe(true);
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

  test("expired() returns true for unknown key", async () => {
    const result = await store.expired("nonexistent-key");
    expect(result).toBe(true);
  });

  test("expired() returns false for recently stored key", async () => {
    await store.checkAndStore("fresh-key");
    const result = await store.expired("fresh-key");
    expect(result).toBe(false);
  });

  test("clear() removes all stored keys", async () => {
    await store.checkAndStore("key-a");
    await store.checkAndStore("key-b");
    await store.clear();
    const expiredA = await store.expired("key-a");
    const expiredB = await store.expired("key-b");
    expect(expiredA).toBe(true);
    expect(expiredB).toBe(true);
  });

  test("checkAndStore() returns false for duplicate within TTL", async () => {
    expect(await store.checkAndStore("dup-key", 60_000)).toBe(true);
    expect(await store.checkAndStore("dup-key", 60_000)).toBe(false);
  });

  test("checkAndStore() allows re-store after TTL expires", async () => {
    // Store with zero TTL so next check immediately sees it as expired
    expect(await store.checkAndStore("ttl-key", 0)).toBe(true);
    // Existing entry is outside TTL window (ttlMs=0) → treated as new
    expect(await store.checkAndStore("ttl-key", 0)).toBe(true);
  });

  test("alarm() deletes expired entries and keeps fresh ones", async () => {
    // Inject an expired entry and a fresh one via checkAndStore + direct put
    await store.checkAndStore("fresh-alarm", 300_000);
    // Manually put an expired entry into storage
    const storage = (store as any).ctx.storage;
    await storage.put("stale-alarm", { storedAt: Date.now() - 400_000 });

    await store.alarm();

    expect(await store.expired("stale-alarm")).toBe(true);
    expect(await store.expired("fresh-alarm")).toBe(false);
  });

  test("alarm() schedules next cleanup when entries remain", async () => {
    let alarmSetTo: number | null = null;
    const mockStorage = new Map<string, { storedAt: number }>();
    mockStorage.set("keep-me", { storedAt: Date.now() });
    const mockCtx = {
      storage: {
        get: async <T>(key: string): Promise<T | undefined> =>
          mockStorage.get(key) as T | undefined,
        put: async <T>(key: string, value: T): Promise<void> => {
          mockStorage.set(key, value as { storedAt: number });
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
        list: async <T>(): Promise<Map<string, T>> => {
          return new Map(mockStorage as Map<string, T>);
        },
        getAlarm: async (): Promise<number | null> => null,
        setAlarm: async (scheduledTime: number): Promise<void> => {
          alarmSetTo = scheduledTime;
        },
      },
      id: { toString: () => "alarm-do", name: "alarm-do" },
      waitUntil: (_p: Promise<unknown>) => {},
    };
    const s = new IdempotencyStore(mockCtx as any);
    await s.alarm();
    expect(alarmSetTo).not.toBeNull();
    expect(alarmSetTo!).toBeGreaterThan(Date.now() - 1000);
  });

  test("clear() is a no-op when empty", async () => {
    await expect(store.clear()).resolves.toBeUndefined();
  });
});
