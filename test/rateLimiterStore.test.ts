/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, expect, test, beforeEach } from "bun:test";
import { RateLimiterStore } from "../src/rateLimiterStore";

function createMockCtx(options?: {
  withBlockConcurrency?: boolean;
}) {
  const mockStorage = new Map<string, unknown>();
  const ctx: Record<string, unknown> = {
    storage: {
      get: async <T>(key: string): Promise<T | undefined> =>
        mockStorage.get(key) as T | undefined,
      put: async <T>(key: string, value: T): Promise<void> => {
        mockStorage.set(key, value);
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
      list: async <T>(): Promise<Map<string, T>> =>
        new Map(mockStorage as Map<string, T>),
      getAlarm: async (): Promise<number | null> => null,
      setAlarm: async (): Promise<void> => {},
      deleteAll: async (): Promise<void> => {
        mockStorage.clear();
      },
    },
    id: { toString: () => "test-rate-do", name: "test-rate-do" },
    waitUntil: (_p: Promise<unknown>) => {},
  };

  if (options?.withBlockConcurrency !== false) {
    ctx.blockConcurrencyWhile = async <T>(fn: () => Promise<T>): Promise<T> =>
      fn();
  }

  return { ctx, mockStorage };
}

describe("RateLimiterStore", () => {
  let store: RateLimiterStore;

  beforeEach(() => {
    const { ctx } = createMockCtx();
    store = new RateLimiterStore(ctx as any);
  });

  test("checkAndIncrement allows first request", async () => {
    expect(await store.checkAndIncrement(3, 60)).toBe(true);
    const state = await store.getState();
    expect(state?.count).toBe(1);
  });

  test("checkAndIncrement blocks at maxRequests", async () => {
    expect(await store.checkAndIncrement(2, 60)).toBe(true);
    expect(await store.checkAndIncrement(2, 60)).toBe(true);
    expect(await store.checkAndIncrement(2, 60)).toBe(false);
    const state = await store.getState();
    expect(state?.count).toBe(2);
  });

  test("checkAndIncrement never exceeds max under sequential load", async () => {
    const max = 10;
    let allowed = 0;
    for (let i = 0; i < 50; i++) {
      if (await store.checkAndIncrement(max, 60)) allowed++;
    }
    expect(allowed).toBe(max);
    expect((await store.getState())?.count).toBe(max);
  });

  test("checkAndIncrement resets after window expires", async () => {
    const { ctx, mockStorage } = createMockCtx();
    const s = new RateLimiterStore(ctx as any);
    mockStorage.set("c", { count: 99, resetAt: Date.now() - 1 });
    expect(await s.checkAndIncrement(1, 60)).toBe(true);
    expect((await s.getState())?.count).toBe(1);
  });

  test("getState returns null when empty", async () => {
    expect(await store.getState()).toBeNull();
  });

  test("clear removes counter", async () => {
    await store.checkAndIncrement(5, 60);
    await store.clear();
    expect(await store.getState()).toBeNull();
    expect(await store.checkAndIncrement(5, 60)).toBe(true);
  });

  test("works without blockConcurrencyWhile (fallback path)", async () => {
    const { ctx } = createMockCtx({ withBlockConcurrency: false });
    const s = new RateLimiterStore(ctx as any);
    expect(await s.checkAndIncrement(1, 60)).toBe(true);
    expect(await s.checkAndIncrement(1, 60)).toBe(false);
  });

  test("blockConcurrencyWhile is invoked when present", async () => {
    let blockCalls = 0;
    const { ctx } = createMockCtx({ withBlockConcurrency: false });
    (ctx as any).blockConcurrencyWhile = async <T>(
      fn: () => Promise<T>
    ): Promise<T> => {
      blockCalls++;
      return fn();
    };
    const s = new RateLimiterStore(ctx as any);
    await s.checkAndIncrement(5, 60);
    expect(blockCalls).toBe(1);
  });
});
