/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, expect, test, beforeEach } from "bun:test";
import {
  checkRateLimit,
  _resetMemoryRateLimiterForTests,
} from "../src/rateLimiter";
import { RateLimiterStore } from "../src/rateLimiterStore";

describe("checkRateLimit - in-memory fallback", () => {
  const key = `mem-${Date.now()}-${Math.random()}`;

  beforeEach(() => {
    _resetMemoryRateLimiterForTests();
  });

  test("allows first request", async () => {
    const allowed = await checkRateLimit(null, `${key}-first`, {
      maxRequests: 2,
      windowSeconds: 60,
    });
    expect(allowed).toBe(true);
  });

  test("blocks after maxRequests within window", async () => {
    const k = `${key}-block`;
    const opts = { maxRequests: 2, windowSeconds: 60 };
    expect(await checkRateLimit(null, k, opts)).toBe(true);
    expect(await checkRateLimit(null, k, opts)).toBe(true);
    expect(await checkRateLimit(null, k, opts)).toBe(false);
  });

  test("uses default limits when opts omitted", async () => {
    const k = `${key}-defaults`;
    // Defaults: 10 / 60s — first call allowed
    expect(await checkRateLimit(null, k)).toBe(true);
  });

  test("does not exceed max under sequential burst", async () => {
    const k = `${key}-burst`;
    const max = 5;
    const opts = { maxRequests: max, windowSeconds: 60 };
    let allowed = 0;
    for (let i = 0; i < max + 10; i++) {
      if (await checkRateLimit(null, k, opts)) allowed++;
    }
    expect(allowed).toBe(max);
  });
});

describe("checkRateLimit - KV-backed", () => {
  let store: Map<string, string>;

  beforeEach(() => {
    store = new Map();
  });

  function mockKv() {
    return {
      get: async (key: string, type?: string) => {
        const raw = store.get(key);
        if (raw === undefined) return null;
        if (type === "json") return JSON.parse(raw);
        return raw;
      },
      put: async (key: string, value: string) => {
        store.set(key, value);
      },
    } as any;
  }

  test("allows first KV request and stores entry", async () => {
    const kv = mockKv();
    const allowed = await checkRateLimit(kv, "user-a", {
      maxRequests: 3,
      windowSeconds: 30,
    });
    expect(allowed).toBe(true);
    expect(store.size).toBe(1);
  });

  test("increments count and blocks at max", async () => {
    const kv = mockKv();
    const opts = { maxRequests: 2, windowSeconds: 30 };
    expect(await checkRateLimit(kv, "user-b", opts)).toBe(true);
    expect(await checkRateLimit(kv, "user-b", opts)).toBe(true);
    expect(await checkRateLimit(kv, "user-b", opts)).toBe(false);
  });

  test("resets window when resetAt is in the past", async () => {
    const kv = mockKv();
    store.set(
      "ratelimit:user-c",
      JSON.stringify({ count: 99, resetAt: Date.now() - 1000 })
    );
    const allowed = await checkRateLimit(kv, "user-c", {
      maxRequests: 1,
      windowSeconds: 60,
    });
    expect(allowed).toBe(true);
  });

  test("fail-closed when stored count already at max", async () => {
    const kv = mockKv();
    store.set(
      "ratelimit:user-d",
      JSON.stringify({ count: 3, resetAt: Date.now() + 60_000 })
    );
    expect(
      await checkRateLimit(kv, "user-d", { maxRequests: 3, windowSeconds: 60 })
    ).toBe(false);
  });

  test("fail-closed when re-read count exceeds max after concurrent write", async () => {
    // Simulate another isolate writing a higher count after our put.
    let putCount = 0;
    const kv = {
      get: async (key: string, type?: string) => {
        const raw = store.get(key);
        if (raw === undefined) return null;
        if (type === "json") return JSON.parse(raw);
        return raw;
      },
      put: async (key: string, value: string) => {
        putCount++;
        store.set(key, value);
        // After first put in this request path, another isolate overshoots
        if (putCount === 1) {
          const parsed = JSON.parse(value) as { count: number; resetAt: number };
          store.set(
            key,
            JSON.stringify({ count: parsed.count + 5, resetAt: parsed.resetAt })
          );
        }
      },
    } as any;

    store.set(
      "ratelimit:user-e",
      JSON.stringify({ count: 2, resetAt: Date.now() + 60_000 })
    );
    // max=3: we try to go 2→3, but concurrent write makes count 8 → fail closed
    const allowed = await checkRateLimit(kv, "user-e", {
      maxRequests: 3,
      windowSeconds: 60,
    });
    expect(allowed).toBe(false);
  });
});

describe("checkRateLimit - Durable Object path", () => {
  function createMockDoNamespace(store: RateLimiterStore) {
    return {
      idFromName: (name: string) => ({ name, toString: () => name }),
      get: () => store,
    } as any;
  }

  function createStore(): RateLimiterStore {
    const mockStorage = new Map<string, unknown>();
    const mockCtx = {
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
      id: { toString: () => "rate-do", name: "rate-do" },
      waitUntil: (_p: Promise<unknown>) => {},
      blockConcurrencyWhile: async <T>(fn: () => Promise<T>): Promise<T> =>
        fn(),
    };
    return new RateLimiterStore(mockCtx as any);
  }

  test("prefers DO over KV when rateLimiter binding present", async () => {
    const doStore = createStore();
    const ns = createMockDoNamespace(doStore);
    let kvGetCalled = false;
    const kv = {
      get: async () => {
        kvGetCalled = true;
        return null;
      },
      put: async () => {},
    } as any;

    const opts = {
      maxRequests: 2,
      windowSeconds: 60,
      rateLimiter: ns,
    };
    expect(await checkRateLimit(kv, "session:abc", opts)).toBe(true);
    expect(await checkRateLimit(kv, "session:abc", opts)).toBe(true);
    expect(await checkRateLimit(kv, "session:abc", opts)).toBe(false);
    expect(kvGetCalled).toBe(false);

    const state = await doStore.getState();
    expect(state?.count).toBe(2);
  });

  test("DO path enforces max under sequential burst", async () => {
    const doStore = createStore();
    const ns = createMockDoNamespace(doStore);
    const max = 10;
    const opts = {
      maxRequests: max,
      windowSeconds: 60,
      rateLimiter: ns,
    };
    let allowed = 0;
    for (let i = 0; i < max + 20; i++) {
      if (await checkRateLimit(null, "session:burst", opts)) allowed++;
    }
    expect(allowed).toBe(max);
  });
});
