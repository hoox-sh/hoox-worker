/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, expect, test, beforeEach } from "bun:test";
import { checkRateLimit } from "../src/rateLimiter";

describe("checkRateLimit - in-memory fallback", () => {
  const key = `mem-${Date.now()}-${Math.random()}`;

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
});
