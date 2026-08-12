/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Rate limiter with optional Durable Object (atomic) and KV persistence.
 *
 * Priority when checking a key:
 *   1. Durable Object (`opts.rateLimiter`) — atomic multi-isolate (preferred)
 *   2. KV namespace — best-effort shared state (last-write-wins under races)
 *   3. In-memory Map — per-isolate only (local tests / cold-start fallback)
 *
 * KV get-then-put cannot guarantee strict limits under concurrent isolates;
 * use RateLimiterStore DO in production for trade-path MAX_TRADES_PER_MINUTE.
 */

import type {
  DurableObjectNamespace,
  KVNamespace,
} from "@cloudflare/workers-types";

import type { RateLimiterStore } from "./rateLimiterStore";

const DEFAULT_MAX_REQUESTS = 10;
const DEFAULT_WINDOW_SECONDS = 60;
const KV_PREFIX = "ratelimit:";
/** Prune expired in-memory entries once the map grows past this size. */
const MEM_PRUNE_THRESHOLD = 256;

interface RateLimitEntry {
  count: number;
  resetAt: number; // epoch ms
}

export interface RateLimiterConfig {
  maxRequests?: number;
  windowSeconds?: number;
  /**
   * Optional RateLimiterStore DO namespace. When set, uses atomic
   * checkAndIncrement (one DO instance per key via idFromName).
   */
  rateLimiter?: DurableObjectNamespace | null;
}

/**
 * Check if a key has exceeded its rate limit.
 *
 * @param kv  — KV namespace (optional; falls back to in-memory Map)
 * @param key — Unique rate limit key (e.g. session ID, API key)
 * @param opts — Optional overrides for maxRequests / windowSeconds / DO
 * @returns `true` if request is allowed, `false` if rate limited
 */
export async function checkRateLimit(
  kv: KVNamespace | null,
  key: string,
  opts: RateLimiterConfig = {}
): Promise<boolean> {
  const maxRequests = opts.maxRequests ?? DEFAULT_MAX_REQUESTS;
  const windowSeconds = opts.windowSeconds ?? DEFAULT_WINDOW_SECONDS;
  const now = Date.now();

  if (opts.rateLimiter) {
    return checkDoRateLimit(
      opts.rateLimiter,
      key,
      maxRequests,
      windowSeconds
    );
  }

  if (kv) {
    return checkKvRateLimit(kv, key, maxRequests, windowSeconds, now);
  }

  return checkMemoryRateLimit(key, maxRequests, windowSeconds, now);
}

// ---- Durable Object (atomic, multi-isolate) ----

async function checkDoRateLimit(
  ns: DurableObjectNamespace,
  key: string,
  maxRequests: number,
  windowSeconds: number
): Promise<boolean> {
  const id = ns.idFromName(key);
  const stub = ns.get(id) as unknown as RateLimiterStore;
  return stub.checkAndIncrement(maxRequests, windowSeconds);
}

// ---- In-memory fallback (per-isolation, resets on cold start) ----

const memMap = new Map<string, RateLimitEntry>();

function pruneMemMap(now: number): void {
  if (memMap.size < MEM_PRUNE_THRESHOLD) return;
  for (const [k, entry] of memMap) {
    if (now > entry.resetAt) {
      memMap.delete(k);
    }
  }
}

function checkMemoryRateLimit(
  key: string,
  maxRequests: number,
  windowSeconds: number,
  now: number
): boolean {
  pruneMemMap(now);

  const entry = memMap.get(key);
  if (!entry || now > entry.resetAt) {
    memMap.set(key, { count: 1, resetAt: now + windowSeconds * 1000 });
    return true;
  }
  if (entry.count >= maxRequests) return false;
  entry.count++;
  return true;
}

/** Test helper: clear in-memory rate limit state. */
export function _resetMemoryRateLimiterForTests(): void {
  memMap.clear();
}

// ---- KV-backed (persistent, best-effort across instances) ----

/**
 * KV get-then-put rate limiting is best-effort under multi-isolate concurrency
 * (last-write-wins). For strictly atomic limits use RateLimiterStore DO.
 *
 * Fail-closed: after write we re-read; if the observed count exceeds max,
 * reject this request. Concurrent last-slot races can still under-count
 * slightly — production trade path should bind RATE_LIMITER.
 */
async function checkKvRateLimit(
  kv: KVNamespace,
  key: string,
  maxRequests: number,
  windowSeconds: number,
  now: number
): Promise<boolean> {
  const kvKey = KV_PREFIX + key;
  const stored = await kv.get<RateLimitEntry>(kvKey, "json");
  const ttlOpts = { expirationTtl: windowSeconds + 5 }; // buffer for clock skew

  if (!stored || now > stored.resetAt) {
    const entry: RateLimitEntry = {
      count: 1,
      resetAt: now + windowSeconds * 1000,
    };
    await kv.put(kvKey, JSON.stringify(entry), ttlOpts);
    return true;
  }

  if (stored.count >= maxRequests) return false;

  const next: RateLimitEntry = {
    count: stored.count + 1,
    resetAt: stored.resetAt,
  };
  await kv.put(kvKey, JSON.stringify(next), ttlOpts);

  // Fail closed on concurrent overshoot when re-read (or local next) exceeds max.
  if (next.count > maxRequests) return false;

  const after = await kv.get<RateLimitEntry>(kvKey, "json");
  if (after && after.count > maxRequests) return false;

  return true;
}
