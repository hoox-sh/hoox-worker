/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

import { DurableObject } from "cloudflare:workers";

interface RateLimitEntry {
  count: number;
  resetAt: number; // epoch ms
}

/** Single counter key per DO instance (one instance per rate-limit key via idFromName). */
const COUNTER_KEY = "c";

/**
 * RateLimiterStore — Durable Object for atomic trade rate limiting.
 *
 * One DO instance is created per rate-limit key (`idFromName(key)`). Requests to
 * the same instance are serialized by the DO input gate; we additionally use
 * `blockConcurrencyWhile` for defense-in-depth against hibernation re-entry.
 *
 * Prefer this over KV get-then-put when multi-isolate correctness is required
 * (gateway MAX_TRADES_PER_MINUTE must not fail open under concurrent bursts).
 */
export class RateLimiterStore extends DurableObject {
  /**
   * Atomically check-and-increment the counter for this DO's key.
   *
   * @param maxRequests — Maximum allowed requests in the window
   * @param windowSeconds — Window length in seconds
   * @returns `true` if the request is allowed, `false` if rate limited
   */
  async checkAndIncrement(
    maxRequests: number,
    windowSeconds: number
  ): Promise<boolean> {
    const run = async (): Promise<boolean> => {
      const now = Date.now();
      const entry = await this.ctx.storage.get<RateLimitEntry>(COUNTER_KEY);

      if (!entry || now > entry.resetAt) {
        await this.ctx.storage.put(COUNTER_KEY, {
          count: 1,
          resetAt: now + windowSeconds * 1000,
        });
        return true;
      }

      if (entry.count >= maxRequests) {
        return false;
      }

      await this.ctx.storage.put(COUNTER_KEY, {
        count: entry.count + 1,
        resetAt: entry.resetAt,
      });
      return true;
    };

    const block = (
      this.ctx as DurableObjectState & {
        blockConcurrencyWhile?: <T>(fn: () => Promise<T>) => Promise<T>;
      }
    ).blockConcurrencyWhile;

    if (typeof block === "function") {
      return block.call(this.ctx, run);
    }
    return run();
  }

  /**
   * Read current counter state (testing / diagnostics).
   */
  async getState(): Promise<RateLimitEntry | null> {
    const entry = await this.ctx.storage.get<RateLimitEntry>(COUNTER_KEY);
    return entry ?? null;
  }

  /**
   * Clear counter (testing / admin).
   */
  async clear(): Promise<void> {
    const storage = this.ctx.storage as DurableObjectStorage & {
      deleteAll?: () => Promise<void>;
    };
    if (typeof storage.deleteAll === "function") {
      await storage.deleteAll();
      return;
    }
    await storage.delete(COUNTER_KEY);
  }
}
