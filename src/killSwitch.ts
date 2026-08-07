/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

import type { KVNamespace } from "@cloudflare/workers-types";
import { createLogger } from "@hoox-sh/hoox-shared/middleware";
import { KVKeys } from "@hoox-sh/hoox-shared/kvKeys";

const logger = createLogger({ service: "hoox", module: "killSwitch" });

/**
 * Canonical kill-switch keys:
 * - `trade:kill_switch` — written by agent-worker, CLI (`hoox monitor`), telegram, dashboard risk.
 * - `global:kill_switch` — gateway dashboard section field (dashboard.jsonc).
 *
 * Either key set to a truthy string enables the breaker.
 */
const KILL_SWITCH_KEYS = [
  KVKeys.KV_TRADE_KILL_SWITCH,
  "global:kill_switch",
] as const;

function isTruthyFlag(value: string | null): boolean {
  if (!value) return false;
  const v = value.trim().toLowerCase();
  return v === "true" || v === "1" || v === "yes" || v === "on";
}

export async function checkKillSwitch(
  kv: KVNamespace | undefined
): Promise<{ enabled: boolean; source?: string; error?: string }> {
  try {
    if (!kv) {
      // No config store — fail open so local/dev without KV still works.
      return { enabled: false };
    }

    // Parallel reads; either key trips the breaker.
    const values = await Promise.all(
      KILL_SWITCH_KEYS.map(async (key) => ({
        key,
        value: await kv.get(key),
      }))
    );

    for (const { key, value } of values) {
      if (isTruthyFlag(value)) {
        return { enabled: true, source: key };
      }
    }

    return { enabled: false };
  } catch (error: unknown) {
    // Fail open on KV errors for availability; operators should monitor logs.
    // Auth remains fail-closed separately.
    logger.error("Error reading kill switch KV", { error });
    return { enabled: false, error: String(error) };
  }
}

export async function isTradingPaused(
  kv: KVNamespace | undefined
): Promise<boolean> {
  const result = await checkKillSwitch(kv);
  return result.enabled;
}
