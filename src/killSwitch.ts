/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Gateway kill-switch wrapper around the shared helper.
 *
 * Policy for the gateway (webhook ingress):
 * - Missing CONFIG_KV → fail-OPEN (local/dev without KV still works)
 * - KV read errors → fail-CLOSED (safer than the historical fail-open)
 *
 * Truthy flags and both trade/global keys are handled by @hoox-sh/hoox-shared.
 */

import type { KVNamespace } from "@cloudflare/workers-types";
import {
  checkKillSwitch as sharedCheckKillSwitch,
  isTradingPaused as sharedIsTradingPaused,
  type KillSwitchResult,
} from "@hoox-sh/hoox-shared/kill-switch";

/** Gateway defaults: open when KV binding is absent, closed on read errors. */
const GATEWAY_OPTIONS = {
  onMissingKv: "open" as const,
  onReadError: "closed" as const,
};

export async function checkKillSwitch(
  kv: KVNamespace | undefined
): Promise<KillSwitchResult> {
  return sharedCheckKillSwitch(kv, GATEWAY_OPTIONS);
}

export async function isTradingPaused(
  kv: KVNamespace | undefined
): Promise<boolean> {
  return sharedIsTradingPaused(kv, GATEWAY_OPTIONS);
}
