/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

import type { KVNamespace } from "@cloudflare/workers-types";
import { createLogger } from "@hoox-sh/hoox-shared/middleware";

const logger = createLogger({ service: "hoox", module: "ipAllowlist" });

const TRADINGVIEW_ALLOWED_IPS = new Set([
  "52.89.214.238",
  "34.212.75.30",
  "54.218.53.128",
  "52.32.178.7",
]);

const KV_IP_CHECK_ENABLED_KEY = "webhook:tradingview:ip_check_enabled";
const KV_ALLOWED_IPS_KEY = "webhook:tradingview:allowed_ips";

const MAX_CUSTOM_IPS = 500;
const MAX_IP_LEN = 64;

/**
 * Runtime-validated custom allowlist from KV.
 * Rejects non-arrays, oversized lists, non-string entries, and
 * prototype-pollution style tokens (`__proto__`, `constructor`).
 */
function parseAllowedIps(raw: unknown): string[] | null {
  if (!Array.isArray(raw) || raw.length === 0 || raw.length > MAX_CUSTOM_IPS) {
    return null;
  }
  const ips: string[] = [];
  for (const entry of raw) {
    if (typeof entry !== "string") continue;
    const ip = entry.trim();
    if (
      ip.length === 0 ||
      ip.length > MAX_IP_LEN ||
      ip.includes("__proto__") ||
      ip.includes("constructor") ||
      ip.includes("prototype")
    ) {
      continue;
    }
    ips.push(ip);
  }
  return ips.length > 0 ? ips : null;
}

export interface IpCheckConfig {
  enabled: boolean;
  allowedIps: Set<string>;
}

export async function checkIpAllowlist(
  kv: KVNamespace | undefined,
  clientIp: string | null | undefined
): Promise<{
  allowed: boolean;
  reason?: string;
  config: IpCheckConfig;
}> {
  const defaultConfig: IpCheckConfig = {
    enabled: true,
    allowedIps: TRADINGVIEW_ALLOWED_IPS,
  };

  if (!clientIp) {
    return {
      allowed: false,
      reason: "No client IP provided",
      config: defaultConfig,
    };
  }

  try {
    const config = await loadIpConfig(kv);
    if (!config.enabled) {
      return { allowed: true, config };
    }

    if (config.allowedIps.has(clientIp)) {
      return { allowed: true, config };
    }

    return {
      allowed: false,
      reason: `IP ${clientIp} not in allowlist`,
      config,
    };
  } catch (error: unknown) {
    logger.error("Error checking IP allowlist", { error });
    return {
      allowed: false,
      reason: String(error),
      config: defaultConfig,
    };
  }
}

export async function loadIpConfig(
  kv: KVNamespace | undefined
): Promise<IpCheckConfig> {
  let ipCheckEnabled = true;
  let allowedIps = new Set(TRADINGVIEW_ALLOWED_IPS);

  if (!kv) {
    return { enabled: ipCheckEnabled, allowedIps };
  }

  try {
    // Parallel KV reads to shave critical-path latency
    const [kvValue, customIpsStr] = await Promise.all([
      kv.get(KV_IP_CHECK_ENABLED_KEY),
      kv.get(KV_ALLOWED_IPS_KEY),
    ]);

    if (kvValue !== null && kvValue !== undefined) {
      ipCheckEnabled = kvValue.toLowerCase() === "true";
    }

    if (customIpsStr) {
      try {
        const raw: unknown = JSON.parse(customIpsStr);
        const parsed = parseAllowedIps(raw);
        if (parsed) {
          allowedIps = new Set(parsed);
        } else {
          logger.error("Invalid IP allowlist payload in KV (ignored)");
        }
      } catch (parseError) {
        logger.error("Error parsing IP config JSON from KV", {
          error: parseError,
        });
      }
    }
  } catch (e) {
    logger.error("Error loading IP config from KV", { error: e });
  }

  return { enabled: ipCheckEnabled, allowedIps };
}

export function getDefaultAllowedIps(): Set<string> {
  return new Set(TRADINGVIEW_ALLOWED_IPS);
}
