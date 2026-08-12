/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Telegram chatId allowlist for gateway notify forwarding.
 *
 * Security: processNotification previously forwarded arbitrary chatId from an
 * authenticated webhook body. Anyone with a valid WEBHOOK_API_KEY could spam
 * arbitrary Telegram chats. This module fail-closes the notify path when no
 * allowlist is configured.
 *
 * Sources (union, any source contributes IDs):
 * - Env: TELEGRAM_ALLOWED_CHAT_IDS (preferred) or AUTHORIZED_CHAT_IDS
 *   (comma-separated, same shape as telegram-worker)
 * - CONFIG_KV key `telegram:allowed_chat_ids` — JSON array of string|number
 *
 * Policy:
 * - Empty / unconfigured allowlist → reject notify (fail-closed)
 * - Configured → chatId must be in the set
 * - Invalid chatId format (path injection, non-finite, empty) → reject
 */

import type { KVNamespace } from "@cloudflare/workers-types";
import { createLogger } from "@hoox-sh/hoox-shared/middleware";

const logger = createLogger({ service: "hoox", module: "chatIdAllowlist" });

/** CONFIG_KV key — JSON array of string|number chat IDs. */
export const KV_TELEGRAM_ALLOWED_CHAT_IDS = "telegram:allowed_chat_ids";

/** Preferred env binding (hoox gateway). */
export const ENV_TELEGRAM_ALLOWED_CHAT_IDS = "TELEGRAM_ALLOWED_CHAT_IDS";

/** Alias matching telegram-worker AUTHORIZED_CHAT_IDS. */
export const ENV_AUTHORIZED_CHAT_IDS = "AUTHORIZED_CHAT_IDS";

const MAX_ALLOWLIST_ENTRIES = 500;
/** Telegram chat IDs are signed 64-bit ints; keep a generous string cap. */
const MAX_CHAT_ID_LEN = 32;

/** Placeholder value from wrangler templates — treat as unset. */
const SECRET_PLACEHOLDER = "__SECRET__";

export type ChatIdAllowlistEnv = {
  TELEGRAM_ALLOWED_CHAT_IDS?: string | null;
  AUTHORIZED_CHAT_IDS?: string | null;
  CONFIG_KV?: KVNamespace;
};

export interface ChatIdAllowlistConfig {
  /** True when at least one allowed ID was loaded from env and/or KV. */
  configured: boolean;
  allowedIds: Set<string>;
  /** Where IDs came from (for logs/tests). */
  sources: Array<"env" | "kv">;
}

export interface ChatIdCheckResult {
  allowed: boolean;
  reason?: string;
  /** Normalized chatId string when format is valid. */
  normalized?: string;
  config: ChatIdAllowlistConfig;
}

/**
 * Normalize and validate a raw chatId from the webhook body.
 * Accepts finite numbers or non-empty strings that look like Telegram chat IDs
 * (optional leading `-`, then digits only). Rejects path/injection tokens.
 */
export function normalizeChatId(raw: unknown): string | null {
  if (typeof raw === "number") {
    if (!Number.isFinite(raw) || !Number.isInteger(raw)) return null;
    // Telegram uses signed 64-bit; JS safe integer is enough for practical IDs
    if (!Number.isSafeInteger(raw)) return null;
    return String(raw);
  }
  if (typeof raw !== "string") return null;

  const trimmed = raw.trim();
  if (trimmed.length === 0 || trimmed.length > MAX_CHAT_ID_LEN) return null;

  // Path / prototype injection guards
  if (
    trimmed.includes("/") ||
    trimmed.includes("\\") ||
    trimmed.includes("..") ||
    trimmed.includes("\0") ||
    trimmed.includes("__proto__") ||
    trimmed.includes("constructor") ||
    trimmed.includes("prototype")
  ) {
    return null;
  }

  // Telegram chat IDs: optional leading minus, digits only (e.g. -100123..., 123456)
  if (!/^-?\d+$/.test(trimmed)) return null;

  return trimmed;
}

/**
 * Parse comma-separated env allowlist (telegram-worker style).
 * Treats empty / "__SECRET__" as unset.
 */
export function parseEnvChatIds(raw: string | null | undefined): string[] {
  if (raw == null) return [];
  const value = String(raw).trim();
  if (!value || value === SECRET_PLACEHOLDER) return [];

  const ids: string[] = [];
  for (const part of value.split(",")) {
    const normalized = normalizeChatId(part.trim());
    if (normalized) ids.push(normalized);
  }
  return ids;
}

/**
 * Parse KV JSON array of string|number chat IDs.
 * Rejects non-arrays, oversized lists, and invalid entries.
 */
export function parseKvChatIds(raw: unknown): string[] | null {
  if (!Array.isArray(raw)) return null;
  if (raw.length === 0 || raw.length > MAX_ALLOWLIST_ENTRIES) return null;

  const ids: string[] = [];
  for (const entry of raw) {
    const normalized = normalizeChatId(entry);
    if (normalized) ids.push(normalized);
  }
  return ids.length > 0 ? ids : null;
}

/**
 * Load allowlist from env bindings and/or CONFIG_KV.
 * Env and KV are unioned when both contribute IDs.
 */
export async function loadChatIdAllowlist(
  env: ChatIdAllowlistEnv
): Promise<ChatIdAllowlistConfig> {
  const allowedIds = new Set<string>();
  const sources: Array<"env" | "kv"> = [];

  // Prefer TELEGRAM_ALLOWED_CHAT_IDS; fall back to AUTHORIZED_CHAT_IDS
  const envRaw =
    env.TELEGRAM_ALLOWED_CHAT_IDS ?? env.AUTHORIZED_CHAT_IDS ?? undefined;
  const fromEnv = parseEnvChatIds(envRaw);
  if (fromEnv.length > 0) {
    for (const id of fromEnv) allowedIds.add(id);
    sources.push("env");
  }

  const kv = env.CONFIG_KV;
  if (kv) {
    try {
      const kvStr = await kv.get(KV_TELEGRAM_ALLOWED_CHAT_IDS);
      if (kvStr) {
        try {
          const raw: unknown = JSON.parse(kvStr);
          const parsed = parseKvChatIds(raw);
          if (parsed) {
            for (const id of parsed) allowedIds.add(id);
            sources.push("kv");
          } else {
            logger.error(
              "Invalid telegram allowed_chat_ids payload in KV (ignored)"
            );
          }
        } catch (parseError) {
          logger.error("Error parsing telegram allowed_chat_ids JSON from KV", {
            error: parseError,
          });
        }
      }
    } catch (e) {
      logger.error("Error loading telegram allowed_chat_ids from KV", {
        error: e,
      });
    }
  }

  return {
    configured: allowedIds.size > 0,
    allowedIds,
    sources,
  };
}

/**
 * Fail-closed check: chatId must be valid format AND present in allowlist.
 * When allowlist is unconfigured, reject with a clear reason (notify path only).
 */
export async function checkChatIdAllowlist(
  chatId: unknown,
  env: ChatIdAllowlistEnv
): Promise<ChatIdCheckResult> {
  const config = await loadChatIdAllowlist(env);
  const normalized = normalizeChatId(chatId);

  if (!normalized) {
    return {
      allowed: false,
      reason: "Invalid notify payload: chatId format is invalid",
      config,
    };
  }

  if (!config.configured) {
    logger.warn(
      "notify chatId allowlist not configured — rejecting notify (fail-closed)"
    );
    return {
      allowed: false,
      reason: "notify chatId allowlist not configured",
      normalized,
      config,
    };
  }

  if (!config.allowedIds.has(normalized)) {
    logger.warn("notify chatId not in allowlist", {
      // Do not log full allowlist; only the requested id
      chatId: normalized,
    });
    return {
      allowed: false,
      reason: "chatId not in notify allowlist",
      normalized,
      config,
    };
  }

  return {
    allowed: true,
    normalized,
    config,
  };
}
