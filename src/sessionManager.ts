/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

import type { KVNamespace } from "@cloudflare/workers-types";
import { createLogger } from "@hoox-sh/hoox-shared/middleware";

const logger = createLogger({ service: "hoox", module: "sessionManager" });

const SESSION_TTL = 3600;
const SESSION_KEY_PREFIX = "sess:";

export interface SessionData {
  lastSeen: string;
}

/**
 * Derive a non-reversible session identity from a secret (e.g. webhook API key).
 * Never store secrets as KV key names — listings/exports would leak them.
 */
export async function deriveSessionId(
  secretOrMaterial: string
): Promise<string> {
  const dig = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(secretOrMaterial)
  );
  const hex = [...new Uint8Array(dig)]
    .map((b) => b.toString(16).padStart(2, "0"))
    .join("");
  return `${SESSION_KEY_PREFIX}${hex}`;
}

export async function getOrCreateSession(
  kv: KVNamespace | undefined,
  sessionId?: string | null
): Promise<{ sessionId: string; isNew: boolean }> {
  // Callers that pass secrets MUST hash first via deriveSessionId().
  const id = sessionId || crypto.randomUUID();

  if (!kv) {
    return { sessionId: id, isNew: !sessionId };
  }

  try {
    const existing = await kv.get(id);
    const isNew = !existing;

    if (isNew || existing) {
      await kv.put(id, JSON.stringify({ lastSeen: new Date().toISOString() }), {
        expirationTtl: SESSION_TTL,
      });
    }

    return { sessionId: id, isNew };
  } catch (error: unknown) {
    logger.error("KV Session Error", { error });
    return { sessionId: id, isNew: !sessionId };
  }
}

export async function updateSession(
  kv: KVNamespace | undefined,
  sessionId: string
): Promise<void> {
  if (!kv) return;

  try {
    await kv.put(
      sessionId,
      JSON.stringify({ lastSeen: new Date().toISOString() }),
      {
        expirationTtl: SESSION_TTL,
      }
    );
  } catch (error: unknown) {
    logger.error("KV Session Error", { error });
  }
}
