/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Operator-plane SSE streams for TUI / management clients.
 *
 * Polls trade-worker feed endpoints over the TRADE_SERVICE binding and emits
 * Server-Sent Events. Streams run for a bounded wall time then close so the
 * client (which already reconnects) can open a fresh connection.
 */

import {
  authenticatedServiceFetch,
  TRADE_READ_AUTH_KEY_FIELDS,
  type AuthenticatedServiceEnv,
  type ServiceBinding,
} from "@hoox-sh/hoox-shared/service-bindings";
import { createLogger } from "@hoox-sh/hoox-shared/middleware";
import { toError } from "@hoox-sh/hoox-shared/errors";

const logger = createLogger({ service: "hoox-gateway", module: "operator-sse" });

/** Default poll cadence for trade-worker feeds. */
export const DEFAULT_POLL_INTERVAL_MS = 2000;
/** SSE comment heartbeats while idle. */
export const DEFAULT_HEARTBEAT_INTERVAL_MS = 15_000;
/** Wall-clock lifetime of one SSE connection before clean close. */
export const DEFAULT_MAX_DURATION_MS = 100_000;
/** How many recent rows to fetch per poll. */
export const DEFAULT_FEED_LIMIT = 20;
/** Cap on dedupe id set size (avoid unbounded growth). */
const MAX_SEEN_IDS = 256;

export type OperatorSseStreamKind = "trades" | "logs";

export interface OperatorSseEnv extends AuthenticatedServiceEnv {
  TRADE_SERVICE?: ServiceBinding;
  INTERNAL_KEY_BINDING?: string;
  TRADE_READ_KEY_BINDING?: string;
}

export interface OperatorSseOptions {
  pollIntervalMs?: number;
  heartbeatIntervalMs?: number;
  maxDurationMs?: number;
  feedLimit?: number;
  /** Injectable clock (tests). */
  now?: () => number;
  /** Injectable sleep (tests). */
  sleep?: (ms: number, signal?: AbortSignal) => Promise<void>;
}

// ─── Pure helpers (unit-tested) ──────────────────────────────────────────────

/**
 * Format a JSON payload as one SSE `data:` event (double newline terminated).
 */
export function formatSseEvent(data: unknown): string {
  return `data: ${JSON.stringify(data)}\n\n`;
}

/**
 * Format an SSE comment line (heartbeat / ignore by clients).
 */
export function formatSseComment(comment: string): string {
  return `: ${comment}\n\n`;
}

/**
 * Normalize epoch seconds → milliseconds when values look like unix seconds.
 */
export function normalizeTimestampMs(value: unknown, fallbackMs: number): number {
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallbackMs;
  // Values below ~year 2001 in ms-space are almost certainly seconds.
  if (n < 1e12) return Math.trunc(n * 1000);
  return Math.trunc(n);
}

/**
 * Extract a stable id from a feed row (signal_id preferred for trades).
 */
export function rowId(row: Record<string, unknown>): string | null {
  const candidates = [row.signal_id, row.id];
  for (const c of candidates) {
    if (typeof c === "string" && c.length > 0) return c;
    if (typeof c === "number" && Number.isFinite(c)) return String(c);
  }
  return null;
}

/**
 * Dedupe helper: on the first batch, seed `seen` and return nothing (baseline).
 * On later batches, return only rows whose id was not yet seen, then add them.
 */
export function takeNewRows(
  rows: Record<string, unknown>[],
  seen: Set<string>,
  seeded: boolean
): { newRows: Record<string, unknown>[]; seeded: boolean } {
  const ordered = [...rows].reverse(); // oldest → newest so client sees chrono order
  if (!seeded) {
    for (const row of ordered) {
      const id = rowId(row);
      if (id) {
        seen.add(id);
        trimSeen(seen);
      }
    }
    return { newRows: [], seeded: true };
  }

  const newRows: Record<string, unknown>[] = [];
  for (const row of ordered) {
    const id = rowId(row);
    if (!id) continue;
    if (seen.has(id)) continue;
    seen.add(id);
    trimSeen(seen);
    newRows.push(row);
  }
  return { newRows, seeded: true };
}

function trimSeen(seen: Set<string>): void {
  if (seen.size <= MAX_SEEN_IDS) return;
  // Set iteration order is insertion order — drop oldest entries.
  const excess = seen.size - MAX_SEEN_IDS;
  let i = 0;
  for (const key of seen) {
    if (i++ >= excess) break;
    seen.delete(key);
  }
}

/**
 * Map a trade_signals row into a TUI-friendly trade SSE payload.
 */
export function mapSignalToTradeEvent(
  signal: Record<string, unknown>,
  nowMs: number = Date.now()
): Record<string, unknown> {
  const signalType = String(signal.signal_type ?? "").toUpperCase();
  const side =
    signalType.includes("SELL") || signalType.includes("SHORT")
      ? "sell"
      : "buy";
  const id =
    rowId(signal) ??
    `signal-${normalizeTimestampMs(signal.timestamp, nowMs)}-${String(signal.symbol ?? "x")}`;

  return {
    type: "trade",
    id,
    symbol: String(signal.symbol ?? "UNKNOWN"),
    side,
    price: typeof signal.price === "number" ? signal.price : 0,
    quantity: typeof signal.quantity === "number" ? signal.quantity : 0,
    timestamp: normalizeTimestampMs(signal.timestamp, nowMs),
    exchange: String(signal.source ?? "signal"),
    strategy:
      typeof signal.source === "string" && signal.source.length > 0
        ? signal.source
        : undefined,
    ts: nowMs,
  };
}

/**
 * Map a system_logs row into a TUI-friendly log SSE payload.
 */
export function mapLogToLogEvent(
  log: Record<string, unknown>,
  nowMs: number = Date.now()
): Record<string, unknown> {
  const levelRaw = String(log.level ?? "info").toLowerCase();
  const level = (["debug", "info", "warn", "error"] as const).includes(
    levelRaw as "debug" | "info" | "warn" | "error"
  )
    ? levelRaw
    : levelRaw === "warning"
      ? "warn"
      : "info";

  const id =
    rowId(log) ??
    `log-${normalizeTimestampMs(log.timestamp, nowMs)}-${level}`;

  let metadata: Record<string, unknown> | undefined;
  if (typeof log.details === "string" && log.details.length > 0) {
    try {
      const parsed = JSON.parse(log.details) as unknown;
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        metadata = parsed as Record<string, unknown>;
      } else {
        metadata = { details: log.details };
      }
    } catch {
      metadata = { details: log.details };
    }
  }

  return {
    type: "log",
    id,
    level,
    message: String(log.message ?? ""),
    timestamp: normalizeTimestampMs(log.timestamp, nowMs),
    workerId:
      typeof log.service === "string" && log.service.length > 0
        ? log.service
        : undefined,
    source:
      typeof log.service === "string" && log.service.length > 0
        ? log.service
        : undefined,
    metadata,
    ts: nowMs,
  };
}

// ─── Internals ───────────────────────────────────────────────────────────────

export function defaultSleep(
  ms: number,
  signal?: AbortSignal
): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new DOMException("Aborted", "AbortError"));
      return;
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      clearTimeout(timer);
      reject(new DOMException("Aborted", "AbortError"));
    };
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

function isAbortError(err: unknown): boolean {
  return (
    (err instanceof DOMException && err.name === "AbortError") ||
    (err instanceof Error && err.name === "AbortError")
  );
}

interface FeedResponse {
  success?: boolean;
  result?: unknown;
  error?: string;
}

async function fetchFeedRows(
  env: OperatorSseEnv,
  path: string
): Promise<Record<string, unknown>[]> {
  if (!env.TRADE_SERVICE) {
    throw new Error("TRADE_SERVICE binding not available");
  }
  if (!env.INTERNAL_KEY_BINDING && !env.TRADE_READ_KEY_BINDING) {
    // authenticatedServiceFetch will reject, but surface a clearer message
    throw new Error("Internal auth key not configured for trade feed");
  }

  const response = await authenticatedServiceFetch(
    env.TRADE_SERVICE,
    env,
    path,
    undefined,
    {
      method: "GET",
      internalKeyFields: TRADE_READ_AUTH_KEY_FIELDS,
      timeout: 10_000,
    }
  );

  if (!response.ok) {
    const body = await response.text().catch(() => "");
    throw new Error(
      `trade-worker ${path} returned ${response.status}${body ? `: ${body.slice(0, 120)}` : ""}`
    );
  }

  const json = (await response.json()) as FeedResponse;
  if (json && json.success === false) {
    throw new Error(json.error || `trade-worker ${path} failed`);
  }

  const result = json?.result;
  if (!Array.isArray(result)) return [];
  return result.filter(
    (row): row is Record<string, unknown> =>
      row !== null && typeof row === "object" && !Array.isArray(row)
  );
}

// ─── Stream factory ──────────────────────────────────────────────────────────

/**
 * Create a long-lived `text/event-stream` Response for operator trade/log feeds.
 *
 * Lifecycle:
 * 1. Emit `{ type: "connected", stream, ts }`
 * 2. Poll trade-worker every ~2s; emit only new rows
 * 3. Heartbeat with SSE comments every ~15s when idle
 * 4. Close after ~100s so clients reconnect cleanly
 * 5. Abort early when `request.signal` fires
 */
export function createOperatorSseStream(
  stream: OperatorSseStreamKind,
  env: OperatorSseEnv,
  request?: Request,
  options: OperatorSseOptions = {}
): Response {
  const pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
  const heartbeatIntervalMs =
    options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS;
  const maxDurationMs = options.maxDurationMs ?? DEFAULT_MAX_DURATION_MS;
  const feedLimit = options.feedLimit ?? DEFAULT_FEED_LIMIT;
  const now = options.now ?? Date.now;
  const sleep = options.sleep ?? defaultSleep;
  const signal = request?.signal;

  const feedPath =
    stream === "trades"
      ? `/api/signals?limit=${feedLimit}`
      : `/api/system-logs?limit=${feedLimit}`;

  const encoder = new TextEncoder();
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      const enqueue = (chunk: string) => {
        try {
          controller.enqueue(encoder.encode(chunk));
        } catch {
          // Controller may already be closed
        }
      };

      const close = () => {
        try {
          controller.close();
        } catch {
          /* already closed */
        }
      };

      const run = async () => {
        const startedAt = now();
        const seen = new Set<string>();
        let seeded = false;
        let lastActivityAt = startedAt;
        let feedErrorEmitted = false;

        enqueue(
          formatSseEvent({
            type: "connected",
            stream,
            ts: startedAt,
          })
        );

        // Missing binding / auth: emit error once, keep heartbeats briefly, then close
        const bindingOk = Boolean(env.TRADE_SERVICE);
        const authOk = Boolean(
          env.INTERNAL_KEY_BINDING || env.TRADE_READ_KEY_BINDING
        );
        if (!bindingOk || !authOk) {
          const message = !bindingOk
            ? "TRADE_SERVICE binding not available"
            : "Internal auth key not configured for trade feed";
          enqueue(
            formatSseEvent({
              type: "error",
              message,
              stream,
              ts: now(),
            })
          );
          // A few heartbeats so clients see a real stream, then close
          try {
            await sleep(Math.min(heartbeatIntervalMs, 3000), signal);
            enqueue(formatSseComment("ping"));
            await sleep(Math.min(heartbeatIntervalMs, 3000), signal);
          } catch (err) {
            if (!isAbortError(err)) {
              logger.warn("operator SSE early-exit sleep failed", {
                error: toError(err),
              });
            }
          }
          close();
          return;
        }

        while (true) {
          if (signal?.aborted) break;
          const t = now();
          if (t - startedAt >= maxDurationMs) break;

          try {
            const rows = await fetchFeedRows(env, feedPath);
            const { newRows, seeded: nextSeeded } = takeNewRows(
              rows,
              seen,
              seeded
            );
            seeded = nextSeeded;

            for (const row of newRows) {
              const event =
                stream === "trades"
                  ? mapSignalToTradeEvent(row, now())
                  : mapLogToLogEvent(row, now());
              enqueue(formatSseEvent(event));
              lastActivityAt = now();
            }
            feedErrorEmitted = false;
          } catch (err) {
            if (isAbortError(err)) break;
            const message = toError(err, "feed poll failed");
            logger.warn(`operator SSE ${stream} poll failed`, { error: message });
            if (!feedErrorEmitted) {
              enqueue(
                formatSseEvent({
                  type: "error",
                  message,
                  stream,
                  ts: now(),
                })
              );
              feedErrorEmitted = true;
              lastActivityAt = now();
            }
            // Graceful degradation: keep heartbeats / retry next interval
          }

          const afterPoll = now();
          if (afterPoll - lastActivityAt >= heartbeatIntervalMs) {
            enqueue(formatSseComment("ping"));
            lastActivityAt = afterPoll;
          }

          const remaining = maxDurationMs - (afterPoll - startedAt);
          if (remaining <= 0) break;

          try {
            await sleep(Math.min(pollIntervalMs, remaining), signal);
          } catch (err) {
            if (isAbortError(err)) break;
            logger.warn("operator SSE sleep failed", { error: toError(err) });
            break;
          }
        }

        close();
      };

      void run().catch((err) => {
        if (!isAbortError(err)) {
          logger.error("operator SSE stream crashed", { error: toError(err) });
        }
        close();
      });
    },
    cancel() {
      // Client disconnected — ReadableStream cancel; sleep abort via signal if set
    },
  });

  return new Response(body, {
    status: 200,
    headers: {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      // Disable buffering on some proxies
      "X-Accel-Buffering": "no",
    },
  });
}
