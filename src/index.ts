/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

// hoox/src/index.ts - Public-facing gateway for TradingView

import type {
  Fetcher,
  KVNamespace,
  Queue,
  DurableObjectNamespace,
} from "@cloudflare/workers-types";

import { checkKillSwitch } from "./killSwitch";
import { checkIpAllowlist } from "./ipAllowlist";
import {
  checkChatIdAllowlist,
  type ChatIdAllowlistEnv,
} from "./chatIdAllowlist";
import { deriveSessionId, getOrCreateSession } from "./sessionManager";
import { IdempotencyStore } from "./idempotencyStore";
import { RateLimiterStore } from "./rateLimiterStore";
import { checkRateLimit as kvRateLimit } from "./rateLimiter";
import {
  Errors,
  toError,
  createJsonResponse,
} from "@hoox-sh/hoox-shared/errors";
import {
  createLogger,
  withRequestLog,
  validateJson,
  requireOperatorAuth,
  timingSafeEqual,
  safeWaitUntil,
} from "@hoox-sh/hoox-shared/middleware";
import { createRouter } from "@hoox-sh/hoox-shared/router";
import {
  WebhookPayloadSchema,
  type WebhookPayload,
  type StandardResponse,
  type ProcessRequestBody,
  type WorkerInfo,
} from "@hoox-sh/hoox-shared/types";
import {
  trackAnalytics,
  type AnalyticsEnv,
} from "@hoox-sh/hoox-shared/analytics";
import { KVKeys } from "@hoox-sh/hoox-shared/kvKeys";
import {
  authenticatedServiceFetch,
  ServiceAuthError,
  TRADE_EXECUTE_AUTH_KEY_FIELDS,
  TELEGRAM_ALERT_AUTH_KEY_FIELDS,
} from "@hoox-sh/hoox-shared/service-bindings";
import {
  DISCLAIMER,
  DISCLAIMER_HEADER,
} from "@hoox-sh/hoox-shared/legal";
import { createOperatorSseStream } from "./operatorSse";

// --- Rate limiting limits (DO-atomic when RATE_LIMITER bound, else KV/memory) ---
const MAX_TRADES_PER_MINUTE = 10;
const RATE_LIMIT_WINDOW = 60; // 60 seconds

// TradingView / webhook payloads are small; keep a tight hard cap.
const MAX_JSON_BODY_BYTES = 64 * 1024; // 64 KiB
const MAX_IDEMPOTENCY_KEY_LEN = 256;

// --- Type Definitions ---

/**
 * Worker env is the wrangler-generated Cloudflare.Env surface.
 * Optional secrets used by operator routes (OPERATOR_API_KEY) and notify
 * chat allowlist (TELEGRAM_ALLOWED_CHAT_IDS / AUTHORIZED_CHAT_IDS) are
 * read as optional so we do not fight generated required bindings.
 */
type Env = Cloudflare.Env &
  ChatIdAllowlistEnv & {
    OPERATOR_API_KEY?: string;
  };

// --- Other interfaces (WebhookData, TradeData, etc.) remain the same ---
// ... existing interfaces ...
interface WebhookData {
  apiKey?: string;
  signal?: string;
  exchange?: string;
  action?: string;
  symbol?: string;
  quantity?: number;
  price?: number;
  leverage?: number;
  /** When true, execute against exchange testnet/sandbox (if supported). */
  test?: boolean;
  /** Optional client-supplied idempotency key (body or Idempotency-Key header). */
  idempotencyKey?: string;
  notify?: {
    message?: string;
    chatId: string | number;
  };
}

interface TradeData {
  requestId: string;
  exchange: string;
  action: string;
  symbol: string;
  quantity: number;
  price?: number;
  leverage?: number;
  /** When true, execute against exchange testnet/sandbox (if supported). */
  test?: boolean;
  /** Resolved idempotency key (client-provided or generated). */
  idempotencyKey?: string;
}

interface NotificationData {
  requestId: string;
  message: string;
  chatId: string | number;
}

interface ServiceResponse {
  success: boolean;
  requestId?: string;
  tradeResult?: unknown;
  notificationResult?: unknown;
  error?: string;
  /** Optional HTTP status hint for handleRequest failure mapping. */
  status?: number;
}

type HooxProcessRequestBody = ProcessRequestBody<{
  message?: string;
  chatId?: string;
}>;

// --- Security Headers ---
const SECURITY_HEADERS = {
  "X-Content-Type-Options": "nosniff",
  "X-Frame-Options": "DENY",
  "X-XSS-Protection": "1; mode=block",
  "Referrer-Policy": "strict-origin-when-cross-origin",
  "Permissions-Policy":
    "accelerometer=(), camera=(), geolocation=(), gyroscope=(), magnetometer=(), microphone=(), payment=(), usb=()",
  "Strict-Transport-Security": "max-age=31536000; includeSubDomains",
  "Content-Security-Policy":
    "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'",
};

// --- Response Wrapper for Security Headers ---
function wrapResponse(response: Response): Response {
  const headers = new Headers(response.headers);
  for (const [key, value] of Object.entries(SECURITY_HEADERS)) {
    headers.set(key, value);
  }
  headers.set(DISCLAIMER_HEADER, DISCLAIMER);
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers,
  });
}

// --- Default Export (Worker Entry Point) ---

const logger = createLogger({ service: "hoox-gateway", module: "router" });

const router = createRouter<Env>();

// Define routes — POST / and POST /webhook are the public signal ingress
const handleWebhook = async (
  request: Request,
  env: Env,
  ctx: ExecutionContext
) => handleRequest(request, env, ctx);

router.post("/webhook", handleWebhook);
router.post("/", handleWebhook);

router.get(
  "/health",
  async (_request: Request, env: Env, _ctx: ExecutionContext) => {
    // Liveness probe: no auth, no KV round-trips — only report binding presence.
    const bindings = {
      kv: env.CONFIG_KV ? "configured" : "missing",
      sessions: env.SESSIONS_KV ? "configured" : "missing",
      queue: env.TRADE_QUEUE ? "configured" : "missing",
      trade: env.TRADE_SERVICE ? "configured" : "missing",
      telegram: env.TELEGRAM_SERVICE ? "configured" : "missing",
      idempotency: env.IDEMPOTENCY_STORE ? "configured" : "missing",
      rateLimiter: env.RATE_LIMITER ? "configured" : "missing",
    };
    return wrapResponse(
      createJsonResponse({
        success: true,
        status: "ok",
        timestamp: Date.now(),
        service: "hoox",
        bindings,
      })
    );
  }
);

// ─── Operator management plane (/v1/*) — Bearer OPERATOR_API_KEY ─────────────
// Prefer a dedicated mgmt hostname + Cloudflare Access in front of these routes.
// TradingView /webhook remains separate (body apiKey + IP allowlist).

router.get(
  "/v1/health",
  async (request: Request, env: Env, _ctx: ExecutionContext) => {
    const denied = await requireOperatorAuth(request, env);
    if (denied) return wrapResponse(denied);
    return wrapResponse(
      createJsonResponse({
        success: true,
        result: {
          status: "ok",
          plane: "operator",
          worker: "hoox",
          timestamp: new Date().toISOString(),
        },
      })
    );
  }
);

router.get(
  "/v1/workers",
  async (request: Request, env: Env, _ctx: ExecutionContext) => {
    const denied = await requireOperatorAuth(request, env);
    if (denied) return wrapResponse(denied);
    // Minimal gateway self-view; extended fleet discovery is a later iteration.
    const workers: WorkerInfo[] = [
      {
        id: "hoox",
        name: "hoox",
        status: "operational",
        uptime: 0,
        cpu: 0,
        memory: 0,
        requests: 0,
        durableObjectCount: 0,
        edgeCount: 0,
        version: "operator-v1",
      },
    ];
    return wrapResponse(
      new Response(JSON.stringify(workers), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      })
    );
  }
);

router.get(
  "/v1/trades/stream",
  async (request: Request, env: Env, _ctx: ExecutionContext) => {
    const denied = await requireOperatorAuth(request, env);
    if (denied) return wrapResponse(denied);
    return wrapResponse(createOperatorSseStream("trades", env, request));
  }
);

router.get(
  "/v1/logs/stream",
  async (request: Request, env: Env, _ctx: ExecutionContext) => {
    const denied = await requireOperatorAuth(request, env);
    if (denied) return wrapResponse(denied);
    return wrapResponse(createOperatorSseStream("logs", env, request));
  }
);

/** Legacy unversioned aliases (same auth) for older clients. */
router.get(
  "/workers",
  async (request: Request, env: Env, ctx: ExecutionContext) => {
    // Reuse /v1/workers handler by internal redirect-style call
    const url = new URL(request.url);
    url.pathname = "/v1/workers";
    return router.handle(new Request(url, request), env, ctx);
  }
);

export default {
  fetch: withRequestLog(
    (request: Request, env: Env, ctx: ExecutionContext) => {
      return router.handle(request, env, ctx);
    },
    { service: "hoox-gateway", module: "router" }
  ),
};

// --- Request Handling Logic ---

/**
 * Early Content-Length reject (does not trust the header alone for the hard
 * cap — see `readJsonBodyWithLimit`).
 */
function rejectOversizedContentLength(request: Request): Response | null {
  const contentLength = request.headers.get("Content-Length");
  if (!contentLength) return null;
  const size = Number.parseInt(contentLength, 10);
  if (Number.isNaN(size) || size < 0 || size > MAX_JSON_BODY_BYTES) {
    return wrapResponse(
      createJsonResponse(
        {
          success: false,
          error: `Request body too large (max ${MAX_JSON_BODY_BYTES} bytes)`,
        },
        413
      )
    );
  }
  return null;
}

/**
 * Parse JSON body with a hard byte cap (does not trust Content-Length alone).
 */
async function readJsonBodyWithLimit(
  request: Request,
  maxBytes: number = MAX_JSON_BODY_BYTES
): Promise<
  | { ok: true; value: unknown }
  | { ok: false; status: number; error: string }
> {
  const reader = request.body?.getReader();
  if (!reader) {
    return { ok: false, status: 400, error: "Empty request body" };
  }
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      if (!value) continue;
      total += value.byteLength;
      if (total > maxBytes) {
        try {
          await reader.cancel();
        } catch {
          /* ignore cancel errors */
        }
        return {
          ok: false,
          status: 413,
          error: `Request body too large (max ${maxBytes} bytes)`,
        };
      }
      chunks.push(value);
    }
  } catch {
    return { ok: false, status: 400, error: "Failed to read request body" };
  }

  if (total === 0) {
    return { ok: false, status: 400, error: "Empty request body" };
  }

  const merged = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    merged.set(chunk, offset);
    offset += chunk.byteLength;
  }

  try {
    const text = new TextDecoder("utf-8", {
      fatal: true,
      ignoreBOM: true,
    }).decode(merged);
    return { ok: true, value: JSON.parse(text) as unknown };
  } catch {
    return { ok: false, status: 400, error: "Invalid JSON in request body" };
  }
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * True when the payload appears to request trade execution.
 * Empty-string placeholders (common in notify-only clients) do not count.
 * Any non-empty exchange/action/symbol triggers full Zod validation
 * (so zero/negative quantity still 400 when paired with real fields).
 */
function hasTradeIntent(data: WebhookData): boolean {
  const hasExchange =
    typeof data.exchange === "string" && data.exchange.trim().length > 0;
  const hasAction =
    typeof data.action === "string" && data.action.trim().length > 0;
  const hasSymbol =
    typeof data.symbol === "string" && data.symbol.trim().length > 0;
  return hasExchange || hasAction || hasSymbol;
}

async function handleRequest(
  request: Request,
  env: Env,
  ctx: ExecutionContext
): Promise<Response> {
  const startTime = Date.now();

  if (request.method !== "POST") {
    logger.info(
      `[handleRequest] Returning METHOD NOT ALLOWED response (status 405)`
    );
    return wrapResponse(new Response("Method not allowed", { status: 405 }));
  }

  const oversized = rejectOversizedContentLength(request);
  if (oversized) return oversized;

  const clientIp = request.headers.get("CF-Connecting-IP") || "";

  // Kill switch + IP allowlist in parallel (both read CONFIG_KV)
  const [killSwitch, ipCheck] = await Promise.all([
    checkKillSwitch(env.CONFIG_KV),
    checkIpAllowlist(env.CONFIG_KV, clientIp),
  ]);

  if (killSwitch.enabled) {
    logger.warn(
      `[handleRequest] Kill switch active (source=${killSwitch.source ?? "unknown"}); rejecting signal`
    );
    return wrapResponse(
      createJsonResponse(
        {
          success: false,
          error: "Trading paused: global kill switch is enabled",
          code: "KILL_SWITCH",
        },
        503
      )
    );
  }

  if (!ipCheck.allowed) {
    // Do not echo the full reason with raw IP internals beyond necessary ops logs
    logger.warn(`[handleRequest] IP rejected: ${ipCheck.reason}`);
    return wrapResponse(
      createJsonResponse({ success: false, error: "Access denied" }, 403)
    );
  }

  try {
    const parsed = await readJsonBodyWithLimit(request);
    if (!parsed.ok) {
      return wrapResponse(
        createJsonResponse(
          { success: false, error: parsed.error },
          parsed.status
        )
      );
    }
    if (!isPlainObject(parsed.value)) {
      return wrapResponse(
        createJsonResponse(
          { success: false, error: "Request body must be a JSON object" },
          400
        )
      );
    }

    const data = parsed.value as WebhookData;

    // Validate the API key using the secret binding (fail-closed)
    const { apiKey } = data;
    if (typeof apiKey !== "string" || apiKey.length === 0) {
      logger.warn("[handleRequest] apiKey missing from payload");
      return wrapResponse(
        createJsonResponse({ success: false, error: "Forbidden" }, 403)
      );
    }

    const isValid = validateApiKeyBinding(apiKey, env.WEBHOOK_API_KEY_BINDING);
    if (!isValid) {
      logger.warn("[handleRequest] Invalid apiKey provided");
      return wrapResponse(
        createJsonResponse({ success: false, error: "Forbidden" }, 403)
      );
    }

    // Remove the API key from the data before processing/forwarding
    delete data.apiKey;

    // Hash API key before using as session/rate-limit identity (never store secrets as KV keys)
    const sessionKey = await deriveSessionId(apiKey);

    // Session + queue mode in parallel (independent KV namespaces / keys)
    const [session, queueMode] = await Promise.all([
      getOrCreateSession(env.SESSIONS_KV, sessionKey),
      getQueueMode(env.CONFIG_KV),
    ]);

    // Rate-limit by stable hashed session key, NOT raw apiKey / per-request UUID
    if (!(await checkRateLimit(session.sessionId, env))) {
      logger.warn(
        `[handleRequest] Rate limit exceeded for session ${session.sessionId.slice(0, 12)}…`
      );
      return wrapResponse(
        createJsonResponse(
          {
            success: false,
            error: `Rate limit exceeded. Maximum ${MAX_TRADES_PER_MINUTE} trades per minute.`,
            code: "RATE_LIMITED",
          },
          429
        )
      );
    }

    const requestId = crypto.randomUUID();
    let overallSuccess = true;
    const errorMessages: string[] = [];

    const {
      exchange,
      action,
      symbol,
      quantity,
      price,
      leverage,
      test,
      notify,
      idempotencyKey: bodyIdempotencyKey,
    } = data;

    // Prefer body key, then Idempotency-Key header
    const headerIdempotencyKey =
      request.headers.get("Idempotency-Key") ??
      request.headers.get("idempotency-key") ??
      undefined;
    const clientIdempotencyKey =
      typeof bodyIdempotencyKey === "string" && bodyIdempotencyKey.length > 0
        ? bodyIdempotencyKey
        : headerIdempotencyKey || undefined;

    // Validate trade + notify first, then run independent I/O in parallel
    let tradeWork:
      | {
          requestId: string;
          exchange: string;
          action: WebhookPayload["action"];
          symbol: string;
          quantity: number;
          price?: number;
          leverage?: number;
          test?: boolean;
          idempotencyKey?: string;
        }
      | null = null;
    if (hasTradeIntent(data)) {
      // Normalize action case before schema validation
      const normalizedAction =
        typeof action === "string" ? action.toUpperCase() : action;
      const tradePayload = {
        exchange,
        action: normalizedAction,
        symbol,
        quantity,
        price: price === null ? undefined : price,
        leverage: leverage === null ? undefined : leverage,
        test,
      };
      const validation = validateJson(WebhookPayloadSchema, tradePayload);
      if (!validation.ok) {
        return wrapResponse(
          createJsonResponse(
            {
              success: false,
              error: `Invalid trade payload: ${validation.error}`,
            },
            400
          )
        );
      }
      const v = validation.value;
      tradeWork = {
        requestId,
        exchange: v.exchange,
        action: v.action,
        symbol: v.symbol,
        quantity: v.quantity,
        price: v.price,
        leverage: v.leverage,
        test: v.test,
        idempotencyKey: clientIdempotencyKey,
      };
    }

    let notifyWork: NotificationData | null = null;
    /** Pre-computed notify failure (allowlist / format) — trade may still run. */
    let notifyBlocked: ServiceResponse | null = null;
    if (notify !== undefined && notify !== null) {
      // Reject non-plain objects (arrays, Date, RegExp, primitives, etc.)
      if (
        typeof notify !== "object" ||
        Array.isArray(notify) ||
        Object.prototype.toString.call(notify) !== "[object Object]"
      ) {
        return wrapResponse(
          createJsonResponse(
            {
              success: false,
              error: "Invalid notify payload: chatId is required",
            },
            400
          )
        );
      }
      const notifyPayload = notify as {
        message?: unknown;
        chatId?: unknown;
      };
      const chatIdRaw = notifyPayload.chatId;
      // Fail-closed allowlist: unconfigured or non-matching chatId rejects
      // notify only (trade path still proceeds when present).
      const chatCheck = await checkChatIdAllowlist(chatIdRaw, env);
      if (!chatCheck.allowed) {
        const status = chatCheck.normalized ? 403 : 400;
        const error =
          chatCheck.reason ??
          (chatCheck.normalized
            ? "chatId not in notify allowlist"
            : "Invalid notify payload: chatId is required");
        // Notify-only request → hard fail with client status
        if (!tradeWork) {
          return wrapResponse(
            createJsonResponse({ success: false, error, requestId }, status)
          );
        }
        // Combined trade+notify: block notify, continue trade
        notifyBlocked = {
          success: false,
          requestId,
          error,
          status,
        };
        logger.warn(`[${requestId}] notify blocked by chatId allowlist`, {
          error,
        });
      } else {
        notifyWork = {
          requestId,
          message:
            typeof notifyPayload.message === "string" &&
            notifyPayload.message.length > 0
              ? notifyPayload.message
              : createDefaultMessage(data),
          chatId: chatCheck.normalized as string,
        };
      }
    }

    // Parallelize independent service-binding work (trade ⊥ notify)
    const [tradeResult, notificationResultRaw] = await Promise.all([
      tradeWork
        ? processTrade(tradeWork, env, queueMode)
        : Promise.resolve(null as ServiceResponse | null),
      notifyWork
        ? processNotification(notifyWork, env)
        : Promise.resolve(null as ServiceResponse | null),
    ]);
    const notificationResult = notifyBlocked ?? notificationResultRaw;

    if (tradeWork && !tradeResult?.success) {
      overallSuccess = false;
      errorMessages.push(tradeResult?.error || "Trade processing failed");
      logger.error(`Trade processing failed for ${requestId}`, {
        error: tradeResult?.error,
      });
    }
    if (
      (notifyWork || notifyBlocked) &&
      notificationResult &&
      !notificationResult.success
    ) {
      overallSuccess = false;
      errorMessages.push(
        notificationResult.error || "Notification processing failed"
      );
      logger.error(`Notification processing failed for ${requestId}`, {
        error: notificationResult.error,
      });
    }

    if (!hasTradeIntent(data) && !notify) {
      return wrapResponse(
        createJsonResponse(
          {
            success: false,
            error:
              "Nothing to process: provide trade fields and/or a notify payload",
          },
          400
        )
      );
    }

    // Track webhook API call (fire-and-forget)
    const latencyMs = Date.now() - startTime;
    safeWaitUntil(
      ctx,
      trackAnalytics(env as AnalyticsEnv, "/track/api-call", {
        worker: "hoox",
        endpoint: "/webhook",
        latencyMs,
        success: overallSuccess,
      }),
      (err) => logger.error("trackAnalytics failed", { error: String(err) })
    );

    const tradeQueued =
      tradeResult?.success === true &&
      tradeResult.tradeResult !== null &&
      typeof tradeResult.tradeResult === "object" &&
      (tradeResult.tradeResult as { queued?: boolean }).queued === true;

    if (overallSuccess) {
      const status = tradeQueued ? 202 : 200;
      logger.info(
        `[handleRequest] Returning SUCCESS response (status ${status}) for ${requestId}`
      );
      return wrapResponse(
        createJsonResponse(
          {
            success: true,
            requestId,
            ...(tradeQueued ? { status: "Enqueued" } : {}),
            tradeResult,
            notificationResult,
          },
          status
        )
      );
    }

    // Map known client-facing failures to appropriate statuses
    const isDuplicate = errorMessages.some((m) =>
      m.toLowerCase().includes("duplicate")
    );
    const isUnavailable = errorMessages.some((m) =>
      m.toLowerCase().includes("unavailable")
    );
    const tradeHint =
      tradeResult?.status &&
      Number.isFinite(tradeResult.status) &&
      tradeResult.status >= 400
        ? tradeResult.status
        : undefined;
    const notifyHint =
      notificationResult?.status &&
      Number.isFinite(notificationResult.status) &&
      notificationResult.status >= 400
        ? notificationResult.status
        : undefined;
    // Prefer more specific client errors; trade hint wins when both present
    const hintedStatus = tradeHint ?? notifyHint;
    const status =
      hintedStatus ?? (isDuplicate ? 409 : isUnavailable ? 503 : 500);
    logger.info(
      `[handleRequest] Returning FAILURE response (status ${status}) for ${requestId}`
    );
    return wrapResponse(
      createJsonResponse(
        {
          success: false,
          requestId,
          error: `Processing failed: ${errorMessages.join("; ")}`,
          tradeResult,
          notificationResult,
        },
        status
      )
    );
  } catch (error: unknown) {
    // Never leak stack / internal details to the client
    logger.error(`[handleRequest] Uncaught error`, {
      error: toError(error),
    });
    return wrapResponse(Errors.internal("Internal Server Error"));
  }
}

/**
 * Secure API key validation using a secret binding.
 * Fail-closed: missing binding → reject. Constant-time compare.
 */
function validateApiKeyBinding(apiKey: string, binding?: string): boolean {
  if (!binding || binding.length === 0) {
    logger.error(
      "[validateApiKeyBinding] WEBHOOK_API_KEY_BINDING is not configured."
    );
    return false;
  }
  return timingSafeEqual(apiKey, binding);
}

/**
 * Get queue mode from KV config.
 * Returns "queue_everywhere" or "queue_failover" (default)
 */
async function getQueueMode(
  kv: KVNamespace
): Promise<"queue_everywhere" | "queue_failover"> {
  const mode = await kv.get(KVKeys.KV_WEBHOOK_QUEUE_MODE);
  return mode === "queue_everywhere" ? "queue_everywhere" : "queue_failover";
}

/**
 * Aligned with trade-worker `FP_TIME_BUCKET_MS` / `buildTradeFingerprint`.
 */
const FP_TIME_BUCKET_MS = 60_000;

/**
 * Generate fingerprint idempotency key for a trade when the client did not
 * supply one. Mode-split so live and test fills never dedupe each other.
 *
 * Format: `trade:{exchange}:{symbol}:{action}:{quantity}:{mode}:{minuteBucket}`
 *
 * Tradeoff (documented intentionally):
 * - A random nonce would defeat dedupe entirely and is NOT used.
 * - Including a coarse per-minute time bucket (`floor(nowMs / 60_000)`) means
 *   true retries (e.g. TradingView redelivery) within the same minute still
 *   collide and are blocked as duplicates, while intentional same-size trades
 *   in a later minute get a new key and are not accidentally blocked for the
 *   full DO TTL.
 * - Client-supplied keys (body / Idempotency-Key) are preferred and are not
 *   time-bucketed.
 *
 * Prefix stays `trade:` (gateway historical format); trade-worker uses
 * `idemp:fp:` when it mints its own fingerprint for direct ingress. When the
 * gateway forwards its resolved key, trade-worker uses it as-is.
 *
 * @param nowMs - injectable clock for tests; defaults to Date.now()
 */
function generateIdempotencyKey(
  tradeData: TradeData,
  nowMs: number = Date.now()
): string {
  const mode = tradeData.test === true ? "test" : "live";
  const bucket = Math.floor(nowMs / FP_TIME_BUCKET_MS);
  return `trade:${tradeData.exchange}:${tradeData.symbol}:${tradeData.action}:${tradeData.quantity}:${mode}:${bucket}`;
}

/**
 * Resolve client-supplied idempotency key (body or header) or auto-generate.
 * Client keys are namespaced and mode-split to avoid collisions.
 */
function resolveIdempotencyKey(tradeData: TradeData): string {
  const provided = tradeData.idempotencyKey?.trim();
  if (provided && provided.length > 0 && provided.length <= MAX_IDEMPOTENCY_KEY_LEN) {
    const mode = tradeData.test === true ? "test" : "live";
    return `idemp:${provided}:${mode}`;
  }
  return generateIdempotencyKey(tradeData);
}

/** Fixed shard count for idempotency DOs — avoids one DO instance per unique key. */
const IDEMPOTENCY_SHARD_COUNT = 16;

/**
 * Map an idempotency key to a stable DO shard name.
 * Keys are stored inside the shard DO; the DO name is not the secret key itself.
 */
function idempotencyShardName(key: string): string {
  let h = 2166136261;
  for (let i = 0; i < key.length; i++) {
    h ^= key.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  const shard = (h >>> 0) % IDEMPOTENCY_SHARD_COUNT;
  return `idemp-shard-${shard}`;
}

/**
 * Resolve the sharded IdempotencyStore stub for a key.
 * Fail-closed: missing DO throws IDEMPOTENCY_UNAVAILABLE.
 */
function getIdempotencyStub(env: Env, key: string): IdempotencyStore {
  if (!env.IDEMPOTENCY_STORE) {
    logger.error(
      "[idempotency] IDEMPOTENCY_STORE missing — refusing trade (fail-closed)"
    );
    throw new Error("IDEMPOTENCY_UNAVAILABLE");
  }
  const id = env.IDEMPOTENCY_STORE.idFromName(idempotencyShardName(key));
  return env.IDEMPOTENCY_STORE.get(id) as unknown as IdempotencyStore;
}

/**
 * Phase 1: reserve an idempotency key (pending) before queue/service.
 * Returns true if the key is new (proceed), false if duplicate (in-flight or committed).
 *
 * Fail-closed: missing DO or storage errors refuse the trade (503 path)
 * rather than risking double fills under webhook retries.
 */
async function reserveIdempotency(env: Env, key: string): Promise<boolean> {
  try {
    const stub = getIdempotencyStub(env, key);
    const result = await stub.reserve(key);
    return result.ok;
  } catch (error) {
    logger.error("[reserveIdempotency] Error:", { error: toError(error) });
    throw error instanceof Error ? error : new Error(String(error));
  }
}

/**
 * Phase 2 success: mark reserved key as committed after queue ack or
 * trade-service response where HTTP ok AND body.success === true.
 * Errors are logged but not rethrown — the trade already succeeded downstream.
 */
async function commitIdempotency(env: Env, key: string): Promise<void> {
  try {
    const stub = getIdempotencyStub(env, key);
    await stub.commit(key);
  } catch (error) {
    logger.error("[commitIdempotency] Error:", { error: toError(error) });
  }
}

/**
 * Phase 2 hard failure: release reservation so retries can proceed.
 * Errors are logged but not rethrown — caller already returns an error response.
 */
async function releaseIdempotency(env: Env, key: string): Promise<void> {
  try {
    const stub = getIdempotencyStub(env, key);
    await stub.release(key);
  } catch (error) {
    logger.error("[releaseIdempotency] Error:", { error: toError(error) });
  }
}

/**
 * Rate limiting delegation — prefers RateLimiterStore DO (atomic multi-isolate)
 * when RATE_LIMITER is bound; else KV-backed best-effort; else in-memory.
 * Key must be stable across requests (session / apiKey), never a UUID.
 */
async function checkRateLimit(sessionId: string, env: Env): Promise<boolean> {
  return kvRateLimit(env.CONFIG_KV ?? null, `session:${sessionId}`, {
    maxRequests: MAX_TRADES_PER_MINUTE,
    windowSeconds: RATE_LIMIT_WINDOW,
    rateLimiter: env.RATE_LIMITER ?? null,
  });
}

/**
 * Send trade to queue for async processing.
 * Passes the gateway-resolved idempotency key so trade-worker KV aligns with DO.
 */
async function sendTradeToQueue(
  queue: Queue,
  tradeData: TradeData,
  idempotencyKey?: string
): Promise<void> {
  const message = {
    requestId: tradeData.requestId,
    exchange: tradeData.exchange,
    action: tradeData.action,
    symbol: tradeData.symbol,
    quantity: tradeData.quantity,
    price: tradeData.price,
    leverage: tradeData.leverage,
    test: tradeData.test,
    ...(idempotencyKey ? { idempotencyKey } : {}),
    queuedAt: new Date().toISOString(),
  };
  await queue.send(message);
  logger.info(`[${tradeData.requestId}] Trade sent to queue`);
}

// Forward to trade worker using Service Binding or Queue
async function processTrade(
  tradeData: TradeData,
  env: Env,
  queueMode: "queue_everywhere" | "queue_failover" = "queue_failover"
): Promise<ServiceResponse> {
  const {
    requestId,
    exchange,
    action,
    symbol,
    quantity,
    price,
    leverage,
    test,
  } = tradeData;
  logger.info(`[${requestId}] processTrade: Received trade data`, {
    tradeData,
  });
  logger.info(`[${requestId}] Queue mode: ${queueMode}`);

  // Phase 1: reserve idempotency key before any queue/service work.
  // Pending blocks in-flight retries; commit/release decide the final state.
  const idempotencyKey = resolveIdempotencyKey(tradeData);
  let reserved: boolean;
  try {
    reserved = await reserveIdempotency(env, idempotencyKey);
  } catch (error: unknown) {
    const msg = toError(error, "Idempotency reserve failed");
    logger.error(`[${requestId}] Idempotency unavailable: ${msg}`);
    return {
      success: false,
      requestId,
      error: "Idempotency store unavailable. Trade refused (fail-closed).",
      status: 503,
    };
  }
  if (!reserved) {
    logger.info(
      `[${requestId}] Duplicate trade detected, rejecting key prefix: ${idempotencyKey.slice(0, 48)}`
    );
    return {
      success: false,
      requestId,
      error: "Duplicate trade request. This trade was already processed.",
      status: 409,
    };
  }

  // Rate limit is enforced in handleRequest against the stable session key.

  // Check if we should use queue
  const useQueue = queueMode === "queue_everywhere" || !env.TRADE_SERVICE;

  if (useQueue && env.TRADE_QUEUE) {
    // Use queue mode - send to queue and return success immediately
    try {
      await sendTradeToQueue(env.TRADE_QUEUE, tradeData, idempotencyKey);
      await commitIdempotency(env, idempotencyKey);
      return {
        success: true,
        requestId,
        tradeResult: { queued: true, message: "Trade queued for execution" },
      };
    } catch (error: unknown) {
      const errorMsg = toError(error, "Unknown error");
      logger.error(`[${requestId}] Failed to queue trade:`, {
        error: errorMsg,
      });
      // Fall back to direct service call if queue fails
    }
  }

  // Direct service call (or fallback from queue mode)
  if (!env.TRADE_SERVICE) {
    logger.error(`[${requestId}] TRADE_SERVICE binding is not configured.`);
    await releaseIdempotency(env, idempotencyKey);
    return {
      success: false,
      requestId,
      error: "Trade service binding not available.",
    };
  }

  try {
    // Construct the payload expected by trade-worker's /webhook endpoint
    const tradeWorkerPayload: WebhookPayload = {
      exchange: exchange,
      // Ensure action matches the expected enum in trade-worker (LONG, SHORT, etc.)
      action: action.toUpperCase() as WebhookPayload["action"],
      symbol: symbol,
      quantity: quantity,
      price: price,
      leverage: leverage,
      test: test,
    };

    logger.info(
      `[${requestId}] Calling TRADE_SERVICE service binding with payload`,
      { payload: tradeWorkerPayload }
    );
    // Fail-closed: never call trade without mesh auth. Prefer TRADE_EXECUTE
    // key, fall back to INTERNAL_KEY_BINDING / AGENT_INTERNAL_KEY.
    // Forward gateway-resolved Idempotency-Key so trade-worker KV shares the
    // same logical key as the DO reservation (client key or auto fingerprint).
    const response = await authenticatedServiceFetch(
      env.TRADE_SERVICE,
      env,
      "/webhook",
      tradeWorkerPayload,
      {
        headers: {
          "X-Request-ID": requestId,
          "Idempotency-Key": idempotencyKey,
        },
        internalKeyFields: TRADE_EXECUTE_AUTH_KEY_FIELDS,
      }
    );

    if (!response.ok) {
      // Log upstream body server-side only — never echo to the client
      const errorText = await response.text();
      logger.error(
        `[${requestId}] Error calling TRADE_SERVICE: ${response.status}`,
        { upstream: errorText.slice(0, 500) }
      );

      // If in queue_failover mode and direct call failed, try queue as fallback
      if (queueMode === "queue_failover" && env.TRADE_QUEUE) {
        logger.info(
          `[${requestId}] Direct call failed, attempting queue fallback...`
        );
        try {
          await sendTradeToQueue(env.TRADE_QUEUE, tradeData, idempotencyKey);
          await commitIdempotency(env, idempotencyKey);
          return {
            success: true,
            requestId,
            tradeResult: {
              queued: true,
              fallback: true,
              message: "Trade queued after direct call failure",
            },
          };
        } catch (queueError: unknown) {
          logger.error(`[${requestId}] Queue fallback also failed:`, {
            error: toError(queueError),
          });
        }
      }

      await releaseIdempotency(env, idempotencyKey);
      return {
        success: false,
        requestId,
        error: `Trade service call failed (${response.status})`,
      };
    }

    // Trade service HTTP 2xx — only commit when body indicates true success.
    // Soft exchange failures (success: false) must release so legitimate retries work.
    let result: StandardResponse;
    try {
      result = (await response.json()) as StandardResponse;
    } catch (parseError: unknown) {
      logger.error(`[${requestId}] Failed to parse TRADE_SERVICE response:`, {
        error: toError(parseError),
      });
      await releaseIdempotency(env, idempotencyKey);
      return {
        success: false,
        requestId,
        error: "Trade service returned an unreadable response.",
      };
    }
    logger.info(`[${requestId}] Response from TRADE_SERVICE`, { result });

    if (result.success === true) {
      await commitIdempotency(env, idempotencyKey);
    } else {
      await releaseIdempotency(env, idempotencyKey);
    }

    return {
      success: result.success === true,
      requestId,
      tradeResult: result.result,
      error: result.error ?? undefined,
    };
  } catch (error: unknown) {
    if (error instanceof ServiceAuthError) {
      logger.error(
        `[${requestId}] TRADE_SERVICE auth misconfigured: ${error.message}`
      );
      await releaseIdempotency(env, idempotencyKey);
      return {
        success: false,
        requestId,
        error: "Internal authentication key not configured.",
      };
    }
    const errorMsg = toError(error, "Unknown error calling trade service");
    logger.error(
      `[${requestId}] Exception calling TRADE_SERVICE: ${errorMsg}`,
      { error: toError(error) }
    );

    // If in queue_failover mode and exception occurred, try queue as fallback
    if (queueMode === "queue_failover" && env.TRADE_QUEUE) {
      logger.info(
        `[${requestId}] Direct call exception, attempting queue fallback...`
      );
      try {
        await sendTradeToQueue(env.TRADE_QUEUE, tradeData, idempotencyKey);
        await commitIdempotency(env, idempotencyKey);
        return {
          success: true,
          requestId,
          tradeResult: {
            queued: true,
            fallback: true,
            message: "Trade queued after exception",
          },
        };
      } catch (queueError: unknown) {
        logger.error(`[${requestId}] Queue fallback also failed:`, {
          error: toError(queueError),
        });
      }
    }

    await releaseIdempotency(env, idempotencyKey);
    return {
      success: false,
      requestId,
      error: `Exception during trade service call: ${errorMsg}`,
    };
  }
}

// Forward to notification worker using Service Binding
async function processNotification(
  notificationData: NotificationData,
  env: Env
): Promise<ServiceResponse> {
  const { requestId, message, chatId } = notificationData;
  logger.info(
    `[${requestId}] processNotification: Received notification data`,
    { notificationData }
  );

  // --- Task 10.5: Implement Inter-Worker Communication ---
  if (!env.TELEGRAM_SERVICE) {
    logger.error(`[${requestId}] TELEGRAM_SERVICE binding is not configured.`);
    return {
      success: false,
      requestId,
      error: "Telegram service binding not available.",
    };
  }
  try {
    // Construct the payload expected by telegram-worker's /process endpoint
    const payload = {
      requestId: requestId,
      payload: {
        message: message,
        chatId: chatId,
      },
    };

    logger.info(`[${requestId}] Calling TELEGRAM_SERVICE service binding...`);
    const response = await authenticatedServiceFetch(
      env.TELEGRAM_SERVICE,
      env,
      "/process",
      payload,
      {
        internalKeyFields: TELEGRAM_ALERT_AUTH_KEY_FIELDS,
      }
    );

    if (!response.ok) {
      const errorText = await response.text();
      logger.error(
        `[${requestId}] Error calling TELEGRAM_SERVICE: ${response.status}`,
        { upstream: errorText.slice(0, 500) }
      );
      return {
        success: false,
        requestId,
        error: `Telegram service call failed (${response.status})`,
      };
    }

    // Assuming telegram-worker returns a StandardResponse { success: boolean, result?, error? }
    const result: StandardResponse = await response.json();
    logger.info(`[${requestId}] Response from TELEGRAM_SERVICE`, { result });
    return {
      success: result.success,
      requestId,
      notificationResult: result.result,
      error: result.error ?? undefined,
    };
  } catch (error: unknown) {
    if (error instanceof ServiceAuthError) {
      logger.error(
        `[${requestId}] TELEGRAM_SERVICE auth misconfigured: ${error.message}`
      );
      return {
        success: false,
        requestId,
        error: "Internal authentication key not configured.",
      };
    }
    const errorMsg = toError(error, "Unknown error calling telegram service");
    logger.error(
      `[${requestId}] Exception calling TELEGRAM_SERVICE: ${errorMsg}`,
      { error: toError(error) }
    );
    return {
      success: false,
      requestId,
      error: `Exception during telegram service call: ${errorMsg}`,
    };
  }
  // --- End Task 10.5 ---
}

// Create default message from trade data
function createDefaultMessage(data: WebhookData): string {
  const { exchange, action, symbol, quantity, price } = data;
  let message = `📊 Trade Alert: ${action ?? "?"} ${symbol ?? "?"}\n`;
  message += `📈 Exchange: ${exchange ?? "?"}\n`;
  message += `💰 Quantity: ${quantity ?? "?"}\n`;

  if (price !== undefined && price !== null) {
    message += `💵 Price: ${price}\n`;
  }

  return message;
}

export { IdempotencyStore, RateLimiterStore };
