/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, it, expect, mock } from "bun:test";
import {
  formatSseEvent,
  formatSseComment,
  normalizeTimestampMs,
  rowId,
  takeNewRows,
  mapSignalToTradeEvent,
  mapLogToLogEvent,
  createOperatorSseStream,
  type OperatorSseEnv,
} from "../src/operatorSse";

describe("operatorSse pure helpers", () => {
  it("formatSseEvent wraps JSON in data: lines", () => {
    const out = formatSseEvent({ type: "connected", stream: "trades", ts: 1 });
    expect(out).toBe(
      'data: {"type":"connected","stream":"trades","ts":1}\n\n'
    );
  });

  it("formatSseComment emits SSE comment", () => {
    expect(formatSseComment("ping")).toBe(": ping\n\n");
  });

  it("normalizeTimestampMs converts seconds to ms", () => {
    expect(normalizeTimestampMs(1_700_000_000, 0)).toBe(1_700_000_000_000);
    expect(normalizeTimestampMs(1_700_000_000_000, 0)).toBe(1_700_000_000_000);
    expect(normalizeTimestampMs("bad", 42)).toBe(42);
  });

  it("rowId prefers signal_id then id", () => {
    expect(rowId({ signal_id: "s1", id: "i1" })).toBe("s1");
    expect(rowId({ id: "i1" })).toBe("i1");
    expect(rowId({ foo: 1 })).toBeNull();
  });

  it("takeNewRows seeds on first batch without emitting", () => {
    const seen = new Set<string>();
    const first = takeNewRows(
      [
        { signal_id: "a", timestamp: 1 },
        { signal_id: "b", timestamp: 2 },
      ],
      seen,
      false
    );
    expect(first.newRows).toEqual([]);
    expect(first.seeded).toBe(true);
    expect(seen.has("a")).toBe(true);
    expect(seen.has("b")).toBe(true);

    const second = takeNewRows(
      [
        { signal_id: "b", timestamp: 2 },
        { signal_id: "c", timestamp: 3 },
      ],
      seen,
      true
    );
    expect(second.newRows).toEqual([{ signal_id: "c", timestamp: 3 }]);
    expect(seen.has("c")).toBe(true);
  });

  it("mapSignalToTradeEvent maps side and fields", () => {
    const ev = mapSignalToTradeEvent(
      {
        signal_id: "sig-1",
        symbol: "BTCUSDT",
        signal_type: "SHORT",
        source: "tv",
        timestamp: 1_700_000_000,
      },
      99
    );
    expect(ev.type).toBe("trade");
    expect(ev.id).toBe("sig-1");
    expect(ev.side).toBe("sell");
    expect(ev.symbol).toBe("BTCUSDT");
    expect(ev.exchange).toBe("tv");
    expect(ev.timestamp).toBe(1_700_000_000_000);
    expect(ev.ts).toBe(99);
  });

  it("mapLogToLogEvent normalizes level and metadata", () => {
    const ev = mapLogToLogEvent(
      {
        id: "log-1",
        level: "WARNING",
        service: "trade-worker",
        message: "retry",
        timestamp: 1_700_000_000,
        details: '{"attempt":2}',
      },
      50
    );
    expect(ev.type).toBe("log");
    expect(ev.level).toBe("warn");
    expect(ev.workerId).toBe("trade-worker");
    expect(ev.metadata).toEqual({ attempt: 2 });
    expect(ev.timestamp).toBe(1_700_000_000_000);
  });
});

describe("createOperatorSseStream", () => {
  async function readUntilClose(response: Response): Promise<string> {
    const text = await response.text();
    return text;
  }

  it("emits connected then closes when TRADE_SERVICE missing", async () => {
    const env: OperatorSseEnv = {
      INTERNAL_KEY_BINDING: "k",
    };
    const response = createOperatorSseStream("trades", env, undefined, {
      maxDurationMs: 50,
      pollIntervalMs: 10,
      heartbeatIntervalMs: 5,
      sleep: async () => {},
      now: () => 1000,
    });
    expect(response.status).toBe(200);
    expect(response.headers.get("Content-Type") ?? "").toContain(
      "text/event-stream"
    );
    const text = await readUntilClose(response);
    expect(text).toContain('"type":"connected"');
    expect(text).toContain('"stream":"trades"');
    expect(text).toContain('"type":"error"');
    expect(text).toContain("TRADE_SERVICE");
  });

  it("polls trade feed and emits only new signals after seed", async () => {
    let call = 0;
    const fetchMock = mock(async (input: Request | string) => {
      const url = typeof input === "string" ? input : input.url;
      expect(url).toContain("/api/signals");
      call += 1;
      if (call === 1) {
        return new Response(
          JSON.stringify({
            success: true,
            result: [
              {
                signal_id: "s1",
                symbol: "BTC",
                signal_type: "BUY",
                source: "a",
                timestamp: 100,
              },
            ],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } }
        );
      }
      return new Response(
        JSON.stringify({
          success: true,
          result: [
            {
              signal_id: "s1",
              symbol: "BTC",
              signal_type: "BUY",
              source: "a",
              timestamp: 100,
            },
            {
              signal_id: "s2",
              symbol: "ETH",
              signal_type: "SELL",
              source: "b",
              timestamp: 200,
            },
          ],
        }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    });

    const env: OperatorSseEnv = {
      INTERNAL_KEY_BINDING: "test-internal-key",
      TRADE_SERVICE: { fetch: fetchMock as any },
    };

    // Stable clock: advance only on sleep so multiple polls fit under maxDurationMs
    let t = 0;
    const response = createOperatorSseStream("trades", env, undefined, {
      maxDurationMs: 10_000,
      pollIntervalMs: 1000,
      heartbeatIntervalMs: 60_000,
      feedLimit: 20,
      now: () => t,
      sleep: async (ms) => {
        t += ms;
      },
    });

    const text = await readUntilClose(response);
    expect(text).toContain('"type":"connected"');
    // First poll seeds — no trade events for s1 alone; second poll emits s2
    expect(text).toContain('"type":"trade"');
    expect(text).toContain('"id":"s2"');
    expect(text).not.toMatch(/"id":"s1"/);
    expect(fetchMock.mock.calls.length).toBeGreaterThanOrEqual(2);
  });

  it("logs stream hits /api/system-logs", async () => {
    const fetchMock = mock(async (input: Request | string) => {
      const url = typeof input === "string" ? input : input.url;
      expect(url).toContain("/api/system-logs");
      return new Response(
        JSON.stringify({
          success: true,
          result: [
            {
              id: "l1",
              level: "info",
              service: "hoox",
              message: "hello",
              timestamp: 50,
            },
          ],
        }),
        { status: 200 }
      );
    });

    const env: OperatorSseEnv = {
      INTERNAL_KEY_BINDING: "k",
      TRADE_SERVICE: { fetch: fetchMock as any },
    };

    let t = 0;
    const response = createOperatorSseStream("logs", env, undefined, {
      maxDurationMs: 2500,
      pollIntervalMs: 1000,
      heartbeatIntervalMs: 60_000,
      now: () => t,
      sleep: async (ms) => {
        t += ms;
      },
    });

    const text = await readUntilClose(response);
    expect(text).toContain('"type":"connected"');
    expect(text).toContain('"stream":"logs"');
    expect(fetchMock.mock.calls.length).toBeGreaterThanOrEqual(1);
  });

  it("respects abort signal", async () => {
    const ac = new AbortController();
    const fetchMock = mock(async () => {
      ac.abort();
      return new Response(JSON.stringify({ success: true, result: [] }), {
        status: 200,
      });
    });

    const env: OperatorSseEnv = {
      INTERNAL_KEY_BINDING: "k",
      TRADE_SERVICE: { fetch: fetchMock as any },
    };

    const request = new Request("https://example.com/v1/trades/stream", {
      signal: ac.signal,
    });

    const response = createOperatorSseStream("trades", env, request, {
      maxDurationMs: 60_000,
      pollIntervalMs: 1000,
      sleep: async (_ms, signal) => {
        if (signal?.aborted) {
          throw new DOMException("Aborted", "AbortError");
        }
        // hang until abort — but fetch already aborted
        throw new DOMException("Aborted", "AbortError");
      },
      now: () => Date.now(),
    });

    const text = await readUntilClose(response);
    expect(text).toContain('"type":"connected"');
  });
});
