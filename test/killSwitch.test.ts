/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, expect, test, beforeEach, jest } from "bun:test";
import { checkKillSwitch, isTradingPaused } from "../src/killSwitch";

describe("killSwitch (gateway wrapper)", () => {
  let mockGet: jest.Mock;

  beforeEach(() => {
    mockGet = jest.fn();
  });

  test("returns enabled: false when KV is undefined (fail-open missing)", async () => {
    const result = await checkKillSwitch(undefined);
    expect(result.enabled).toBe(false);
  });

  test("returns enabled: false when KV returns null", async () => {
    mockGet.mockResolvedValue(null);
    const kv = { get: mockGet } as any;
    const result = await checkKillSwitch(kv);
    expect(result.enabled).toBe(false);
  });

  test("returns enabled: true when trade:kill_switch is true", async () => {
    mockGet.mockImplementation(async (key: string) =>
      key === "trade:kill_switch" ? "true" : null
    );
    const kv = { get: mockGet } as any;
    const result = await checkKillSwitch(kv);
    expect(result.enabled).toBe(true);
    expect(result.source).toBe("trade:kill_switch");
  });

  test("returns enabled: true when global:kill_switch is true", async () => {
    mockGet.mockImplementation(async (key: string) =>
      key === "global:kill_switch" ? "true" : null
    );
    const kv = { get: mockGet } as any;
    const result = await checkKillSwitch(kv);
    expect(result.enabled).toBe(true);
    expect(result.source).toBe("global:kill_switch");
  });

  test("returns enabled: true when KV returns 'TRUE' (case insensitive)", async () => {
    mockGet.mockImplementation(async (key: string) =>
      key === "trade:kill_switch" ? "TRUE" : null
    );
    const kv = { get: mockGet } as any;
    const result = await checkKillSwitch(kv);
    expect(result.enabled).toBe(true);
  });

  test("returns enabled: true for truthy 1/yes/on", async () => {
    for (const flag of ["1", "yes", "on"]) {
      mockGet.mockImplementation(async (key: string) =>
        key === "trade:kill_switch" ? flag : null
      );
      const kv = { get: mockGet } as any;
      const result = await checkKillSwitch(kv);
      expect(result.enabled).toBe(true);
    }
  });

  test("returns enabled: false for other values", async () => {
    mockGet.mockResolvedValue("false");
    const kv = { get: mockGet } as any;
    const result = await checkKillSwitch(kv);
    expect(result.enabled).toBe(false);
  });

  test("handles KV error fail-closed (gateway onReadError: closed)", async () => {
    mockGet.mockRejectedValue(new Error("KV Error"));
    const kv = { get: mockGet } as any;
    const result = await checkKillSwitch(kv);
    // Safer than historical fail-open: treat unread kill switch as active.
    expect(result.enabled).toBe(true);
    expect(result.source).toBe("read_error");
    expect(result.error).toBeDefined();
  });

  test("isTradingPaused returns boolean", async () => {
    mockGet.mockResolvedValue(null);
    const kv = { get: mockGet } as any;
    const result = await isTradingPaused(kv);
    expect(typeof result).toBe("boolean");
  });
});
