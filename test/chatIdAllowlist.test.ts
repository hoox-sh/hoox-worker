/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained (hoox-sh)
 * SPDX-License-Identifier: Apache-2.0
 */

import { describe, expect, test, beforeEach, jest } from "bun:test";
import {
  normalizeChatId,
  parseEnvChatIds,
  parseKvChatIds,
  loadChatIdAllowlist,
  checkChatIdAllowlist,
  KV_TELEGRAM_ALLOWED_CHAT_IDS,
} from "../src/chatIdAllowlist";

describe("chatIdAllowlist — normalizeChatId", () => {
  test("accepts finite integer numbers", () => {
    expect(normalizeChatId(123456)).toBe("123456");
    expect(normalizeChatId(-100200300)).toBe("-100200300");
    expect(normalizeChatId(0)).toBe("0");
  });

  test("accepts digit strings with optional leading minus", () => {
    expect(normalizeChatId("123456")).toBe("123456");
    expect(normalizeChatId("  -100123  ")).toBe("-100123");
  });

  test("rejects non-finite / non-integer numbers", () => {
    expect(normalizeChatId(NaN)).toBeNull();
    expect(normalizeChatId(Infinity)).toBeNull();
    expect(normalizeChatId(1.5)).toBeNull();
  });

  test("rejects empty / non-string-non-number", () => {
    expect(normalizeChatId("")).toBeNull();
    expect(normalizeChatId("   ")).toBeNull();
    expect(normalizeChatId(null)).toBeNull();
    expect(normalizeChatId(undefined)).toBeNull();
    expect(normalizeChatId({})).toBeNull();
    expect(normalizeChatId([])).toBeNull();
  });

  test("rejects path injection and non-digit tokens", () => {
    expect(normalizeChatId("../etc/passwd")).toBeNull();
    expect(normalizeChatId("123/456")).toBeNull();
    expect(normalizeChatId("123\\456")).toBeNull();
    expect(normalizeChatId("__proto__")).toBeNull();
    expect(normalizeChatId("constructor")).toBeNull();
    expect(normalizeChatId("abc")).toBeNull();
    expect(normalizeChatId("12e3")).toBeNull();
  });
});

describe("chatIdAllowlist — parseEnvChatIds", () => {
  test("parses comma-separated list", () => {
    expect(parseEnvChatIds("111,222, -333 ")).toEqual(["111", "222", "-333"]);
  });

  test("treats empty / placeholder as unset", () => {
    expect(parseEnvChatIds(undefined)).toEqual([]);
    expect(parseEnvChatIds(null)).toEqual([]);
    expect(parseEnvChatIds("")).toEqual([]);
    expect(parseEnvChatIds("  ")).toEqual([]);
    expect(parseEnvChatIds("__SECRET__")).toEqual([]);
  });

  test("filters invalid entries", () => {
    expect(parseEnvChatIds("111,not-a-id,222")).toEqual(["111", "222"]);
  });
});

describe("chatIdAllowlist — parseKvChatIds", () => {
  test("parses JSON array of string|number", () => {
    expect(parseKvChatIds([123, "456", -100])).toEqual([
      "123",
      "456",
      "-100",
    ]);
  });

  test("rejects non-array / empty / invalid", () => {
    expect(parseKvChatIds({ "123": true })).toBeNull();
    expect(parseKvChatIds([])).toBeNull();
    expect(parseKvChatIds(["bad", "worse"])).toBeNull();
    expect(parseKvChatIds(null)).toBeNull();
  });
});

describe("chatIdAllowlist — load + check", () => {
  let mockGet: jest.Mock;

  beforeEach(() => {
    mockGet = jest.fn();
  });

  test("fail-closed when allowlist not configured", async () => {
    const result = await checkChatIdAllowlist("123456", {});
    expect(result.allowed).toBe(false);
    expect(result.reason).toContain("allowlist not configured");
    expect(result.config.configured).toBe(false);
  });

  test("allows chatId present in TELEGRAM_ALLOWED_CHAT_IDS env", async () => {
    const result = await checkChatIdAllowlist("123456", {
      TELEGRAM_ALLOWED_CHAT_IDS: "123456,789",
    });
    expect(result.allowed).toBe(true);
    expect(result.normalized).toBe("123456");
    expect(result.config.sources).toContain("env");
  });

  test("allows chatId present in AUTHORIZED_CHAT_IDS alias", async () => {
    const result = await checkChatIdAllowlist(111, {
      AUTHORIZED_CHAT_IDS: "111,222",
    });
    expect(result.allowed).toBe(true);
    expect(result.normalized).toBe("111");
  });

  test("denies chatId not in allowlist", async () => {
    const result = await checkChatIdAllowlist("999", {
      TELEGRAM_ALLOWED_CHAT_IDS: "111,222",
    });
    expect(result.allowed).toBe(false);
    expect(result.reason).toContain("not in notify allowlist");
  });

  test("prefers TELEGRAM_ALLOWED_CHAT_IDS over AUTHORIZED_CHAT_IDS", async () => {
    const result = await checkChatIdAllowlist("111", {
      TELEGRAM_ALLOWED_CHAT_IDS: "111",
      AUTHORIZED_CHAT_IDS: "999",
    });
    expect(result.allowed).toBe(true);
    // When preferred is set, alias is not consulted for the env string
    expect(result.config.allowedIds.has("111")).toBe(true);
    expect(result.config.allowedIds.has("999")).toBe(false);
  });

  test("loads allowlist from CONFIG_KV JSON array", async () => {
    mockGet.mockImplementation(async (key: string) => {
      if (key === KV_TELEGRAM_ALLOWED_CHAT_IDS) {
        return JSON.stringify([123456, "-100200"]);
      }
      return null;
    });
    const result = await checkChatIdAllowlist(123456, {
      CONFIG_KV: { get: mockGet } as any,
    });
    expect(result.allowed).toBe(true);
    expect(result.config.sources).toContain("kv");
  });

  test("unions env and KV allowlists", async () => {
    mockGet.mockResolvedValue(JSON.stringify([777]));
    const config = await loadChatIdAllowlist({
      TELEGRAM_ALLOWED_CHAT_IDS: "111",
      CONFIG_KV: { get: mockGet } as any,
    });
    expect(config.allowedIds.has("111")).toBe(true);
    expect(config.allowedIds.has("777")).toBe(true);
    expect(config.sources).toContain("env");
    expect(config.sources).toContain("kv");
  });

  test("rejects invalid chatId format even when allowlist configured", async () => {
    const result = await checkChatIdAllowlist("../evil", {
      TELEGRAM_ALLOWED_CHAT_IDS: "123",
    });
    expect(result.allowed).toBe(false);
    expect(result.reason).toContain("format is invalid");
  });

  test("ignores malformed KV JSON (env-only still works)", async () => {
    mockGet.mockResolvedValue("not-json");
    const result = await checkChatIdAllowlist("111", {
      TELEGRAM_ALLOWED_CHAT_IDS: "111",
      CONFIG_KV: { get: mockGet } as any,
    });
    expect(result.allowed).toBe(true);
  });

  test("fail-closed when only malformed KV present (no env)", async () => {
    mockGet.mockResolvedValue("not-json");
    const result = await checkChatIdAllowlist("111", {
      CONFIG_KV: { get: mockGet } as any,
    });
    expect(result.allowed).toBe(false);
    expect(result.reason).toContain("allowlist not configured");
  });
});
