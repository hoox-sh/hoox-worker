/**
 * Copyright (c) 2026 HOOX · HOOX · jango-blockchained
 * SPDX-License-Identifier: Apache-2.0
 */

import { expect, jest, test, describe, beforeEach, afterEach } from "bun:test";

Object.assign(global, {
  expect,
  jest,
  test,
  describe,
  beforeEach,
  afterEach,
});

global.Response = Response;
global.Request = Request;
global.Headers = Headers;
