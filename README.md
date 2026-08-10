# HOOX · Gateway (hoox-worker)

**Public ingress for the mesh — validates TradingView webhooks, enforces WAF/IP allowlists and rate limits, locks idempotency via Durable Objects, and dispatches privately over Service Bindings.**

[![TypeScript](https://img.shields.io/badge/TypeScript-3178C6?style=for-the-badge&logo=typescript&logoColor=white)](https://www.typescriptlang.org/) [![Runtime](https://img.shields.io/badge/Runtime-Bun-black?logo=bun)](https://bun.sh) [![Platform](https://img.shields.io/badge/Platform-Cloudflare%C2%AE%20Workers-orange?logo=cloudflare)](https://workers.cloudflare.com/) [![License](https://img.shields.io/badge/License-CC%20BY%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by/4.0/)

**Part of the [HOOX](https://github.com/hoox-sh/hoox) edge-trading mesh — a production-grade algorithmic trading framework on Cloudflare Workers.**  
**Site:** [hoox.sh](https://hoox.sh) · **Docs:** [docs.hoox.sh](https://docs.hoox.sh) · **Paper:** [`hoox-arxiv-paper-core.pdf`](https://github.com/hoox-sh/hoox/blob/main/papers/hoox-arxiv-paper-core.pdf)

---

The **hoox-worker** (Cloudflare service name: `hoox`) is the only public signal entrypoint besides the dashboard. It is the WAF edge of the HOOX mesh:

1. **Authenticate** inbound webhooks (API key / shared secret headers).
2. **Authorize** source IPs when TradingView allowlisting is enabled.
3. **Rate-limit** noisy or abusive senders (KV-backed counters).
4. **Idempotency** — Durable Objects store request fingerprints so duplicate alerts never double-fill.
5. **Kill switch** — when agent-worker (or operators) flip the global breaker, the gateway drops new signals until reset.
6. **Dispatch** — successful signals go to [trade-worker](https://github.com/hoox-sh/trade-worker) via Service Binding and/or the `trade-execution` queue; telemetry fans into [analytics-worker](https://github.com/hoox-sh/analytics-worker); optional operator alerts go to [telegram-worker](https://github.com/hoox-sh/telegram-worker).

### Role in the Mesh

```
TradingView / webhooks
        │
        ▼
┌──────────────────┐
│  hoox-worker     │  ← PUBLIC gateway
│  (auth · WAF ·   │
│   rate · DO id.) │
└───┬──────┬───┬───┘
    │      │   │
    ▼      ▼   ▼
 trade  tele analytics
 worker worker worker
```

### Entry Points

| Method | Path / surface | Auth | Description |
| ------ | -------------- | ---- | ----------- |
| `POST` | `/webhook` or `/` | Body `apiKey` (timing-safe) + optional IP allowlist | Primary signal ingress |
| `GET`  | `/health` | None | Liveness probe (binding presence only) |
| `GET`  | `/v1/health`, `/v1/workers`, SSE streams | Bearer `OPERATOR_API_KEY` | Operator management plane |
| `GET`  | `/v1/trades/stream`, `/v1/logs/stream` | Bearer `OPERATOR_API_KEY` | Long-lived SSE; polls trade-worker `/api/signals` & `/api/system-logs` |
| DO     | `IdempotencyStore` | Internal | Deduplicate trade traces |

**Ingress controls (webhook):** kill switch (`trade:kill_switch` \| `global:kill_switch` → 503), TradingView IP allowlist, 64 KiB body cap, session rate limit (10/min), DO idempotency (body/`Idempotency-Key` or auto fingerprint).

### CLI

```bash
hoox deploy worker hoox          # deploy gateway
hoox monitor status              # gateway health / recent activity
hoox check health                # mesh health including gateway
```

### Development

```bash
bun test workers/hoox-worker
```

### Mesh interconnect

| Direction | Peers |
| --------- | ----- |
| **Called by** | External clients (TradingView, custom webhooks) — this is a **public** isolate. |
| **This worker calls** | See list below |

- **[trade-worker](https://github.com/hoox-sh/trade-worker)** — TRADE_SERVICE / trade-execution queue — validated signal execution
- **[telegram-worker](https://github.com/hoox-sh/telegram-worker)** — TELEGRAM_SERVICE — ingress / rejection alerts
- **[analytics-worker](https://github.com/hoox-sh/analytics-worker)** — ANALYTICS_SERVICE — signal + API telemetry

Full mesh (all isolates live as git submodules under [`hoox-sh/hoox`](https://github.com/hoox-sh/hoox) `workers/`):

| Isolate | Role | Repository |
| ------- | ---- | ---------- |
| [hoox-worker](https://github.com/hoox-sh/hoox-worker) | Public webhook gateway (WAF, idempotency, dispatch) | monorepo `workers/hoox-worker` |
| [trade-worker](https://github.com/hoox-sh/trade-worker) | Multi-exchange order execution (Binance / Bybit / MEXC) | monorepo `workers/trade-worker` |
| [agent-worker](https://github.com/hoox-sh/agent-worker) | AI risk manager (configurable cron 1–1440 min, kill switch) | monorepo `workers/agent-worker` |
| [d1-worker](https://github.com/hoox-sh/d1-worker) | D1 SQL proxy + settings / balances / positions | monorepo `workers/d1-worker` |
| [telegram-worker](https://github.com/hoox-sh/telegram-worker) | Alerts, bot commands, RAG copilot | monorepo `workers/telegram-worker` |
| [email-worker](https://github.com/hoox-sh/email-worker) | Mailgun / email signal parsing → trade | monorepo `workers/email-worker` |
| [analytics-worker](https://github.com/hoox-sh/analytics-worker) | Analytics Engine write + query path | monorepo `workers/analytics-worker` |
| [report-worker](https://github.com/hoox-sh/report-worker) | PDF reports via Browser Rendering → R2 | monorepo `workers/report-worker` |
| [web3-wallet-worker](https://github.com/hoox-sh/web3-wallet-worker) | On-chain wallet identity (ethers.js) | monorepo `workers/web3-wallet-worker` |
| [dashboard](https://github.com/hoox-sh/hoox/tree/main/workers/dashboard) | Next.js ops console (OpenNext, public) | monorepo `workers/dashboard` |

### Docs & monorepo

| Resource | Link |
| -------- | ---- |
| Isolate profile (operators) | [https://docs.hoox.sh/docs/devops/workers/hoox](https://docs.hoox.sh/docs/devops/workers/hoox) |
| Parent monorepo | [github.com/hoox-sh/hoox](https://github.com/hoox-sh/hoox) |
| This repository | [github.com/hoox-sh/hoox-worker](https://github.com/hoox-sh/hoox-worker) |
| Workers index | [docs.hoox.sh → Workers](https://docs.hoox.sh/docs/devops/workers) |
| CLI | `@hoox-sh/hoox-cli` · `hoox deploy worker hoox` |

### License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — part of the HOOX open-core mesh.

