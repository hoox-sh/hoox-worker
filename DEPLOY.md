# hoox-worker — post-hardening deploy checklist

No secret values. Copy from your password manager / `hoox setup` output.

## 1. Wrangler config

- Copy `wrangler.jsonc.example` → `wrangler.jsonc` (or use CLI-generated config).
- Fill real KV / queue / Vectorize IDs from your account — **do not invent IDs**.
- Confirm Durable Objects + migrations:

```jsonc
"durable_objects": {
  "bindings": [
    { "name": "IDEMPOTENCY_STORE", "class_name": "IdempotencyStore" },
    { "name": "RATE_LIMITER", "class_name": "RateLimiterStore" }
  ]
},
"migrations": [
  { "tag": "v1", "new_sqlite_classes": ["IdempotencyStore"] },
  { "tag": "v2", "new_sqlite_classes": ["RateLimiterStore"] }
]
```

`RateLimiterStore` and `IdempotencyStore` are exported from `src/index.ts`.

## 2. Deploy (applies migrations)

```bash
# Preferred
hoox deploy worker hoox

# Or
wrangler deploy --config workers/hoox-worker/wrangler.jsonc
```

`wrangler deploy` applies new migration tags once. First deploy after adding `v2` registers `RateLimiterStore`.

## 3. Secrets (hoox)

```bash
wrangler secret put WEBHOOK_API_KEY_BINDING --config workers/hoox-worker/wrangler.jsonc
wrangler secret put INTERNAL_KEY_BINDING --config workers/hoox-worker/wrangler.jsonc
wrangler secret put OPERATOR_API_KEY --config workers/hoox-worker/wrangler.jsonc
# Notify chat allowlist — fail-closed when unset (comma-separated chat IDs)
wrangler secret put TELEGRAM_ALLOWED_CHAT_IDS --config workers/hoox-worker/wrangler.jsonc
# Optional: HA_TOKEN_BINDING, AUTHORIZED_CHAT_IDS (alias for allowlist)
```

Optional KV allowlist (union with env): `CONFIG_KV` key `telegram:allowed_chat_ids` = JSON array, e.g. `["123456789"]`.

## 4. telegram-worker secrets (aligned allowlists)

```bash
wrangler secret put TG_BOT_TOKEN_BINDING --config workers/telegram-worker/wrangler.jsonc
wrangler secret put TG_CHAT_ID_BINDING --config workers/telegram-worker/wrangler.jsonc
wrangler secret put TELEGRAM_SECRET_TOKEN --config workers/telegram-worker/wrangler.jsonc
wrangler secret put INTERNAL_KEY_BINDING --config workers/telegram-worker/wrangler.jsonc
wrangler secret put AUTHORIZED_CHAT_IDS --config workers/telegram-worker/wrangler.jsonc
```

Align `AUTHORIZED_CHAT_IDS` (telegram) with `TELEGRAM_ALLOWED_CHAT_IDS` (hoox) so gateway notify and bot commands share the same operator chats.

## 5. Verify

```bash
hoox check health
# GET /health on hoox should show bindings.rateLimiter: "configured"
# and bindings.idempotency: "configured"
```

## Secrets list (names only)

| Worker | Secret |
| ------ | ------ |
| hoox | `WEBHOOK_API_KEY_BINDING` |
| hoox | `INTERNAL_KEY_BINDING` |
| hoox | `OPERATOR_API_KEY` |
| hoox | `TELEGRAM_ALLOWED_CHAT_IDS` (or alias `AUTHORIZED_CHAT_IDS`) |
| hoox | `HA_TOKEN_BINDING` (optional) |
| telegram-worker | `INTERNAL_KEY_BINDING` |
| telegram-worker | `TG_BOT_TOKEN_BINDING` |
| telegram-worker | `TG_CHAT_ID_BINDING` |
| telegram-worker | `TELEGRAM_SECRET_TOKEN` |
| telegram-worker | `AUTHORIZED_CHAT_IDS` |
