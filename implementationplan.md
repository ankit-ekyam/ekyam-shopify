# Production Readiness Implementation Plan

This document outlines the architectural changes required to resolve the 5 critical production risks in the Ekyam Shopify integration.

## Goal Description

Resolve five critical issues preventing the current prototype from being safely deployed to production: unsecured custom API endpoints, consumer data loss on database errors, indefinite in-memory mapping cache retention, suboptimal local development webserver flags being used, and the lack of a Dead Letter Queue (DLQ) for failed mappings.

## Proposed Changes

### Configuration and Dependencies

#### [MODIFY] pyproject.toml
- Add `cachetools` to dependencies to provide safe, auto-expiring TTL caching.

#### [MODIFY] app/config/settings.py
- Add `admin_api_key` to `Settings` (defaulting to a dev value but configurable via `.env`) to secure internal endpoints.

---

### API Layer (Producer)

#### [MODIFY] producer.py
- Add `cachetools.TTLCache` with a 5-minute expiry for `destination_mapping_cache`.
- Create a `verify_api_key` dependency that checks the `X-API-Key` header against `settings.admin_api_key`.
- Apply `Depends(verify_api_key)` to `/sync/{entity}` and `/push-all-to-store/{entity}`.
- Remove the `--reload` flag from any internal documentation or commands we document, replacing it with `--workers 4`.

---

### Consumer Layer

#### [MODIFY] consumer.py
- Implement `cachetools.TTLCache` (5-minute expiry) for `mapping_cache` to ensure rule updates are picked up.
- **Reliability:** Refactor the MongoDB bulk write try/except block. Implement a robust retry loop with exponential backoff if `bulk_write` fails. If all retries fail, crash the consumer to prevent data skipping and offset commitment.
- **DLQ:** Initialize a Kafka `Producer`. Wrap the JSON parsing and `map_dynamic_source` execution in a try/except block. If it fails, push the raw message, error reason, and topic to `shopify.dlq`.

#### [MODIFY] push_consumer.py
- Implement similar reliability (retry logic on HTTP push failures or MongoDB read failures).
- Add DLQ logic for unprocessable push operations.

---

### Documentation

#### [MODIFY] README.md
- Update the **Quick Start** section to recommend `uvicorn producer:app --host 0.0.0.0 --port 8000 --workers 4` for production, and separate the local dev command.
- Document the new `X-API-Key` requirement for the sync/push endpoints.
- Explain the DLQ topic (`shopify.dlq`) and how to monitor it.

## User Review Required

> [!WARNING]
> **API Key Enforcement:** Once these changes are made, any existing tools or scripts calling `/sync/orders` or `/push-all-to-store/orders` will receive a 401 Unauthorized unless they include the `X-API-Key` header matching your `.env` file's `ADMIN_API_KEY`. 

> [!IMPORTANT]
> **Consumer Crashing:** By making the consumer crash on repeated database failures (a good thing for preventing data loss), your Docker configuration must include `restart: always` or `restart: unless-stopped` so it automatically comes back up when the database recovers.

## Open Questions

1. For the DLQ, should we create one unified DLQ topic (e.g. `shopify.dlq`) or a specific one for each entity (e.g. `shopify.orders.dlq`)? The plan currently uses a unified DLQ with an `original_topic` field injected.
2. Are you comfortable with the consumer crashing itself after 3 failed database connection attempts?

## Verification Plan

### Automated Verification
- Run `test_transformer.py` to ensure core logic remains unbroken.

### Manual Verification
1. Attempt to hit `/sync/orders` without an API key (should fail 401).
2. Attempt to hit it with the correct API key (should succeed).
3. Send a malformed JSON message to Kafka and verify it appears in the DLQ topic.
4. Temporarily shut down MongoDB while the consumer is running, ensure the consumer pauses, retries, and crashes rather than committing the offset.
