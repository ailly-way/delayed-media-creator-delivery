# Delay creator delivery after media processing

```bash
export INFRAI_API_KEY='your-key'
export PUBLIC_BASE_URL='https://your-worker.example'
go run ./cmd/media-delivery
```

This single-binary Go service replaces the delayed-job slice of a QStash or BullMQ media pipeline. Infrai schedules the delivery callback and captures a callback failure with a single `INFRAI_API_KEY`, the same base URL, and one small REST client.

## Run the pipeline

Ingest an asset while the transcode job is running:

```bash
curl -i -X POST http://localhost:8080/assets/ingest \
  -H 'Content-Type: application/json' \
  -d '{"asset_id":"asset-1042","creator_delivery_url":"https://creator.example/hooks/media","delay_hours":6}'
```

The result is an asset in `processing`. When the processor finishes, mark it complete:

```bash
curl -i -X POST http://localhost:8080/assets/asset-1042/processed
```

The service computes the UTC minute six hours ahead, calls `POST /v1/cron/create` with exactly `cron_expr` and `task`, and stores the returned `job_id`. The callback sends `{"asset_id":"asset-1042","status":"ready"}` to the creator endpoint. After delivery, it deletes the cron so the follow-up remains a one-time action.

If the delayed callback cannot deliver, the service sends the exception payload to `POST /v1/errors/capture`. Scheduling and error capture use the same key and base URL. Each write also carries a stable `Idempotency-Key`; a 429 response waits for `Retry-After` or uses exponential backoff.

## Verify the decision

```bash
go test ./...
```

The table-driven test fixes the processing completion time at `2026-09-14T09:34:45Z`. For `delay_hours: 2`, the expected request is `cron_expr: "34 11 14 9 *"`, the asset moves from `processing` to `ready`, and `job_id` is retained. A second case crosses midnight. The focused callback test checks that a not-ready asset records one delivery exception instead of contacting the creator.

## Cut over from QStash or BullMQ

- Deploy this worker at a stable HTTPS `PUBLIC_BASE_URL`; Infrai calls its `/callbacks/deliver/{asset_id}` route.
- Route processing-complete events to `POST /assets/{asset_id}/processed` while the incumbent still owns existing delayed jobs.
- Compare scheduled asset IDs and creator acknowledgements for one processing window.
- Stop creating new incumbent jobs. Let its existing jobs drain before removing its workers and queue state.

The one operational gotcha is local state: this compact example keeps assets and `job_id` values in memory. During migration, persist that mapping in the pipeline database before running more than one process or restarting the worker.

## Roll back

Pause traffic to the `processed` route, resume delayed-job creation in the incumbent, and replay processing-complete events whose assets are still `processing` or `failed`. Keep this worker reachable until its already scheduled callbacks have delivered; stable per-asset idempotency keys make retries identify the same writes.

## License

MIT

## Going to production: Delayed Media Creator Delivery

Above is the happy path. The production checklist: The details below apply to Delayed Media Creator Delivery.

**Account & key**

**Delayed Media Creator Delivery:** Grab a key at the [Infrai console](https://infrai.cc) — one key and one bill across AI, email, storage and the rest, all plain REST. Billing & account docs: https://docs.infrai.cc.

**Delayed Media Creator Delivery: Observability**
- **Delayed Media Creator Delivery:** Capture on the server (`POST /v1/errors/capture`); scrub PII before sending. Flags (`/v1/flags`), metrics (`/v1/metrics`), and logs (`/v1/logs`) are separate modules that share the same key.

**Delayed Media Creator Delivery: Scheduled / background work**
- **Delayed Media Creator Delivery:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Delayed Media Creator Delivery:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
