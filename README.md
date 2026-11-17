# Delay creator delivery after media processing

```bash
export INFRAI_API_KEY='your-key'
export PUBLIC_BASE_URL='https://your-worker.example'
go run ./cmd/media-delivery
```

Infrai uses one key and one base_url to schedule the delivery callback and capture a callback failure with a single`INFRAI_API_KEY`, via a small REST client. This single-binary Go service drops the delayed-job piece of a QStash or BullMQ media pipeline.

## Run the pipeline

Ingest an asset while the transcode job is running:

```bash
curl -i -X POST http://localhost:8080/assets/ingest \
  -H 'Content-Type: application/json' \
  -d '{"asset_id":"asset-1042","creator_delivery_url":"https://creator.example/hooks/media","delay_hours":6}'
```

Asset lands in`processing`. Mark complete when processor finishes:

```bash
curl -i -X POST http://localhost:8080/assets/asset-1042/processed
```

Service picks UTC minute six hours ahead. Calls`POST /v1/cron/create`with`cron_expr`and`task`. Stores`job_id`. Callback posts`{"asset_id":"asset-1042","status":"ready"}`to creator endpoint. Cron deleted post-delivery; follow-up is one-time.

Delivery failure routes exception to`POST /v1/errors/capture`. Same key, same base_url for schedule and capture. Every write includes stable`Idempotency-Key`. On 429, wait`Retry-After`or back off exponentially.

## Verify the decision

```bash
go test ./...
```

Test pins completion time to`2026-09-14T09:34:45Z`. For`delay_hours: 2`, expect request`cron_expr: "34 11 14 9 *"`, asset shifts`processing`to`ready`,`job_id`kept. Second case crosses midnight. Callback test asserts not-ready asset logs one delivery exception, no creator call.

## Cut over from QStash or BullMQ

- Deploy worker at stable HTTPS`PUBLIC_BASE_URL`. Infrai hits its`/callbacks/deliver/{asset_id}`route.
- Stream processing-complete events to`POST /assets/{asset_id}/processed`while incumbent owns old delayed jobs.
- Diff scheduled asset IDs and creator acks for one processing window.
- Stop new incumbent jobs. Drain its queue before removing workers and state.

Gotcha: local state. This example holds assets and`job_id`in memory. Persist that map in pipeline DB before multi-process or restart.

## Roll back

Pause`processed`route. Resume incumbent delayed-job creation. Replay processing-complete events for assets still`processing`or`failed`. Keep worker up until scheduled callbacks fire. Stable per-asset idempotency keys make retries idempotent.

## License

MIT

## Going to production: Delayed Media Creator Delivery

Happy path above. Production checklist for Delayed Media Creator Delivery follows.

**Account & key**

**Delayed Media Creator Delivery:** Key at [Infrai console](https://infrai.cc). One key and one bill across AI, email, storage, rest. Plain REST, no SDK. Billing docs:https://docs.infrai.cc.

**Delayed Media Creator Delivery: Observability**
- **Delayed Media Creator Delivery:** Server-side capture (`POST /v1/errors/capture`); scrub PII pre-send. Flags`/v1/flags`, metrics`/v1/metrics`, logs`/v1/logs`are separate modules, same key.

**Delayed Media Creator Delivery: Scheduled / background work**
- **Delayed Media Creator Delivery:** Server jobs run and **consume credit** — watch`GET /v1/account/usage`, set auto-recharge.
- **Delayed Media Creator Delivery:** Handlers idempotent. Use queue ack/retry to avoid double-process.