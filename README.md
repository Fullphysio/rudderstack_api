# rudderstack_api

[![pub package](https://img.shields.io/pub/v/rudderstack_api.svg)](https://pub.dev/packages/rudderstack_api)
[![CI](https://github.com/Fullphysio/rudderstack_api/actions/workflows/ci.yml/badge.svg)](https://github.com/Fullphysio/rudderstack_api/actions/workflows/ci.yml)

A pure Dart server-side SDK for [RudderStack](https://www.rudderstack.com). Send `identify`, `track`, `page`, `screen`, `group` and `alias` events to your data plane in gzip-compressed batches, with retries — from Dart servers, CLI tools, Cloud Functions and Flutter apps alike.

**No Flutter dependency.** The official `rudder_sdk_flutter` declares the Flutter SDK, which pub refuses to resolve inside a plain Dart project, and is built for devices, not servers. This one is a port of the official Node.js server SDK.

Built by [Fullphysio](https://www.fullphysio.com) for its Dart Cloud Functions. Bug reports are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). This is an independent project, not affiliated with or endorsed by RudderStack.

```
dart pub add rudderstack_api
```

```dart
import 'package:rudderstack_api/rudderstack_api.dart';

final analytics = RudderAnalytics(
  'WRITE_KEY',
  dataPlaneUrl: Uri.parse('https://example.dataplane.rudderstack.com'),
);

final delivered = [
  analytics.identify(userId: 'user-42', traits: {'plan': 'pro'}),
  analytics.track(
    userId: 'user-42',
    event: 'Order Completed',
    properties: {'revenue': 49.9},
  ),
];

await analytics.flush(delivered); // throws RudderStackException if one failed
```

## Delivery

Calls queue the event and return at once. A batch leaves in the background
when `flushAt` events (default 20) or `maxBatchBytes` (450 KB) are queued, or
`flushInterval` (10 s) after the first queued event.

Each call returns a `Future` that completes when that event is delivered, or
fails with a `RudderStackException` when its batch is rejected. Awaiting it is
optional: a failure nobody awaits does not crash the isolate, and still reaches
`onError`. Projects that enable the `unawaited_futures` lint wrap
fire-and-forget calls in `unawaited(...)`.

A future settles only once its batch has been sent, by `flush()` or in the
background. Awaiting it before a flush can wait for `flushInterval`, or forever
when the interval is `Duration.zero`: pass it to `flush()` instead.

`flush(events)` sends everything still queued, waits for batches in flight,
then throws the `RudderStackException` of the first of `events` that was not
delivered. On a shared client, a request passes only its own events, so it
never sees another request's failure. `flush()` with no events never throws
for a failed delivery. `shutdown()` flushes and closes the HTTP client.

## On servers

Create one client per process and reuse it. On a platform that freezes or
recycles the instance once the response is sent — Cloud Run, Cloud Functions —
`await flush(events)` with the request's own events before returning: queued
events may otherwise never leave, and the throw fails the request, so the
platform retries, when they were not delivered:

```dart
RudderAnalytics? _analytics;
RudderAnalytics analytics() => _analytics ??= RudderAnalytics(
      writeKey,
      dataPlaneUrl: dataPlaneUrl,
      flushInterval: Duration.zero,
    );

Future<void> onUserCreated(String uid, Map<String, Object?> traits) async {
  await analytics().flush([
    analytics().identify(userId: uid, traits: traits),
  ]);
}
```

`flushInterval: Duration.zero` disables the timer, so nothing is sent while
the instance is frozen.

## Errors

Every failure is reported. Nothing is swallowed.

| What | How |
|---|---|
| Missing `userId`/`anonymousId`, `event`, `groupId` or `previousId` | `ArgumentError` at the call site |
| A property JSON cannot encode | `JsonUnsupportedObjectError` at the call site. `DateTime` is encoded as an ISO-8601 string, `NaN` and `Infinity` as `null`, as `JSON.stringify` does |
| A batch the data plane rejected or never answered | the futures of its events fail with `RudderStackException`, `flush(events)` throws it for those events, and `onError` is called once. `eventCount` is the batch size, which on a shared client includes other callers' events |
| More than `maxPendingEvents` events waiting or in flight | the event is dropped: its future fails and `onError` is called |

Connection errors, `429` and `5xx` are retried `retryCount` times (default 3)
with exponential backoff — 200 ms, 400 ms, 800 ms — or the `Retry-After`
header, in seconds, when it is longer, up to 30 s. Timeouts (`timeout`, default
10 s per attempt) and other statuses are not retried. A retried batch keeps its
`messageId`s, so RudderStack can deduplicate it.

## Options compared with the Node SDK

| Node option | Here |
|---|---|
| `flushAt`, `flushInterval`, `retryCount`, `timeout`, `gzip`, `errorHandler` | `flushAt`, `flushInterval`, `retryCount`, `timeout`, `gzip`, `onError` |
| `maxQueueSize` (bytes, 450 KB) | `maxBatchBytes` |
| `maxInternalQueueSize` (events, 20000) | `maxPendingEvents` |
| `enable` | `enabled` |
| `dataPlaneUrl` (string, defaults to the retired hosted URL) | `dataPlaneUrl` (`Uri`, required) |
| `host`, `path`, `axiosConfig`, `axiosInstance`, `axiosRetryConfig`, `logLevel` | `httpClient`, `retryDelay`; no logger |

## Relationship to @rudderstack/rudder-sdk-node

This is a Dart port of the official Node.js SDK, `@rudderstack/rudder-sdk-node`
3.0.3 (MIT). Message shape, validation messages, gzip, Basic auth and retry
conditions follow that implementation, and the test suite compares this
client's requests against fixtures captured from it. See `THIRD_PARTY_NOTICES`.

Deliberate differences:

- **Futures instead of callbacks.** Each call returns a future for its event,
  the Dart form of the Node SDK's per-message callback. `flush()` drains the
  whole queue, where the Node SDK sends one batch per call, and rejects only for
  the events passed to it.
- **No unhandled errors.** The Node SDK flushes the first event from inside
  `track()` without awaiting it, so a failed delivery becomes an unhandled
  rejection.
- **`dataPlaneUrl` is required.** The Node SDK falls back to the retired
  `hosted.rudderlabs.com`.
- **A 10 s per-attempt timeout by default.** The Node SDK has none.
- **`Retry-After` is read in seconds and capped at 30 s.** The Node SDK also
  accepts an HTTP date and waits however long it says, which would stall every
  later batch.
- **Batches are also split by size**, so one request never exceeds
  `maxBatchBytes`.
- **`context.library` names this package** (`rudderstack_api`), and there is no
  Node-specific `_metadata.nodeVersion`.
- **No Redis persistence queue** (`createPersistenceQueue`).
- **No gzip on the web**, where `dart:io` is unavailable.
