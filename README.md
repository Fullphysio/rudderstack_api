# rudderstack_api

[![pub package](https://img.shields.io/pub/v/rudderstack_api.svg)](https://pub.dev/packages/rudderstack_api)
[![CI](https://github.com/Fullphysio/rudderstack_api/actions/workflows/ci.yml/badge.svg)](https://github.com/Fullphysio/rudderstack_api/actions/workflows/ci.yml)

A pure Dart server-side SDK for [RudderStack](https://www.rudderstack.com). Send `identify`, `track`, `page`, `screen`, `group` and `alias` events to your data plane in gzip-compressed batches, with retries — from Dart servers, CLI tools, Cloud Functions and Flutter apps alike.

**No Flutter dependency.** The official `rudder_sdk_flutter` declares the Flutter SDK, which pub refuses to resolve inside a plain Dart project, and is built for devices, not servers. This one is a port of the official Node.js server SDK.

Used in production at [Fullphysio](https://www.fullphysio.com), where it runs inside our Dart Cloud Functions. Bug reports are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md).

```dart
import 'package:rudderstack_api/rudderstack_api.dart';

final analytics = RudderAnalytics(
  'WRITE_KEY',
  dataPlaneUrl: Uri.parse('https://example.dataplane.rudderstack.com'),
);

analytics.identify(userId: 'user-42', traits: {'plan': 'pro'});
analytics.track(
  userId: 'user-42',
  event: 'Order Completed',
  properties: {'revenue': 49.9},
);

await analytics.flush();
```

## Delivery

Calls return immediately and only queue the event. A batch leaves in the
background when `flushAt` events (default 20) or `maxBatchBytes` (450 KB) are
queued, or `flushInterval` (10 s) after the first queued event.

`flush()` sends everything still queued, waits for batches already in flight,
and throws a `RudderStackException` if any batch failed since the previous
flush — including background ones. `shutdown()` flushes and closes the HTTP
client.

## On servers

Create one client per process and reuse it. On a platform that freezes or
recycles the instance once the response is sent — Cloud Run, Cloud Functions —
`await flush()` before returning, or queued events may never leave:

```dart
RudderAnalytics? _analytics;
RudderAnalytics analytics() => _analytics ??= RudderAnalytics(
      writeKey,
      dataPlaneUrl: dataPlaneUrl,
      flushInterval: Duration.zero,
    );

Future<void> onUserCreated(String uid, Map<String, Object?> traits) async {
  analytics().identify(userId: uid, traits: traits);
  await analytics().flush();
}
```

`flushInterval: Duration.zero` disables the timer, so nothing is sent while
the instance is frozen.

## Errors

Every failure is reported. Nothing is swallowed.

| What | How |
|---|---|
| Missing `userId`/`anonymousId`, `event`, `groupId` or `previousId` | `ArgumentError` at the call site |
| A property JSON cannot encode | `JsonUnsupportedObjectError` at the call site. `DateTime` is encoded as an ISO-8601 string |
| A batch the data plane rejected or never answered | `RudderStackException` from the next `flush()`, and `onError` if set |
| More than `maxQueueSize` events waiting | the event is dropped and reported like a failed batch |

Connection errors, `429` and `5xx` are retried `retryCount` times (default 3)
with exponential backoff — 200 ms, 400 ms, 800 ms — or the `Retry-After`
header when it is longer. Timeouts (`timeout`, default 10 s per attempt) and
other statuses are not retried. A retried batch keeps its `messageId`s, so
RudderStack can deduplicate it.

## Relationship to @rudderstack/rudder-sdk-node

This is a Dart port of the official Node.js SDK, `@rudderstack/rudder-sdk-node`
3.0.3 (MIT). Message shape, validation messages, gzip, Basic auth and retry
conditions follow that implementation, and the test suite compares this
client's requests against fixtures captured from it. See `THIRD_PARTY_NOTICES`.

Deliberate differences:

- **`flush()` drains the whole queue and throws on failure.** The Node SDK
  sends one batch per call and reports failures through callbacks. There are no
  per-call callbacks here; `await flush()` or `onError` replace them.
- **No unhandled errors.** The Node SDK flushes the first event from inside
  `track()` without awaiting it, so a failed delivery becomes an unhandled
  rejection. Here background failures are held for the next `flush()`.
- **`dataPlaneUrl` is required.** The Node SDK falls back to the retired
  `hosted.rudderlabs.com`.
- **A 10 s per-attempt timeout by default.** The Node SDK has none.
- **Batches are also split by size**, so one request never exceeds
  `maxBatchBytes`.
- **`context.library` names this package** (`rudderstack_api`), and there is no
  Node-specific `_metadata.nodeVersion`.
- **No Redis persistence queue** (`createPersistenceQueue`).
- **No gzip on the web**, where `dart:io` is unavailable.
