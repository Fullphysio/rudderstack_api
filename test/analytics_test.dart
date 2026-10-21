// Fire-and-forget event calls are a supported use, exercised on purpose.
// ignore_for_file: unawaited_futures
@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:rudderstack_api/rudderstack_api.dart';
import 'package:rudderstack_api/src/analytics.dart'
    show defaultRetryDelay, formatTimestamp, parseRetryAfter;
import 'package:test/test.dart';

import '_support/recording_client.dart';

RudderAnalytics _client(
  http.Client httpClient, {
  int flushAt = 20,
  Duration flushInterval = Duration.zero,
  int maxBatchBytes = 450 * 1024,
  int maxPendingEvents = 20000,
  int retryCount = 3,
  Duration timeout = const Duration(seconds: 10),
  bool gzip = true,
  bool enabled = true,
  RudderStackErrorHandler? onError,
}) =>
    RudderAnalytics(
      'key',
      dataPlaneUrl: Uri.parse('https://example.dataplane.rudderstack.com'),
      flushAt: flushAt,
      flushInterval: flushInterval,
      maxBatchBytes: maxBatchBytes,
      maxPendingEvents: maxPendingEvents,
      retryCount: retryCount,
      retryDelay: (_) => Duration.zero,
      timeout: timeout,
      gzip: gzip,
      enabled: enabled,
      onError: onError,
      httpClient: httpClient,
    );

List<Future<void>> _trackMany(RudderAnalytics analytics, int count) => [
      for (var i = 0; i < count; i++)
        analytics.track(userId: 'u', event: 'E$i'),
    ];

void main() {
  group('constructor', () {
    test('posts to <dataPlaneUrl>/v1/batch, ignoring trailing slashes',
        () async {
      final http = RecordingClient();
      final analytics = RudderAnalytics('key',
          dataPlaneUrl: Uri.parse('https://dp.example.com/proxy//'),
          httpClient: http);
      analytics.track(userId: 'u', event: 'E');
      await analytics.flush();
      expect(http.requests.single.request.url.toString(),
          'https://dp.example.com/proxy/v1/batch');
    });

    test('rejects an empty write key and a relative data plane URL', () {
      final url = Uri.parse('https://dp.example.com');
      expect(() => RudderAnalytics('', dataPlaneUrl: url), throwsArgumentError);
      expect(() => RudderAnalytics('k', dataPlaneUrl: Uri.parse('/v1')),
          throwsArgumentError);
      expect(
          () => RudderAnalytics('k',
              dataPlaneUrl: Uri.parse('ftp://dp.example.com')),
          throwsArgumentError);
    });

    test('rejects out-of-range numeric options', () {
      final url = Uri.parse('https://dp.example.com');
      expect(() => RudderAnalytics('k', dataPlaneUrl: url, flushAt: 0),
          throwsArgumentError);
      expect(() => RudderAnalytics('k', dataPlaneUrl: url, retryCount: -1),
          throwsArgumentError);
      expect(
          () => RudderAnalytics('k', dataPlaneUrl: url, timeout: Duration.zero),
          throwsArgumentError);
    });
  });

  group('batching', () {
    test('sends nothing until flush when the interval is disabled', () async {
      final http = RecordingClient();
      final analytics = _client(http);
      _trackMany(analytics, 5);
      await pumpEventQueue();
      expect(http.requests, isEmpty);

      await analytics.flush();
      expect(http.requests.single.batch, hasLength(5));
    });

    test('sends a batch in the background once flushAt is reached', () async {
      final http = RecordingClient();
      final analytics = _client(http, flushAt: 3);
      _trackMany(analytics, 7);
      await pumpEventQueue();
      expect([for (final r in http.requests) r.batch.length], [3, 3]);

      await analytics.flush();
      expect([for (final r in http.requests) r.batch.length], [3, 3, 1]);
      expect([for (final m in http.messages) m['event']],
          [for (var i = 0; i < 7; i++) 'E$i']);
    });

    test('splits batches to stay under maxBatchBytes', () async {
      final http = RecordingClient();
      final analytics = _client(http, maxBatchBytes: 1200);
      for (var i = 0; i < 6; i++) {
        analytics
            .track(userId: 'u', event: 'E', properties: {'pad': 'x' * 300});
      }
      await analytics.flush();
      expect(http.requests.length, greaterThan(1));
      for (final request in http.requests) {
        final bytes = utf8.encode(jsonEncode(request.batch)).length;
        expect(bytes, lessThanOrEqualTo(1200 + 100 * request.batch.length));
      }
      expect(http.messages, hasLength(6));
    });

    test('sends a message larger than maxBatchBytes on its own', () async {
      final http = RecordingClient();
      final analytics = _client(http, maxBatchBytes: 100);
      analytics.track(userId: 'u', event: 'E', properties: {'pad': 'x' * 500});
      await analytics.flush();
      expect(http.requests.single.batch, hasLength(1));
    });

    test('flushes on the interval timer', () async {
      final http = RecordingClient();
      final analytics =
          _client(http, flushInterval: const Duration(milliseconds: 20));
      _trackMany(analytics, 2);
      expect(http.requests, isEmpty);
      await Future<void>.delayed(const Duration(milliseconds: 80));
      expect(http.requests.single.batch, hasLength(2));
      await analytics.shutdown();
    });

    test('flush waits for batches already in flight', () async {
      final release = Completer<void>();
      var received = 0;
      final analytics = _client(
        MockClient((request) async {
          received++;
          await release.future;
          return http.Response('OK', 200);
        }),
        flushAt: 1,
      );
      analytics.track(userId: 'u', event: 'E');
      await pumpEventQueue();
      expect(received, 1);

      var flushed = false;
      final flush = analytics.flush().then((_) => flushed = true);
      await pumpEventQueue();
      expect(flushed, isFalse);
      release.complete();
      await flush;
      expect(flushed, isTrue);
    });
  });

  group('message', () {
    test('is a snapshot taken at call time', () async {
      final http = RecordingClient();
      final analytics = _client(http);
      final properties = <String, Object?>{'count': 1};
      analytics.track(userId: 'u', event: 'E', properties: properties);
      properties['count'] = 2;
      await analytics.flush();
      expect(http.messages.single['properties'], {'count': 1});
    });

    test('encodes DateTime values as JavaScript does', () async {
      final http = RecordingClient();
      final analytics = _client(http);
      analytics.track(
        userId: 'u',
        event: 'E',
        properties: {'at': DateTime.utc(2026, 1, 31, 9, 5, 3, 42, 999)},
        timestamp: DateTime.utc(2026, 1, 31, 9, 5, 3, 42),
      );
      await analytics.flush();
      expect(http.messages.single['properties'],
          {'at': '2026-01-31T09:05:03.042Z'});
      expect(http.messages.single['timestamp'], '2026-01-31T09:05:03.042Z');
    });

    test('encodes NaN and Infinity as null, as JSON.stringify does', () async {
      final http = RecordingClient();
      final analytics = _client(http);
      analytics.track(
        userId: 'u',
        event: 'E',
        properties: {'nan': double.nan, 'inf': double.infinity, 'n': 1.5},
      );
      await analytics.flush();
      expect(http.messages.single['properties'],
          {'nan': null, 'inf': null, 'n': 1.5});
    });

    test('throws at the call site for a value JSON cannot encode', () {
      final analytics = _client(RecordingClient());
      expect(
        () => analytics
            .track(userId: 'u', event: 'E', properties: {'x': Object()}),
        throwsA(isA<JsonUnsupportedObjectError>()),
      );
    });

    test('keeps a caller-supplied messageId', () async {
      final http = RecordingClient();
      final analytics = _client(http);
      analytics.track(userId: 'u', event: 'E', messageId: 'my-id');
      await analytics.flush();
      expect(http.messages.single['messageId'], 'my-id');
    });

    test('generates distinct messageIds', () async {
      final http = RecordingClient();
      final analytics = _client(http);
      _trackMany(analytics, 50);
      await analytics.flush();
      expect({for (final m in http.messages) m['messageId']}, hasLength(50));
    });

    test('is plain JSON when gzip is off', () async {
      final http = RecordingClient();
      final analytics = _client(http, gzip: false);
      analytics.track(userId: 'u', event: 'E');
      await analytics.flush();
      expect(http.requests.single.request.headers['Content-Encoding'], isNull);
      expect(http.requests.single.request.headers['Content-Type'],
          startsWith('application/json'));
    });
  });

  group('failures', () {
    test('the event future fails with its batch, flush does not throw',
        () async {
      final http = RecordingClient(statuses: [400, 200]);
      final analytics = _client(http);
      final rejected = analytics.track(userId: 'u', event: 'E');
      await analytics.flush();
      await expectLater(
        rejected,
        throwsA(isA<RudderStackException>()
            .having((e) => e.statusCode, 'statusCode', 400)
            .having((e) => e.responseBody, 'responseBody', 'error')
            .having((e) => e.eventCount, 'eventCount', 1)),
      );
      final accepted = analytics.track(userId: 'u', event: 'E');
      await analytics.flush();
      await accepted;
    });

    test('flush(events) throws for the events passed, only those', () async {
      final analytics = _client(
        MockClient((request) async {
          final body = utf8.decode(gzip.decode(request.bodyBytes));
          return http.Response('', body.contains('"B"') ? 400 : 200);
        }),
        flushAt: 1,
      );
      final fromA = analytics.track(userId: 'a', event: 'A');
      final fromB = analytics.track(userId: 'b', event: 'B');
      await analytics.flush([fromA]);
      await expectLater(
        analytics.flush([fromB]),
        throwsA(isA<RudderStackException>()
            .having((e) => e.statusCode, 'statusCode', 400)),
      );
      await analytics.flush();
    });

    test('interleaved callers each see their own outcome', () async {
      final release = Completer<void>();
      final analytics = _client(
        MockClient((request) async {
          final body = utf8.decode(gzip.decode(request.bodyBytes));
          if (body.contains('"A"')) await release.future;
          return http.Response('', body.contains('"B"') ? 503 : 200);
        }),
        flushAt: 1,
        retryCount: 0,
      );
      final fromB = analytics.track(userId: 'b', event: 'B');
      final fromA = analytics.track(userId: 'a', event: 'A');
      final flushA = analytics.flush([fromA]);
      await expectLater(
          analytics.flush([fromB]), throwsA(isA<RudderStackException>()));
      await pumpEventQueue();
      var aDone = false;
      unawaited(flushA.then((_) => aDone = true));
      await pumpEventQueue();
      expect(aDone, isFalse, reason: "A's batch is still in flight");
      release.complete();
      await flushA;
    });

    test('flush(events) evaluates a lazy iterable before sending', () async {
      final http = RecordingClient();
      final analytics = _client(http);
      await analytics.flush(
          ['A', 'B'].map((name) => analytics.track(userId: 'u', event: name)));
      expect([for (final m in http.messages) m['event']], ['A', 'B']);
    });

    test('flush(events) does not wait for batches after its own', () async {
      final release = Completer<void>();
      final analytics = _client(
        MockClient((request) async {
          final body = utf8.decode(gzip.decode(request.bodyBytes));
          if (body.contains('"B"')) await release.future;
          return http.Response('', 200);
        }),
        flushAt: 1,
      );
      final fromA = analytics.track(userId: 'a', event: 'A');
      analytics.track(userId: 'b', event: 'B');
      await analytics.flush([fromA]);
      release.complete();
      await analytics.flush();
    });

    test('flush(events) during shutdown waits instead of throwing', () async {
      final http = RecordingClient();
      final analytics = _client(http);
      final delivered = analytics.track(userId: 'u', event: 'E');
      final shutdown = analytics.shutdown();
      await analytics.flush([delivered]);
      await shutdown;
      expect(http.messages, hasLength(1));
    });

    test('an event future settles only once its batch is sent', () async {
      final http = RecordingClient();
      final analytics = _client(http);
      var settled = false;
      final delivered =
          analytics.track(userId: 'u', event: 'E').then((_) => settled = true);
      await pumpEventQueue();
      expect(settled, isFalse);
      expect(http.requests, isEmpty);
      await analytics.flush([delivered]);
      expect(settled, isTrue);
    });

    test('a caller only sees the failure of its own events', () async {
      final analytics = _client(
        MockClient((request) async {
          final body = utf8.decode(gzip.decode(request.bodyBytes));
          return http.Response('', body.contains('"B"') ? 400 : 200);
        }),
        flushAt: 1,
      );
      final fromA = analytics.track(userId: 'a', event: 'A');
      final fromB = analytics.track(userId: 'b', event: 'B');
      await analytics.flush();
      await fromA;
      await expectLater(fromB, throwsA(isA<RudderStackException>()));
    });

    test('an unawaited failure reaches onError without escaping', () async {
      final errors = <RudderStackException>[];
      final analytics =
          _client(RecordingClient(statuses: [400]), onError: errors.add);
      _trackMany(analytics, 3);
      await analytics.flush();
      expect(errors.single.eventCount, 3);
    });

    test('a background failure fails the events it carried', () async {
      final http = RecordingClient(statuses: [500, 500, 500, 500, 200]);
      final analytics = _client(http, flushAt: 2);
      final first = _trackMany(analytics, 2);
      await pumpEventQueue();
      expect(http.requests, hasLength(4));
      for (final delivered in first) {
        await expectLater(
            delivered,
            throwsA(isA<RudderStackException>()
                .having((e) => e.statusCode, 'statusCode', 500)
                .having((e) => e.eventCount, 'eventCount', 2)));
      }
      final next = analytics.track(userId: 'u', event: 'E');
      await analytics.flush();
      await next;
    });

    test('retries connection errors', () async {
      var calls = 0;
      final analytics = _client(MockClient((request) async {
        if (++calls < 3) throw http.ClientException('reset');
        return http.Response('OK', 200);
      }));
      final delivered = analytics.track(userId: 'u', event: 'E');
      await analytics.flush();
      await delivered;
      expect(calls, 3);
    });

    test('reports a connection error after the last retry', () async {
      var calls = 0;
      final analytics = _client(
        MockClient((request) async {
          calls++;
          throw http.ClientException('reset');
        }),
        retryCount: 1,
      );
      final delivered = analytics.track(userId: 'u', event: 'E');
      await analytics.flush();
      await expectLater(
        delivered,
        throwsA(isA<RudderStackException>()
            .having((e) => e.statusCode, 'statusCode', isNull)
            .having((e) => e.cause, 'cause', isA<http.ClientException>())),
      );
      expect(calls, 2);
    });

    test('does not retry a timeout', () async {
      var calls = 0;
      final analytics = _client(
        MockClient((request) async {
          calls++;
          await Future<void>.delayed(const Duration(seconds: 1));
          return http.Response('OK', 200);
        }),
        timeout: const Duration(milliseconds: 20),
      );
      final delivered = analytics.track(userId: 'u', event: 'E');
      await analytics.flush();
      await expectLater(
        delivered,
        throwsA(isA<RudderStackException>()
            .having((e) => e.message, 'message', contains('timed out'))),
      );
      expect(calls, 1);
    });

    test('waits for a Retry-After longer than the backoff', () async {
      final http = RecordingClient(
          statuses: [429, 200], headers: {'retry-after': '0.2'});
      final analytics = _client(http);
      final delivered = analytics.track(userId: 'u', event: 'E');
      final stopwatch = Stopwatch()..start();
      await analytics.flush();
      await delivered;
      expect(stopwatch.elapsedMilliseconds, greaterThanOrEqualTo(190));
      expect(http.requests, hasLength(2));
    });

    test('a non-finite Retry-After falls back to the backoff', () async {
      final http = RecordingClient(
          statuses: [503, 200], headers: {'retry-after': 'Infinity'});
      final analytics = _client(http);
      final delivered = analytics.track(userId: 'u', event: 'E');
      await analytics.flush();
      await delivered;
      expect(http.requests, hasLength(2));
    });

    test('drops events beyond maxPendingEvents and reports them', () async {
      final http = RecordingClient();
      final errors = <RudderStackException>[];
      final analytics = _client(http, maxPendingEvents: 2, onError: errors.add);
      final delivered = _trackMany(analytics, 3);
      expect(errors.single.message, contains('Dropped a "track" event'));
      await analytics.flush();
      await Future.wait(delivered.take(2));
      await expectLater(delivered.last, throwsA(isA<RudderStackException>()));
      expect(http.messages, hasLength(2));
    });

    test('an onError that throws does not stop delivery', () async {
      final uncaught = <Object>[];
      await runZonedGuarded(() async {
        final http = RecordingClient(statuses: [400, 200]);
        final analytics = _client(
          http,
          flushAt: 1,
          onError: (_) => throw StateError('handler bug'),
        );
        final delivered = _trackMany(analytics, 2);
        await analytics.flush();
        await delivered.last;
        expect(http.requests, hasLength(2));
      }, (error, _) => uncaught.add(error));
      expect(uncaught.single, isA<StateError>());
    });
  });

  group('lifecycle', () {
    test('shutdown flushes, and later events throw', () async {
      final http = RecordingClient();
      final analytics = _client(http);
      _trackMany(analytics, 3);
      final shutdown = analytics.shutdown();
      expect(analytics.shutdown(), same(shutdown));
      await shutdown;
      expect(http.messages, hasLength(3));
      expect(() => analytics.track(userId: 'u', event: 'E'), throwsStateError);
      await analytics.flush();
    });

    test('shutdown does not throw when a batch fails', () async {
      final analytics = _client(RecordingClient(statuses: [400]));
      _trackMany(analytics, 2);
      await analytics.shutdown();
    });

    test('a disabled client validates but sends nothing', () async {
      final http = RecordingClient();
      final analytics = _client(http, enabled: false);
      _trackMany(analytics, 3);
      expect(() => analytics.track(event: 'E'), throwsArgumentError);
      await analytics.flush([analytics.track(userId: 'u', event: 'E')]);
      expect(http.requests, isEmpty);
    });
  });

  test('formatTimestamp matches Date.toJSON', () {
    expect(formatTimestamp(DateTime.utc(7, 1, 2, 3, 4, 5, 6)),
        '0007-01-02T03:04:05.006Z');
    expect(formatTimestamp(DateTime.utc(2026, 12, 31, 23, 59, 59, 999, 999)),
        '2026-12-31T23:59:59.999Z');
  });

  test('parseRetryAfter reads finite seconds and caps them at 30 s', () {
    expect(parseRetryAfter('1.5'), const Duration(milliseconds: 1500));
    expect(parseRetryAfter('-3'), Duration.zero);
    expect(parseRetryAfter('3600'), const Duration(seconds: 30));
    expect(parseRetryAfter('1e306'), const Duration(seconds: 30));
    expect(parseRetryAfter('9e15'), const Duration(seconds: 30));
    expect(parseRetryAfter('1e400'), isNull);
    expect(parseRetryAfter('NaN'), isNull);
    expect(parseRetryAfter('Wed, 21 Oct 2015 07:28:00 GMT'), isNull);
  });

  test('defaultRetryDelay doubles from 200 ms with up to 20 % jitter', () {
    for (final (retry, base) in [(1, 200), (2, 400), (3, 800)]) {
      final delay = defaultRetryDelay(retry).inMicroseconds / 1000;
      expect(delay, inInclusiveRange(base, base * 1.2));
    }
  });
}
