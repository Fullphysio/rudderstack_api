@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'package:rudderstack_api/rudderstack_api.dart';
import 'package:test/test.dart';

/// Runs the client against a real HTTP server on the loopback interface, with
/// its default `http.Client`, so compression, headers and timeouts are
/// exercised on the wire rather than through a mock.
void main() {
  late HttpServer server;
  late List<(HttpHeaders, List<int>)> received;
  late FutureOr<void> Function(HttpRequest request) respond;

  setUp(() async {
    received = [];
    respond = (request) => request.response.statusCode = 200;
    server = await HttpServer.bind(InternetAddress.loopbackIPv4, 0);
    server.listen((request) async {
      final body = await request.fold<List<int>>([], (a, b) => a..addAll(b));
      received.add((request.headers, body));
      await respond(request);
      await request.response.close();
    });
  });

  tearDown(() => server.close(force: true));

  RudderAnalytics client({Duration timeout = const Duration(seconds: 5)}) =>
      RudderAnalytics(
        'write-key',
        dataPlaneUrl: Uri.parse('http://127.0.0.1:${server.port}'),
        flushInterval: Duration.zero,
        retryCount: 0,
        timeout: timeout,
      );

  test('sends a gzip body with Basic auth and a User-Agent', () async {
    final analytics = client()
      ..identify(userId: 'u', traits: {'name': 'Ünïcode ✓'})
      ..track(userId: 'u', event: 'E');
    await analytics.shutdown();

    final (headers, body) = received.single;
    expect(headers.value('content-encoding'), 'gzip');
    expect(headers.value('authorization'),
        'Basic ${base64Encode(utf8.encode('write-key:'))}');
    expect(headers.value('user-agent'), 'rudderstack_api/$packageVersion');
    final json = jsonDecode(utf8.decode(gzip.decode(body))) as Map;
    final batch = json['batch'] as List;
    expect(batch, hasLength(2));
    expect((batch.first as Map)['traits'], {'name': 'Ünïcode ✓'});
  });

  test('surfaces the status and body of a rejected batch', () async {
    respond = (request) {
      request.response
        ..statusCode = 401
        ..write('invalid write key');
    };
    final analytics = client()..track(userId: 'u', event: 'E');
    await expectLater(
      analytics.flush(),
      throwsA(isA<RudderStackException>()
          .having((e) => e.statusCode, 'statusCode', 401)
          .having((e) => e.responseBody, 'responseBody', 'invalid write key')),
    );
    await analytics.shutdown();
  });

  test('abandons a request that outlives the timeout', () async {
    final hang = Completer<void>();
    respond = (_) => hang.future;
    final analytics = client(timeout: const Duration(milliseconds: 200))
      ..track(userId: 'u', event: 'E');
    final stopwatch = Stopwatch()..start();
    await expectLater(analytics.flush(), throwsA(isA<RudderStackException>()));
    expect(stopwatch.elapsed, lessThan(const Duration(seconds: 2)));
    hang.complete();
    await analytics.shutdown();
  });
}
