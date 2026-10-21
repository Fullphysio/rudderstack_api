import 'dart:convert';
import 'dart:io';

import 'package:rudderstack_api/rudderstack_api.dart';
import 'package:test/test.dart';

import '_support/recording_client.dart';

Map<String, Object?> _fixture(String name) {
  final json = jsonDecode(File('test/fixtures/$name.json').readAsStringSync());
  return (json as Map<String, Object?>)['cases']! as Map<String, Object?>;
}

Map<String, Object?>? _map(Object? value) =>
    (value as Map<String, Object?>?)?.cast<String, Object?>();

final _isoMillis = matches(RegExp(r'^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$'));
final _uuidV4 = matches(RegExp(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'));

/// Replaces the fields that differ on every run (ids, clocks) or by design
/// (library name, Node's `_metadata`) after checking their shape.
Map<String, Object?> _normalize(Map<String, Object?> message) {
  expect(message['messageId'], _uuidV4);
  expect(message['originalTimestamp'], _isoMillis);
  expect(message['sentAt'], _isoMillis);
  final context = Map<String, Object?>.of(_map(message['context'])!)
    ..remove('library');
  return Map.of(message)
    ..remove('messageId')
    ..remove('originalTimestamp')
    ..remove('sentAt')
    ..remove('_metadata')
    ..['context'] = context;
}

Object? _revive(Object? value) {
  if (value is String && RegExp(r'^\d{4}-\d\d-\d\dT').hasMatch(value)) {
    return DateTime.parse(value);
  }
  if (value is Map) {
    return {
      for (final entry in value.entries)
        entry.key as String: _revive(entry.value)
    };
  }
  return value;
}

void _call(RudderAnalytics analytics, String type, Map<String, Object?> raw) {
  final input = _revive(raw)! as Map<String, Object?>;
  final userId = input['userId'] as String?;
  final anonymousId = input['anonymousId'] as String?;
  final context = _map(input['context']);
  final integrations = _map(input['integrations']);
  final timestamp = input['timestamp'] as DateTime?;
  switch (type) {
    case 'identify':
      analytics.identify(
          userId: userId,
          anonymousId: anonymousId,
          traits: _map(input['traits']),
          context: context,
          integrations: integrations,
          timestamp: timestamp);
    case 'track':
      analytics.track(
          event: input['event'] as String? ?? '',
          userId: userId,
          anonymousId: anonymousId,
          properties: _map(input['properties']),
          context: context,
          integrations: integrations,
          timestamp: timestamp);
    case 'page':
      analytics.page(
          name: input['name'] as String?,
          category: input['category'] as String?,
          userId: userId,
          anonymousId: anonymousId,
          properties: _map(input['properties']),
          context: context,
          integrations: integrations,
          timestamp: timestamp);
    case 'screen':
      analytics.screen(
          name: input['name'] as String?,
          userId: userId,
          anonymousId: anonymousId,
          properties: _map(input['properties']),
          context: context,
          integrations: integrations,
          timestamp: timestamp);
    case 'group':
      analytics.group(
          groupId: input['groupId'] as String? ?? '',
          userId: userId,
          anonymousId: anonymousId,
          traits: _map(input['traits']),
          context: context,
          integrations: integrations,
          timestamp: timestamp);
    case 'alias':
      analytics.alias(
          userId: userId ?? '',
          previousId: input['previousId'] as String? ?? '',
          context: context,
          integrations: integrations,
          timestamp: timestamp);
    default:
      throw ArgumentError('Unknown type $type');
  }
}

RudderAnalytics _client(RecordingClient http) => RudderAnalytics(
      'test-write-key',
      dataPlaneUrl: Uri.parse('http://127.0.0.1:1/'),
      flushInterval: Duration.zero,
      retryDelay: (_) => Duration.zero,
      httpClient: http,
    );

void main() {
  group('matches the payload @rudderstack/rudder-sdk-node sends:', () {
    for (final MapEntry(key: name, value: raw)
        in _fixture('message_golden').entries) {
      test(name, () async {
        final golden = raw! as Map<String, Object?>;
        final request = _map(golden['request'])!;
        final goldenHeaders = _map(request['headers'])!;
        final goldenBody = _map(request['body'])!;

        final http = RecordingClient();
        final analytics = _client(http);
        _call(analytics, golden['type']! as String, _map(golden['input'])!);
        await analytics.flush();

        final sent = http.requests.single;
        expect(sent.request.method, request['method']);
        expect(sent.request.url.path, request['path']);
        expect(sent.request.headers['Authorization'],
            goldenHeaders['authorization']);
        expect(sent.request.headers['Content-Encoding'],
            goldenHeaders['content-encoding']);
        expect(sent.request.headers['User-Agent'],
            'rudderstack_api/$packageVersion');

        expect(sent.body.keys, unorderedEquals(goldenBody.keys));
        expect(sent.body['timestamp'], _isoMillis);
        expect(sent.body['sentAt'], sent.body['timestamp']);

        final message = sent.batch.single;
        expect(_map(message['context'])!['library'],
            {'name': 'rudderstack_api', 'version': packageVersion});
        final goldenMessage = _map((goldenBody['batch']! as List).single)!;
        expect(_normalize(message), _normalize(goldenMessage));
      });
    }
  });

  group('rejects the calls @rudderstack/rudder-sdk-node rejects:', () {
    for (final MapEntry(key: name, value: raw)
        in _fixture('validation_golden').entries) {
      test(name, () {
        final golden = raw! as Map<String, Object?>;
        final analytics = _client(RecordingClient());
        void call() =>
            _call(analytics, golden['type']! as String, _map(golden['input'])!);
        if (golden['threw'] == true) {
          expect(
              call,
              throwsA(isA<ArgumentError>()
                  .having((e) => e.message, 'message', golden['message'])));
        } else {
          call();
        }
      });
    }
  });

  group('retries like @rudderstack/rudder-sdk-node:', () {
    for (final MapEntry(key: name, value: raw)
        in _fixture('retry_golden').entries) {
      test(name, () async {
        final golden = raw! as Map<String, Object?>;
        final statuses = (golden['statuses']! as List).cast<int>();
        final http = RecordingClient(statuses: statuses);
        final analytics = _client(http)..track(userId: 'u', event: 'E');

        final flush = analytics.flush();
        if (golden['threw'] == true) {
          await expectLater(flush, throwsA(isA<RudderStackException>()));
        } else {
          await flush;
        }
        expect(http.requests, hasLength(golden['attempts']));
        final ids = {
          for (final request in http.requests) request.batch.single['messageId']
        };
        expect(ids, hasLength(1));
      });
    }
  });
}
