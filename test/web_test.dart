@TestOn('browser')
library;

import 'dart:convert';

import 'package:http/http.dart' as http;
import 'package:http/testing.dart';
import 'package:rudderstack_api/rudderstack_api.dart';
import 'package:test/test.dart';

void main() {
  test('sends plain JSON without gzip or a User-Agent on the web', () async {
    late http.Request sent;
    final analytics = RudderAnalytics(
      'key',
      dataPlaneUrl: Uri.parse('https://example.dataplane.rudderstack.com'),
      flushInterval: Duration.zero,
      httpClient: MockClient((request) async {
        sent = request;
        return http.Response('OK', 200);
      }),
    );
    final delivered = analytics.track(userId: 'u', event: 'E');
    await analytics.flush([delivered]);

    expect(sent.headers['Content-Encoding'], isNull);
    expect(sent.headers['User-Agent'], isNull);
    expect(sent.headers['Content-Type'], startsWith('application/json'));
    final body = jsonDecode(sent.body) as Map<String, Object?>;
    final message = (body['batch']! as List).single as Map<String, Object?>;
    expect(message['event'], 'E');
    expect(message['channel'], 'server');
  });
}
