@Tags(['integration'])
library;

import 'dart:io';

import 'package:rudderstack_api/rudderstack_api.dart';
import 'package:test/test.dart';

/// Sends real events to a RudderStack data plane.
///
/// Reads its target from `RUDDERSTACK_TEST_WRITE_KEY` and
/// `RUDDERSTACK_TEST_DATA_PLANE_URL` and skips itself when they are unset, so
/// a checkout without configuration still passes. Use a source dedicated to
/// testing: the events are real.
void main() {
  final writeKey = Platform.environment['RUDDERSTACK_TEST_WRITE_KEY'] ?? '';
  final dataPlaneUrl =
      Platform.environment['RUDDERSTACK_TEST_DATA_PLANE_URL'] ?? '';

  if (writeKey.isEmpty || dataPlaneUrl.isEmpty) {
    test('live API', () {},
        skip: 'RUDDERSTACK_TEST_WRITE_KEY/DATA_PLANE_URL not set');
    return;
  }

  RudderAnalytics client(String key) => RudderAnalytics(
        key,
        dataPlaneUrl: Uri.parse(dataPlaneUrl),
        flushInterval: Duration.zero,
      );

  test('delivers one event of every type in a gzip batch', () async {
    const anonymousId = 'rudderstack_api-integration-test';
    final analytics = client(writeKey)
      ..identify(anonymousId: anonymousId, traits: {'test': true})
      ..track(
        anonymousId: anonymousId,
        event: 'rudderstack_api integration test',
        properties: {'at': DateTime.now()},
      )
      ..page(anonymousId: anonymousId, name: 'Integration')
      ..screen(anonymousId: anonymousId, name: 'Integration')
      ..group(anonymousId: anonymousId, groupId: 'rudderstack_api-test');
    await analytics.shutdown();
  });

  test('rejects an invalid write key with 401', () async {
    final analytics = client('invalid-write-key')
      ..track(anonymousId: 'a', event: 'E');
    await expectLater(
      analytics.flush(),
      throwsA(isA<RudderStackException>()
          .having((e) => e.statusCode, 'statusCode', 401)),
    );
    await analytics.shutdown();
  });
}
