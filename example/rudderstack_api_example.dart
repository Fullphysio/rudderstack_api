import 'dart:io';

import 'package:rudderstack_api/rudderstack_api.dart';

Future<void> main() async {
  final analytics = RudderAnalytics(
    Platform.environment['RUDDERSTACK_WRITE_KEY']!,
    dataPlaneUrl:
        Uri.parse(Platform.environment['RUDDERSTACK_DATA_PLANE_URL']!),
  );

  final delivered = [
    analytics.identify(
      userId: 'user-42',
      traits: {'email': 'jane@example.com', 'plan': 'pro'},
    ),
    analytics.track(
      userId: 'user-42',
      event: 'Order Completed',
      properties: {'revenue': 49.9, 'currency': 'EUR'},
    ),
  ];

  await analytics.flush();
  try {
    await Future.wait(delivered);
  } on RudderStackException catch (error) {
    stderr.writeln('${error.eventCount} events were not delivered: $error');
  } finally {
    await analytics.shutdown();
  }
}
