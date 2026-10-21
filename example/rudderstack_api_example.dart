import 'dart:io';

import 'package:rudderstack_api/rudderstack_api.dart';

Future<void> main() async {
  final analytics = RudderAnalytics(
    Platform.environment['RUDDERSTACK_WRITE_KEY']!,
    dataPlaneUrl:
        Uri.parse(Platform.environment['RUDDERSTACK_DATA_PLANE_URL']!),
  );

  analytics
    ..identify(
      userId: 'user-42',
      traits: {'email': 'jane@example.com', 'plan': 'pro'},
    )
    ..track(
      userId: 'user-42',
      event: 'Order Completed',
      properties: {'revenue': 49.9, 'currency': 'EUR'},
    );

  try {
    await analytics.flush();
  } on RudderStackException catch (error) {
    stderr.writeln('${error.eventCount} events were not delivered: $error');
  } finally {
    await analytics.shutdown();
  }
}
