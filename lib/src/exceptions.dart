/// A batch of events that could not be delivered to the data plane.
///
/// Thrown by `RudderAnalytics.flush` and passed to `RudderAnalytics.onError`.
/// The events it covers are dropped, not re-queued.
class RudderStackException implements Exception {
  /// Creates an exception covering [eventCount] undelivered events.
  RudderStackException(
    this.message, {
    required this.eventCount,
    this.statusCode,
    this.responseBody,
    this.cause,
  });

  /// What went wrong, including the request URL.
  final String message;

  /// How many events were not delivered.
  final int eventCount;

  /// The data plane's HTTP status, or `null` when no response arrived.
  final int? statusCode;

  /// The data plane's response body, or `null` when no response arrived.
  final String? responseBody;

  /// The underlying error, such as a socket or timeout exception.
  final Object? cause;

  @override
  String toString() => 'RudderStackException: $message';
}
