/// A batch of events that could not be delivered to the data plane.
///
/// Every event future of the batch fails with it, so `RudderAnalytics.flush`
/// throws it for the events passed to it, and `RudderAnalytics.onError`
/// receives it once. The events it covers are dropped, not re-queued.
class RudderStackException implements Exception {
  /// Creates an exception for a batch of [eventCount] events.
  RudderStackException(
    this.message, {
    required this.eventCount,
    this.statusCode,
    this.responseBody,
    this.cause,
  });

  /// What went wrong, including the request URL.
  final String message;

  /// How many events the failed batch carried. On a client shared by
  /// several callers, that includes events other callers recorded.
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
