import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:http/http.dart' as http;
import 'package:http_parser/http_parser.dart';

import 'exceptions.dart';
import 'platform_stub.dart' if (dart.library.io) 'platform_io.dart';
import 'version.dart';

/// Computes the pause before retry number `retryNumber` (starting at 1).
typedef RetryDelay = Duration Function(int retryNumber);

/// Receives every batch that could not be delivered.
typedef RudderStackErrorHandler = void Function(RudderStackException error);

/// The default [RetryDelay]: 200 ms, 400 ms, 800 ms… plus up to 20 % jitter.
///
/// This is the exponential backoff `@rudderstack/rudder-sdk-node` gets from
/// `axios-retry`.
Duration defaultRetryDelay(int retryNumber) {
  final base = pow(2, retryNumber) * 100;
  final jitter = base * 0.2 * _random.nextDouble();
  return Duration(microseconds: ((base + jitter) * 1000).round());
}

final Random _random = Random();
final Random _secureRandom = Random.secure();

const String _libraryName = 'rudderstack_api';

/// A RudderStack server-side client: queues events and delivers them in
/// batches to the data plane's `/v1/batch` endpoint.
///
/// Create one per process and reuse it. Calls such as [track] return
/// immediately; delivery happens in the background once [flushAt] events are
/// queued, [maxBatchBytes] is reached, or [flushInterval] elapses.
///
/// On a server that may be frozen or recycled between requests — Cloud Run,
/// Cloud Functions — `await` [flush] before the request returns. It sends
/// everything still queued, waits for batches already in flight, and throws a
/// [RudderStackException] if any batch failed since the previous flush.
///
/// ```dart
/// final analytics = RudderAnalytics(
///   'WRITE_KEY',
///   dataPlaneUrl: Uri.parse('https://example.dataplane.rudderstack.com'),
/// );
/// analytics.track(userId: 'user-1', event: 'Order Completed');
/// await analytics.flush();
/// ```
class RudderAnalytics {
  /// Creates a client for the source identified by [writeKey], sending to
  /// [dataPlaneUrl].
  ///
  /// Throws an [ArgumentError] when [writeKey] is empty, [dataPlaneUrl] is not
  /// an absolute `http` or `https` URL, or a numeric option is out of range.
  RudderAnalytics(
    this.writeKey, {
    required Uri dataPlaneUrl,
    this.flushAt = 20,
    this.flushInterval = const Duration(seconds: 10),
    this.maxBatchBytes = 450 * 1024,
    this.maxQueueSize = 20000,
    this.retryCount = 3,
    this.retryDelay = defaultRetryDelay,
    this.timeout = const Duration(seconds: 10),
    this.gzip = true,
    this.enabled = true,
    this.onError,
    http.Client? httpClient,
  })  : batchUrl = _batchUrl(dataPlaneUrl),
        _httpClient = httpClient ?? http.Client(),
        _ownsClient = httpClient == null {
    if (writeKey.isEmpty) {
      throw ArgumentError.value(writeKey, 'writeKey', 'must not be empty');
    }
    _checkPositive(flushAt, 'flushAt');
    _checkPositive(maxBatchBytes, 'maxBatchBytes');
    _checkPositive(maxQueueSize, 'maxQueueSize');
    if (retryCount < 0) {
      throw ArgumentError.value(
          retryCount, 'retryCount', 'must not be negative');
    }
    if (flushInterval.isNegative) {
      throw ArgumentError.value(
          flushInterval, 'flushInterval', 'must not be negative');
    }
    if (timeout <= Duration.zero) {
      throw ArgumentError.value(timeout, 'timeout', 'must be positive');
    }
  }

  /// The source's write key, sent as the Basic auth user name.
  final String writeKey;

  /// The endpoint batches are posted to: `<dataPlaneUrl>/v1/batch`.
  final Uri batchUrl;

  /// How many queued events trigger a background flush. Also the largest
  /// number of events sent in one request.
  final int flushAt;

  /// How long an event may wait in the queue before a background flush.
  ///
  /// [Duration.zero] disables the timer, so events leave only when [flushAt]
  /// or [maxBatchBytes] is reached or on an explicit [flush].
  final Duration flushInterval;

  /// Upper bound, in bytes of JSON, of one batch request. Reaching it
  /// triggers a background flush. The data plane rejects bodies over 500 KB.
  final int maxBatchBytes;

  /// How many events may be waiting or in flight at once. Further events are
  /// dropped and reported as failures until the backlog drains.
  final int maxQueueSize;

  /// How many times a failed request is retried. Connection errors, `429`
  /// and `5xx` responses are retried; timeouts and other statuses are not.
  final int retryCount;

  /// The pause before each retry. A longer `Retry-After` header wins.
  final RetryDelay retryDelay;

  /// Deadline of a single HTTP attempt.
  final Duration timeout;

  /// Whether request bodies are gzip-compressed. Ignored on the web, where
  /// `dart:io` is unavailable.
  final bool gzip;

  /// When `false`, calls are validated but nothing is queued or sent.
  final bool enabled;

  /// Called with every batch that could not be delivered, in addition to the
  /// failure being reported by the next [flush].
  final RudderStackErrorHandler? onError;

  final http.Client _httpClient;
  final bool _ownsClient;
  final List<_QueuedMessage> _queue = [];
  int _queueBytes = 0;
  int _backlog = 0;
  Timer? _timer;
  Future<void> _inFlight = Future.value();
  bool _closed = false;
  RudderStackException? _firstFailure;
  int _failedBatches = 0;
  int _failedEvents = 0;

  /// Records who a user is and the [traits] that describe them.
  ///
  /// The traits are sent both at the top level and as `context.traits`.
  void identify({
    String? userId,
    String? anonymousId,
    Map<String, Object?>? traits,
    Map<String, Object?>? context,
    Map<String, Object?>? integrations,
    DateTime? timestamp,
    String? messageId,
  }) {
    _requireIdentity(userId, anonymousId);
    _enqueue(
        'identify',
        {
          'userId': userId,
          'anonymousId': anonymousId,
          'traits': traits,
          'integrations': integrations,
          'timestamp': timestamp,
        },
        context: {
          ...?context,
          if (traits != null) 'traits': traits,
        },
        messageId: messageId);
  }

  /// Records that a user performed [event].
  void track({
    required String event,
    String? userId,
    String? anonymousId,
    Map<String, Object?>? properties,
    Map<String, Object?>? context,
    Map<String, Object?>? integrations,
    DateTime? timestamp,
    String? messageId,
  }) {
    _requireIdentity(userId, anonymousId);
    _require(event, 'You must pass an "event".');
    _enqueue(
        'track',
        {
          'userId': userId,
          'anonymousId': anonymousId,
          'event': event,
          'properties': properties,
          'integrations': integrations,
          'timestamp': timestamp,
        },
        context: context,
        messageId: messageId);
  }

  /// Records that a user viewed a web page.
  void page({
    String? name,
    String? category,
    String? userId,
    String? anonymousId,
    Map<String, Object?>? properties,
    Map<String, Object?>? context,
    Map<String, Object?>? integrations,
    DateTime? timestamp,
    String? messageId,
  }) {
    _requireIdentity(userId, anonymousId);
    _enqueue(
        'page',
        {
          'userId': userId,
          'anonymousId': anonymousId,
          'name': name,
          'category': category,
          'properties': properties,
          'integrations': integrations,
          'timestamp': timestamp,
        },
        context: context,
        messageId: messageId);
  }

  /// Records that a user viewed a mobile app screen.
  void screen({
    String? name,
    String? userId,
    String? anonymousId,
    Map<String, Object?>? properties,
    Map<String, Object?>? context,
    Map<String, Object?>? integrations,
    DateTime? timestamp,
    String? messageId,
  }) {
    _requireIdentity(userId, anonymousId);
    _enqueue(
        'screen',
        {
          'userId': userId,
          'anonymousId': anonymousId,
          'name': name,
          'properties': properties,
          'integrations': integrations,
          'timestamp': timestamp,
        },
        context: context,
        messageId: messageId);
  }

  /// Associates a user with the group [groupId], such as a company or team.
  void group({
    required String groupId,
    String? userId,
    String? anonymousId,
    Map<String, Object?>? traits,
    Map<String, Object?>? context,
    Map<String, Object?>? integrations,
    DateTime? timestamp,
    String? messageId,
  }) {
    _requireIdentity(userId, anonymousId);
    _require(groupId, 'You must pass a "groupId".');
    _enqueue(
        'group',
        {
          'userId': userId,
          'anonymousId': anonymousId,
          'groupId': groupId,
          'traits': traits,
          'integrations': integrations,
          'timestamp': timestamp,
        },
        context: context,
        messageId: messageId);
  }

  /// Merges the identity [previousId] into [userId].
  void alias({
    required String userId,
    required String previousId,
    Map<String, Object?>? context,
    Map<String, Object?>? integrations,
    DateTime? timestamp,
    String? messageId,
  }) {
    _require(userId, 'You must pass a "userId".');
    _require(previousId, 'You must pass a "previousId".');
    _enqueue(
        'alias',
        {
          'userId': userId,
          'previousId': previousId,
          'integrations': integrations,
          'timestamp': timestamp,
        },
        context: context,
        messageId: messageId);
  }

  /// Sends every queued event and waits until all batches, including those
  /// already in flight, have been delivered or have failed.
  ///
  /// Throws a [RudderStackException] when any batch failed since the
  /// previous call. Failed events are not re-queued. Throws a [StateError]
  /// after [shutdown].
  Future<void> flush() {
    _ensureOpen();
    return _flush();
  }

  /// Flushes, then releases the HTTP client if this instance created it.
  ///
  /// Events recorded afterwards throw a [StateError]. Calling it again is a
  /// no-op.
  Future<void> shutdown() async {
    if (_closed) return;
    _closed = true;
    try {
      await _flush();
    } finally {
      if (_ownsClient) _httpClient.close();
    }
  }

  Future<void> _flush() async {
    _drain();
    await _inFlight;
    _throwFailures();
  }

  void _enqueue(
    String type,
    Map<String, Object?> fields, {
    required Map<String, Object?>? context,
    required String? messageId,
  }) {
    _ensureOpen();
    if (!enabled) return;
    if (_backlog >= maxQueueSize) {
      _recordFailure(RudderStackException(
        'Dropped a "$type" event: $maxQueueSize events are already waiting '
        'or in flight.',
        eventCount: 1,
      ));
      return;
    }
    final message = <String, Object?>{
      for (final entry in fields.entries)
        if (entry.value != null) entry.key: entry.value,
      'type': type,
      'context': {
        ...?context,
        'library': {'name': _libraryName, 'version': packageVersion},
      },
      'channel': 'server',
      'originalTimestamp': DateTime.now(),
      'messageId': messageId ?? _uuidV4(),
    };
    final json = jsonEncode(message, toEncodable: _toEncodable);
    final queued = _QueuedMessage(json, utf8.encode(json).length);
    _queue.add(queued);
    _queueBytes += queued.bytes;
    _backlog++;
    if (_queue.length >= flushAt || _queueBytes >= maxBatchBytes) {
      _drain();
    } else if (flushInterval > Duration.zero) {
      _timer ??= Timer(flushInterval, _drain);
    }
  }

  void _drain() {
    _timer?.cancel();
    _timer = null;
    while (_queue.isNotEmpty) {
      final batch = <_QueuedMessage>[];
      var bytes = 0;
      while (_queue.isNotEmpty &&
          batch.length < flushAt &&
          (batch.isEmpty || bytes + _queue.first.bytes <= maxBatchBytes)) {
        final next = _queue.removeAt(0);
        bytes += next.bytes;
        batch.add(next);
      }
      _queueBytes -= bytes;
      final previous = _inFlight;
      _inFlight = previous.then((_) => _send(batch));
    }
  }

  Future<void> _send(List<_QueuedMessage> batch) async {
    try {
      final sentAt = jsonEncode(formatTimestamp(DateTime.now()));
      final messages = batch.map((message) => message.withSentAt(sentAt));
      final body = utf8.encode(
        '{"batch":[${messages.join(',')}],'
        '"timestamp":$sentAt,"sentAt":$sentAt}',
      );
      await _post(gzip && supportsGzip ? gzipEncode(body) : body, batch.length);
    } on RudderStackException catch (error) {
      _recordFailure(error);
    } on Object catch (error) {
      _recordFailure(RudderStackException(
        'Could not send ${batch.length} events: $error',
        cause: error,
        eventCount: batch.length,
      ));
    } finally {
      _backlog -= batch.length;
    }
  }

  Future<void> _post(List<int> body, int eventCount) async {
    final headers = {
      'Authorization': 'Basic ${base64Encode(utf8.encode('$writeKey:'))}',
      'Content-Type': 'application/json',
      if (gzip && supportsGzip) 'Content-Encoding': 'gzip',
      if (supportsUserAgent) 'User-Agent': '$_libraryName/$packageVersion',
    };
    var retries = 0;
    while (true) {
      http.Response? response;
      Object? transportError;
      try {
        response = await _attempt(body, headers);
      } on _AttemptTimedOut catch (timedOut) {
        throw RudderStackException(
          'POST $batchUrl timed out after ${timeout.inMilliseconds} ms',
          cause: timedOut.cause,
          eventCount: eventCount,
        );
      } on Object catch (error) {
        transportError = error;
      }

      final status = response?.statusCode;
      if (status != null && status >= 200 && status < 300) return;

      final retriable = status == null || status == 429 || status >= 500;
      if (retriable && retries < retryCount) {
        retries++;
        await Future<void>.delayed(_delayBefore(retries, response));
        continue;
      }

      final attempts = retries == 0 ? '' : ' after ${retries + 1} attempts';
      if (response == null) {
        throw RudderStackException(
          'POST $batchUrl failed$attempts: $transportError',
          cause: transportError,
          eventCount: eventCount,
        );
      }
      throw RudderStackException(
        'POST $batchUrl returned ${response.statusCode}'
        '${response.reasonPhrase == null ? '' : ' ${response.reasonPhrase}'}'
        '$attempts',
        statusCode: response.statusCode,
        responseBody: response.body,
        eventCount: eventCount,
      );
    }
  }

  Future<http.Response> _attempt(
    List<int> body,
    Map<String, String> headers,
  ) async {
    final abort = Completer<void>();
    final timer = Timer(timeout, abort.complete);
    try {
      final request = http.AbortableRequest(
        'POST',
        batchUrl,
        abortTrigger: abort.future,
      )
        ..headers.addAll(headers)
        ..bodyBytes = body;
      final streamed = await _httpClient.send(request).timeout(timeout);
      return await http.Response.fromStream(streamed).timeout(timeout);
    } on TimeoutException catch (error) {
      throw _AttemptTimedOut(error);
    } on http.RequestAbortedException catch (error) {
      throw _AttemptTimedOut(error);
    } finally {
      timer.cancel();
    }
  }

  Duration _delayBefore(int retryNumber, http.Response? response) {
    final computed = retryDelay(retryNumber);
    final header = response?.headers['retry-after'];
    if (header == null) return computed;
    final retryAfter = _parseRetryAfter(header);
    return retryAfter != null && retryAfter > computed ? retryAfter : computed;
  }

  void _recordFailure(RudderStackException error) {
    _firstFailure ??= error;
    _failedBatches++;
    _failedEvents += error.eventCount;
    final handler = onError;
    if (handler == null) return;
    try {
      handler(error);
    } on Object catch (handlerError, stackTrace) {
      Zone.current.handleUncaughtError(handlerError, stackTrace);
    }
  }

  void _throwFailures() {
    final first = _firstFailure;
    if (first == null) return;
    final batches = _failedBatches;
    final events = _failedEvents;
    _firstFailure = null;
    _failedBatches = 0;
    _failedEvents = 0;
    if (batches == 1) throw first;
    throw RudderStackException(
      '$batches deliveries ($events events) failed since the last flush. '
      'First failure: ${first.message}',
      statusCode: first.statusCode,
      responseBody: first.responseBody,
      cause: first,
      eventCount: events,
    );
  }

  void _ensureOpen() {
    if (_closed) {
      throw StateError('This RudderAnalytics has been shut down.');
    }
  }

  static void _requireIdentity(String? userId, String? anonymousId) {
    if ((userId == null || userId.isEmpty) &&
        (anonymousId == null || anonymousId.isEmpty)) {
      throw ArgumentError(
          'You must pass either an "anonymousId" or a "userId".');
    }
  }

  static void _require(String value, String message) {
    if (value.isEmpty) throw ArgumentError(message);
  }

  static void _checkPositive(int value, String name) {
    if (value < 1) throw ArgumentError.value(value, name, 'must be positive');
  }

  static Uri _batchUrl(Uri dataPlaneUrl) {
    if ((!dataPlaneUrl.isScheme('http') && !dataPlaneUrl.isScheme('https')) ||
        dataPlaneUrl.host.isEmpty) {
      throw ArgumentError.value(dataPlaneUrl, 'dataPlaneUrl',
          'must be an absolute http or https URL');
    }
    final base = dataPlaneUrl.toString().replaceFirst(RegExp(r'/+$'), '');
    return Uri.parse('$base/v1/batch');
  }
}

/// Formats [time] the way JavaScript's `Date.toJSON` does: UTC, millisecond
/// precision, `Z` suffix — e.g. `2026-01-31T09:05:03.042Z`.
String formatTimestamp(DateTime time) {
  final utc = time.toUtc();
  String two(int value) => value.toString().padLeft(2, '0');
  return '${utc.year.toString().padLeft(4, '0')}-${two(utc.month)}-'
      '${two(utc.day)}T${two(utc.hour)}:${two(utc.minute)}:'
      '${two(utc.second)}.${utc.millisecond.toString().padLeft(3, '0')}Z';
}

Object? _toEncodable(Object? value) {
  if (value is DateTime) return formatTimestamp(value);
  // ignore: avoid_dynamic_calls
  return (value as dynamic).toJson();
}

Duration? _parseRetryAfter(String header) {
  final seconds = num.tryParse(header.trim());
  if (seconds != null) {
    return seconds > 0
        ? Duration(milliseconds: (seconds * 1000).round())
        : Duration.zero;
  }
  try {
    final wait = parseHttpDate(header).difference(DateTime.now());
    return wait.isNegative ? Duration.zero : wait;
  } on FormatException {
    return null;
  }
}

String _uuidV4() {
  final bytes = List<int>.generate(16, (_) => _secureRandom.nextInt(256));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  final hex = bytes.map((byte) => byte.toRadixString(16).padLeft(2, '0'));
  final digits = hex.join();
  return '${digits.substring(0, 8)}-${digits.substring(8, 12)}-'
      '${digits.substring(12, 16)}-${digits.substring(16, 20)}-'
      '${digits.substring(20)}';
}

class _QueuedMessage {
  _QueuedMessage(this.json, this.bytes);

  final String json;
  final int bytes;

  String withSentAt(String sentAtJson) =>
      '${json.substring(0, json.length - 1)},"sentAt":$sentAtJson}';
}

class _AttemptTimedOut implements Exception {
  _AttemptTimedOut(this.cause);

  final Object cause;
}
