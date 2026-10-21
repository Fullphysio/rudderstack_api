import 'dart:async';
import 'dart:convert';
import 'dart:math';

import 'package:http/http.dart' as http;

import 'exceptions.dart';
import 'platform_stub.dart' if (dart.library.io) 'platform_io.dart';
import 'version.dart';

/// Computes the pause before retry number `retryNumber` (starting at 1).
typedef RetryDelay = Duration Function(int retryNumber);

/// Receives every batch that could not be delivered.
typedef RudderStackErrorHandler = void Function(RudderStackException error);

const Duration _maxRetryAfter = Duration(seconds: 30);

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
/// Create one per process and reuse it. Calls such as [track] queue the event
/// and return at once; delivery happens in the background once [flushAt]
/// events are queued, [maxBatchBytes] is reached, or [flushInterval] elapses.
///
/// Each call returns a future that completes when that event is delivered, or
/// fails with a [RudderStackException] when its batch is rejected — the Dart
/// form of the Node SDK's per-message callback. Not awaiting it is safe: the
/// failure then only reaches [onError].
///
/// The future settles only once its batch has been sent, by [flush] or in the
/// background. Awaiting it before a flush can wait for [flushInterval], or
/// forever when the interval is [Duration.zero]. Pass it to [flush] instead.
///
/// On a server that may be frozen or recycled between requests — Cloud Run,
/// Cloud Functions — `await` [flush] with the request's own events before
/// returning. It throws when one of them was not delivered:
///
/// ```dart
/// final analytics = RudderAnalytics(
///   'WRITE_KEY',
///   dataPlaneUrl: Uri.parse('https://example.dataplane.rudderstack.com'),
/// );
/// final delivered = analytics.track(userId: 'user-1', event: 'Order Completed');
/// await analytics.flush([delivered]);
/// ```
class RudderAnalytics {
  /// Creates a client for the source identified by [writeKey], sending to
  /// [dataPlaneUrl].
  ///
  /// Throws an [ArgumentError] when [writeKey] is empty, [dataPlaneUrl] is not
  /// an absolute `http` or `https` URL, or a numeric option is out of range.
  RudderAnalytics(
    String writeKey, {
    required Uri dataPlaneUrl,
    this.flushAt = 20,
    this.flushInterval = const Duration(seconds: 10),
    this.maxBatchBytes = 450 * 1024,
    this.maxPendingEvents = 20000,
    this.retryCount = 3,
    this.retryDelay = defaultRetryDelay,
    this.timeout = const Duration(seconds: 10),
    this.gzip = true,
    this.enabled = true,
    this.onError,
    http.Client? httpClient,
  })  : _writeKey = writeKey,
        _url = _batchUrl(dataPlaneUrl),
        _httpClient = httpClient ?? http.Client(),
        _ownsClient = httpClient == null {
    if (writeKey.isEmpty) {
      throw ArgumentError.value(writeKey, 'writeKey', 'must not be empty');
    }
    _checkPositive(flushAt, 'flushAt');
    _checkPositive(maxBatchBytes, 'maxBatchBytes');
    _checkPositive(maxPendingEvents, 'maxPendingEvents');
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
  ///
  /// The Node SDK calls this `maxInternalQueueSize`; its `maxQueueSize` is a
  /// size in bytes, which is [maxBatchBytes] here.
  final int maxPendingEvents;

  /// How many times a failed request is retried. Connection errors, `429`
  /// and `5xx` responses are retried; timeouts and other statuses are not.
  final int retryCount;

  /// The pause before each retry. A longer `Retry-After` header, in seconds
  /// and capped at 30 s, wins.
  final RetryDelay retryDelay;

  /// Deadline of a single HTTP attempt.
  final Duration timeout;

  /// Whether request bodies are gzip-compressed. Ignored on the web, where
  /// `dart:io` is unavailable.
  final bool gzip;

  /// When `false`, calls are validated but nothing is queued or sent.
  final bool enabled;

  /// Called once for every batch that could not be delivered, whether or not
  /// the futures of its events are awaited.
  final RudderStackErrorHandler? onError;

  final String _writeKey;
  final Uri _url;
  final http.Client _httpClient;
  final bool _ownsClient;
  final List<_QueuedMessage> _queue = [];
  int _queueBytes = 0;
  int _backlog = 0;
  Timer? _timer;
  Future<void> _inFlight = Future.value();
  bool _closed = false;

  /// Records who a user is and the [traits] that describe them.
  ///
  /// The traits are sent both at the top level and as `context.traits`.
  ///
  /// Returns a future that settles once the event's batch has been sent: pass
  /// it to [flush] rather than awaiting it first. See [RudderAnalytics].
  Future<void> identify({
    String? userId,
    String? anonymousId,
    Map<String, Object?>? traits,
    Map<String, Object?>? context,
    Map<String, Object?>? integrations,
    DateTime? timestamp,
    String? messageId,
  }) {
    _requireIdentity(userId, anonymousId);
    return _enqueue(
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
  ///
  /// Returns a future that settles once the event's batch has been sent: pass
  /// it to [flush] rather than awaiting it first. See [RudderAnalytics].
  Future<void> track({
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
    return _enqueue(
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
  ///
  /// Returns a future that settles once the event's batch has been sent: pass
  /// it to [flush] rather than awaiting it first. See [RudderAnalytics].
  Future<void> page({
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
    return _enqueue(
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
  ///
  /// Returns a future that settles once the event's batch has been sent: pass
  /// it to [flush] rather than awaiting it first. See [RudderAnalytics].
  Future<void> screen({
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
    return _enqueue(
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
  ///
  /// Returns a future that settles once the event's batch has been sent: pass
  /// it to [flush] rather than awaiting it first. See [RudderAnalytics].
  Future<void> group({
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
    return _enqueue(
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
  ///
  /// Returns a future that settles once the event's batch has been sent: pass
  /// it to [flush] rather than awaiting it first. See [RudderAnalytics].
  Future<void> alias({
    required String userId,
    required String previousId,
    Map<String, Object?>? context,
    Map<String, Object?>? integrations,
    DateTime? timestamp,
    String? messageId,
  }) {
    _require(userId, 'You must pass a "userId".');
    _require(previousId, 'You must pass a "previousId".');
    return _enqueue(
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

  /// Sends every queued event, waits until all batches, including those
  /// already in flight, have been delivered or have failed, then waits for
  /// [events].
  ///
  /// [events] are futures returned by the event calls. Throws the
  /// [RudderStackException] of the first one that was not delivered, so a
  /// caller sees its own failures and never those of other callers sharing
  /// this client. With no [events], it never throws for a failed delivery:
  /// [onError] still sees every one. Failed events are not re-queued.
  ///
  /// Throws a [StateError] after [shutdown].
  Future<void> flush([Iterable<Future<void>> events = const []]) async {
    _ensureOpen();
    await _flush();
    await Future.wait(events);
  }

  /// Flushes, then releases the HTTP client if this instance created it.
  /// Like [flush], it does not throw for failed deliveries.
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

  Future<void> _flush() {
    _drain();
    return _inFlight;
  }

  Future<void> _enqueue(
    String type,
    Map<String, Object?> fields, {
    required Map<String, Object?>? context,
    required String? messageId,
  }) {
    _ensureOpen();
    if (!enabled) return Future.value();
    if (_backlog >= maxPendingEvents) {
      final dropped = RudderStackException(
        'Dropped a "$type" event: $maxPendingEvents events are already waiting '
        'or in flight.',
        eventCount: 1,
      );
      _reportToHandler(dropped);
      return Future<void>.error(dropped)..ignore();
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
    return queued.delivered.future..ignore();
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
      for (final message in batch) {
        message.delivered.complete();
      }
    } on RudderStackException catch (error) {
      _fail(batch, error);
    } on Object catch (error) {
      _fail(
        batch,
        RudderStackException(
          'Could not send ${batch.length} events: $error',
          cause: error,
          eventCount: batch.length,
        ),
      );
    } finally {
      _backlog -= batch.length;
    }
  }

  Future<void> _post(List<int> body, int eventCount) async {
    final headers = {
      'Authorization': 'Basic ${base64Encode(utf8.encode('$_writeKey:'))}',
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
          'POST $_url timed out after ${timeout.inMilliseconds} ms',
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
          'POST $_url failed$attempts: $transportError',
          cause: transportError,
          eventCount: eventCount,
        );
      }
      throw RudderStackException(
        'POST $_url returned ${response.statusCode}'
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
        _url,
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
    final retryAfter = parseRetryAfter(header);
    return retryAfter != null && retryAfter > computed ? retryAfter : computed;
  }

  void _fail(List<_QueuedMessage> batch, RudderStackException error) {
    _reportToHandler(error);
    for (final message in batch) {
      message.delivered.completeError(error);
    }
  }

  void _reportToHandler(RudderStackException error) {
    final handler = onError;
    if (handler == null) return;
    try {
      handler(error);
    } on Object catch (handlerError, stackTrace) {
      Zone.current.handleUncaughtError(handlerError, stackTrace);
    }
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
  if (value is double && !value.isFinite) return null;
  // ignore: avoid_dynamic_calls
  return (value as dynamic).toJson();
}

/// Reads a `Retry-After` header given in seconds, capped at 30 s.
///
/// Returns `null` for anything else, including an HTTP date or a non-finite
/// number, so the computed backoff applies instead.
Duration? parseRetryAfter(String header) {
  final seconds = double.tryParse(header.trim());
  if (seconds == null || !seconds.isFinite) return null;
  if (seconds <= 0) return Duration.zero;
  if (seconds >= _maxRetryAfter.inSeconds) return _maxRetryAfter;
  return Duration(milliseconds: (seconds * 1000).round());
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
  final Completer<void> delivered = Completer<void>();

  String withSentAt(String sentAtJson) =>
      '${json.substring(0, json.length - 1)},"sentAt":$sentAtJson}';
}

class _AttemptTimedOut implements Exception {
  _AttemptTimedOut(this.cause);

  final Object cause;
}
