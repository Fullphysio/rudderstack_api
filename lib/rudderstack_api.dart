/// A pure Dart RudderStack server-side SDK.
///
/// Sends `identify`, `track`, `page`, `screen`, `group` and `alias` events to
/// a RudderStack data plane in gzip-compressed batches, with retries. Runs on
/// Dart servers, CLIs and Flutter apps. No Flutter dependency.
library;

export 'src/analytics.dart'
    show
        RetryDelay,
        RudderAnalytics,
        RudderStackErrorHandler,
        defaultRetryDelay;
export 'src/exceptions.dart';
