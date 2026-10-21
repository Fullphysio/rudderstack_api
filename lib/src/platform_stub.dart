/// Whether request bodies can be gzip-compressed on this platform.
const bool supportsGzip = false;

/// Whether this platform lets a client set the `User-Agent` header.
const bool supportsUserAgent = false;

/// Compresses [bytes] with gzip. Unavailable without `dart:io`.
List<int> gzipEncode(List<int> bytes) =>
    throw UnsupportedError('gzip requires dart:io');
