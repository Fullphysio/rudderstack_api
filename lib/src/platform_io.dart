import 'dart:io' as io;

/// Whether request bodies can be gzip-compressed on this platform.
const bool supportsGzip = true;

/// Whether this platform lets a client set the `User-Agent` header.
const bool supportsUserAgent = true;

/// Compresses [bytes] with gzip.
List<int> gzipEncode(List<int> bytes) => io.gzip.encode(bytes);
