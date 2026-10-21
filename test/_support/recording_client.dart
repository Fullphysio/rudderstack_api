import 'dart:convert';
import 'dart:io';

import 'package:http/http.dart' as http;
import 'package:http/testing.dart';

/// A request the client sent, with its body gunzipped and decoded.
class SentRequest {
  SentRequest(this.request, this.body);

  final http.Request request;
  final Map<String, Object?> body;

  List<Map<String, Object?>> get batch =>
      (body['batch']! as List<Object?>).cast<Map<String, Object?>>();
}

/// A mock data plane that answers each request with the next status in
/// [statuses] (repeating the last one) and records what it received.
class RecordingClient extends MockClient {
  RecordingClient._(this.requests, MockClientHandler handler) : super(handler);

  factory RecordingClient({
    List<int> statuses = const [200],
    Map<String, String> headers = const {},
  }) {
    final requests = <SentRequest>[];
    return RecordingClient._(requests, (request) async {
      final bytes = request.headers['Content-Encoding'] == 'gzip'
          ? gzip.decode(request.bodyBytes)
          : request.bodyBytes;
      requests.add(SentRequest(
        request,
        jsonDecode(utf8.decode(bytes)) as Map<String, Object?>,
      ));
      final status = statuses[requests.length <= statuses.length
          ? requests.length - 1
          : statuses.length - 1];
      return http.Response(status == 200 ? 'OK' : 'error', status,
          headers: headers);
    });
  }

  final List<SentRequest> requests;

  List<Map<String, Object?>> get messages =>
      [for (final request in requests) ...request.batch];
}
