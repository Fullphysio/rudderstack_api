import 'dart:io';

import 'package:rudderstack_api/rudderstack_api.dart';
import 'package:test/test.dart';

void main() {
  test('packageVersion matches pubspec.yaml', () {
    final pubspec = File('pubspec.yaml').readAsStringSync();
    final version = RegExp(r'^version: (\S+)$', multiLine: true)
        .firstMatch(pubspec)!
        .group(1);
    expect(packageVersion, version);
  });
}
