# Contributing

## Reporting a bug

Open an issue with the package version, the call you made, and the
`RudderStackException` you got (redact write keys and personal data).

Behaviour follows the official `@rudderstack/rudder-sdk-node`. If this package
differs from it, that is a bug unless the README lists it as a deliberate
divergence.

## Running the tests

```
dart pub get
dart analyze --fatal-infos
dart test                      # unit, Node conformance and local mock server, no network
dart test -p chrome            # compiles the web build and runs test/web_test.dart (needs Chrome)
dart test --tags integration --run-skipped   # live data plane, needs `RUDDERSTACK_TEST_WRITE_KEY` and `RUDDERSTACK_TEST_DATA_PLANE_URL`
```

## Pull requests

- Keep `dart format` (default width) and `dart analyze --fatal-infos` clean.
- Every exported symbol needs `///` dartdoc.
- Add a `CHANGELOG.md` entry under an `## Unreleased` heading.
