## 0.1.0

- Initial release: `identify`, `track`, `page`, `screen`, `group` and `alias`,
  batched and gzip-compressed, with retries, `flush()` and `shutdown()`.
  Each call returns a future for its own event's delivery; `flush(events)`
  throws for the events passed to it.
  Payloads, validation and retry behaviour are checked against fixtures
  captured from `@rudderstack/rudder-sdk-node` 3.0.3.
