# Changelog

## [0.1.0-beta.1-wip]

### Added

- Initial release.
- `tracedRedpanda<T>(...)` — wraps a otel_redpanda call in a CLIENT-kind span
  with vendor-specific attributes.
- Zone-scoped suppression via `runWithoutRedpandaInstrumentation()` / `Async()`.
