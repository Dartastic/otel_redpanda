# otel_redpanda

Dartastic OpenTelemetry bridge for Redpanda (Kafka-compatible streaming).

Wraps each call in a CLIENT-kind span with vendor-specific attributes following OpenTelemetry semantic conventions where applicable.

```dart
import 'package:otel_redpanda/otel_redpanda.dart';

await tracedRedpanda<void>(
  operation: 'someOp',
  invoke: () async {
    final r = await client.someOp();
    return RedpandaCallResult(value: r, result: RedpandaResult(statusCode: 200));
  },
);
```

Suppress instrumentation within a block:

```dart
await runWithoutRedpandaInstrumentationAsync(() async {
  await client.someInternalSync();
});
```
