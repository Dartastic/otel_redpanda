// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:otel_redpanda/otel_redpanda.dart';

/// Traces Redpanda calls with OpenTelemetry.
///
/// `otel_redpanda` depends on the OpenTelemetry API only, so the application
/// initializes an SDK once at startup. Wrap each Redpanda call in
/// [tracedRedpanda]; the `invoke` callback makes the real call and reports what
/// came back, which is recorded on the span.
Future<void> main() async {
  await OTel.initialize(serviceName: 'redpanda-example');

  // Traces a Redpanda call.
  final redpandaId = await tracedRedpanda<String>(
    operation: 'create',
    invoke: () async {
      // Make the Redpanda call here, then report what it returned.
      const id = 'redpanda_8f3a2c';
      return const RedpandaCallResult(
          value: id,
          result: RedpandaResult(
              clusterId: 'cluster_8f3a2c',
              brokerAddress: 'example',
              topic: 'orders',
              partition: 1,
              offset: 1,
              consumerGroup: 'example',
              statusCode: 201,
              errorCode: 'example',
              errorMessage: 'example'));
    },
  );
  OTelLog.info('tracedRedpanda create returned $redpandaId');

  // Calls that should not be traced, such as health checks, run inside
  // runWithoutRedpandaInstrumentationAsync.
  await runWithoutRedpandaInstrumentationAsync(
    () => tracedRedpanda<String>(
      operation: 'create',
      invoke: () async {
        const id = 'redpanda_8f3a2c';
        return const RedpandaCallResult(
            value: id,
            result: RedpandaResult(
                clusterId: 'cluster_8f3a2c',
                brokerAddress: 'example',
                topic: 'orders',
                partition: 1,
                offset: 1,
                consumerGroup: 'example',
                statusCode: 201,
                errorCode: 'example',
                errorMessage: 'example'));
      },
    ),
  );

  await OTel.shutdown();
}
