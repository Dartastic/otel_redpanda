// Licensed under the Dartastic Pro Software License Agreement.
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';

import 'otel_redpanda_suppression.dart';

class RedpandaResult {
  const RedpandaResult({
    this.clusterId,
    this.brokerAddress,
    this.topic,
    this.partition,
    this.offset,
    this.consumerGroup,
    this.statusCode,
    this.errorCode,
    this.errorMessage,
  });

  final String? clusterId;
  final String? brokerAddress;
  final String? topic;
  final int? partition;
  final int? offset;
  final String? consumerGroup;
  final int? statusCode;
  final String? errorCode;
  final String? errorMessage;
}

class RedpandaCallResult<T> {
  const RedpandaCallResult({required this.value, this.result});
  final T value;
  final RedpandaResult? result;
}

Future<T> tracedRedpanda<T>({
  required String operation,
  required Future<RedpandaCallResult<T>> Function() invoke,
  Tracer? tracer,
}) async {
  if (redpandaInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }
  final t = tracer ?? OTel.tracerProvider().getTracer('otel_redpanda');
  final span = t.startSpan('redpanda $operation', kind: SpanKind.producer);
  try {
    span.addAttributes(
        OTel.attributes([OTel.attributeString('messaging.system', 'kafka')]));
    final r = await invoke();
    final res = r.result;
    if (res != null) {
      if (res.clusterId != null) {
        span.addAttributes(OTel.attributes(
            [OTel.attributeString('redpanda.cluster.id', res.clusterId!)]));
      }
      if (res.brokerAddress != null) {
        span.addAttributes(OTel.attributes(
            [OTel.attributeString('server.address', res.brokerAddress!)]));
      }
      if (res.topic != null) {
        span.addAttributes(OTel.attributes(
            [OTel.attributeString('messaging.destination.name', res.topic!)]));
      }
      if (res.partition != null) {
        span.addAttributes(OTel.attributes([
          OTel.attributeInt(
            'messaging.kafka.destination.partition',
            res.partition!,
          )
        ]));
      }
      if (res.offset != null) {
        span.addAttributes(OTel.attributes([
          OTel.attributeInt('messaging.kafka.message.offset', res.offset!)
        ]));
      }
      if (res.consumerGroup != null) {
        span.addAttributes(OTel.attributes([
          OTel.attributeString(
            'messaging.kafka.consumer.group',
            res.consumerGroup!,
          )
        ]));
      }
      if (res.statusCode != null) {
        span.addAttributes(OTel.attributes(
            [OTel.attributeInt('http.response.status_code', res.statusCode!)]));
      }
      if (res.errorCode != null) {
        span.addAttributes(OTel.attributes(
            [OTel.attributeString('redpanda.error.code', res.errorCode!)]));
      }
      final fail = res.errorCode != null ||
          (res.statusCode != null && res.statusCode! >= 400);
      if (fail) {
        span.addAttributes(OTel.attributes([
          OTel.attributeString(
            ErrorResource.errorType.key,
            res.errorCode ?? 'redpanda.error',
          )
        ]));
        span.setStatus(SpanStatusCode.Error, res.errorMessage);
      }
    }
    return r.value;
  } catch (e, st) {
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}
