// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:otel_redpanda/otel_redpanda.dart';
import 'package:test/test.dart';

void main() {
  group('otel_redpanda', () {
    test('suppression toggles in zone', () async {
      expect(redpandaInstrumentationSuppressed(), isFalse);
      await runWithoutRedpandaInstrumentationAsync(() async {
        expect(redpandaInstrumentationSuppressed(), isTrue);
      });
      expect(redpandaInstrumentationSuppressed(), isFalse);
    });

    test('RedpandaResult fields default null', () {
      const r = RedpandaResult();
      expect(r.statusCode, isNull);
    });
  });
}
