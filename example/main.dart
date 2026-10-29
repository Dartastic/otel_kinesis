// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:otel_kinesis/otel_kinesis.dart';

/// Traces Kinesis calls with OpenTelemetry.
///
/// `otel_kinesis` depends on the OpenTelemetry API only, so the application
/// initializes an SDK once at startup. Wrap each Kinesis call in
/// [tracedKinesisPutRecord]; the `invoke` callback makes the real call and reports what
/// came back, which is recorded on the span.
Future<void> main() async {
  await OTel.initialize(serviceName: 'kinesis-example');

  // Wraps a Kinesis PutRecord / PutRecords in a PRODUCER span using OTel messaging semconv (messaging.system=aws_kinesis).
  final kinesisPutRecordId = await tracedKinesisPutRecord<String>(
    streamName: 'example',
    partitionKey: 'orders/2026/inv-1042.pdf',
    payloadSize: 512,
    region: 'us-east-1',
    invoke: () async {
      // Make the Kinesis call here, then report what it returned.
      const id = 'kinesis_8f3a2c';
      return const KinesisPutResult(
          value: id, sequenceNumber: 'example', shardId: 'shard_8f3a2c');
    },
  );
  OTelLog.info('tracedKinesisPutRecord returned $kinesisPutRecordId');

  // Wraps a Kinesis GetRecords consumer iteration in a CONSUMER span.
  final kinesisGetRecordsId = await tracedKinesisGetRecords<String>(
    streamName: 'example',
    shardId: 'shard_8f3a2c',
    batchSize: 512,
    region: 'us-east-1',
    invoke: () async {
      // Make the Kinesis call here, then report what it returned.
      return 'kinesis_8f3a2c';
    },
  );
  OTelLog.info('tracedKinesisGetRecords returned $kinesisGetRecordsId');

  // Calls that should not be traced, such as health checks, run inside
  // runWithoutKinesisInstrumentationAsync.
  await runWithoutKinesisInstrumentationAsync(
    () => tracedKinesisPutRecord<String>(
      streamName: 'example',
      partitionKey: 'orders/2026/inv-1042.pdf',
      payloadSize: 512,
      region: 'us-east-1',
      invoke: () async {
        const id = 'kinesis_8f3a2c';
        return const KinesisPutResult(
            value: id, sequenceNumber: 'example', shardId: 'shard_8f3a2c');
      },
    ),
  );

  await OTel.shutdown();
}
