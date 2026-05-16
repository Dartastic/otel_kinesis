// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry/testing.dart';
import 'package:otel_kinesis/otel_kinesis.dart';
import 'package:test/test.dart';

void main() {
  late TestHarness harness;
  late InMemorySpanExporter spans;

  setUpAll(() async {
    harness = await maybeInitializeOtelForTest(
      serviceName: 'otel_kinesis-test',
    );
    spans = harness.spans;
  });

  setUp(() => harness.clear());

  Map<String, Object> attrsOf(Span s) =>
      {for (final a in s.attributes.toList()) a.key: a.value};

  test('put records PRODUCER span with sequence + shard', () async {
    await tracedKinesisPutRecord<String>(
      streamName: 'orders-stream',
      partitionKey: 'user-1',
      payloadSize: 1024,
      region: 'us-east-1',
      invoke: () async => const KinesisPutResult(
        value: 'ok',
        sequenceNumber: '49500000000',
        shardId: 'shardId-000000000001',
      ),
    );
    final span = spans.findSpanByName('put orders-stream');
    expect(span, isNotNull);
    expect(span!.kind, SpanKind.producer);
    final a = attrsOf(span);
    expect(a['messaging.system'], 'aws_kinesis');
    expect(a['messaging.operation'], 'publish');
    expect(a['messaging.destination.name'], 'orders-stream');
    expect(a['aws.kinesis.stream.name'], 'orders-stream');
    expect(a['aws.kinesis.partition.key'], 'user-1');
    expect(a['messaging.message.body.size'], 1024);
    expect(a['cloud.region'], 'us-east-1');
    expect(a['messaging.message.id'], '49500000000');
    expect(a['aws.kinesis.sequence.number'], '49500000000');
    expect(a['aws.kinesis.shard.id'], 'shardId-000000000001');
  });

  test('get records CONSUMER span with batch + shard', () async {
    await tracedKinesisGetRecords<String>(
      streamName: 'orders-stream',
      shardId: 'shardId-000000000002',
      batchSize: 50,
      region: 'us-west-2',
      invoke: () async => 'done',
    );
    final span = spans.findSpanByName('receive orders-stream');
    expect(span, isNotNull);
    expect(span!.kind, SpanKind.consumer);
    final a = attrsOf(span);
    expect(a['messaging.operation'], 'receive');
    expect(a['aws.kinesis.shard.id'], 'shardId-000000000002');
    expect(a['messaging.batch.message_count'], 50);
    expect(a['cloud.region'], 'us-west-2');
  });

  test('records exception', () async {
    await expectLater(
      tracedKinesisPutRecord<String>(
        streamName: 's',
        invoke: () async => throw StateError('boom'),
      ),
      throwsA(isA<StateError>()),
    );
    expect(spans.findSpanByName('put s')!.status, SpanStatusCode.Error);
  });

  test('suppression skips spans', () async {
    await runWithoutKinesisInstrumentationAsync(() async {
      await tracedKinesisPutRecord<String>(
        streamName: 's',
        invoke: () async => const KinesisPutResult(value: 'v'),
      );
      await tracedKinesisGetRecords<String>(
        streamName: 's',
        shardId: 'sh-1',
        invoke: () async => 'ok',
      );
    });
    expect(spans.findSpansStartingWith('put '), isEmpty);
    expect(spans.findSpansStartingWith('receive '), isEmpty);
  });
}
