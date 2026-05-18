// Licensed under the Dartastic Pro Software License Agreement.
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';

import 'otel_kinesis_suppression.dart';

const _tracerName = 'dartastic_otel_libs_kinesis';
const _systemValue = 'aws_kinesis';

Tracer _tracer() => OTel.tracerProvider().getTracer(_tracerName);

/// Pair returned from `tracedKinesisPutRecord` callback.
class KinesisPutResult<T> {
  const KinesisPutResult({
    required this.value,
    this.sequenceNumber,
    this.shardId,
  });
  final T value;
  final String? sequenceNumber;
  final String? shardId;
}

/// Wraps a Kinesis `PutRecord` / `PutRecords` in a PRODUCER span
/// using OTel messaging semconv (messaging.system=aws_kinesis).
Future<T> tracedKinesisPutRecord<T>({
  required String streamName,
  required Future<KinesisPutResult<T>> Function() invoke,
  String? partitionKey,
  int? payloadSize,
  String? region,
}) async {
  if (kinesisInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }
  final m = <String, Object>{
    'messaging.system': _systemValue,
    'messaging.operation': 'publish',
    'messaging.destination.name': streamName,
    'aws.kinesis.stream.name': streamName,
  };
  if (partitionKey != null) m['aws.kinesis.partition.key'] = partitionKey;
  if (payloadSize != null) m['messaging.message.body.size'] = payloadSize;
  if (region != null) m['cloud.region'] = region;

  final span = _tracer().startSpan(
    'put $streamName',
    kind: SpanKind.producer,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    final r = await invoke();
    final extras = <Attribute<Object>>[];
    if (r.sequenceNumber != null) {
      extras.add(OTel.attributeString(
        'messaging.message.id',
        r.sequenceNumber!,
      ));
      extras.add(OTel.attributeString(
        'aws.kinesis.sequence.number',
        r.sequenceNumber!,
      ));
    }
    if (r.shardId != null) {
      extras.add(OTel.attributeString('aws.kinesis.shard.id', r.shardId!));
    }
    if (extras.isNotEmpty) span.addAttributes(OTel.attributes(extras));
    return r.value;
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

/// Wraps a Kinesis `GetRecords` consumer iteration in a CONSUMER
/// span. Call inside the loop that pulls and processes records.
Future<T> tracedKinesisGetRecords<T>({
  required String streamName,
  required String shardId,
  required Future<T> Function() invoke,
  int? batchSize,
  String? region,
}) async {
  if (kinesisInstrumentationSuppressed()) {
    return invoke();
  }
  final m = <String, Object>{
    'messaging.system': _systemValue,
    'messaging.operation': 'receive',
    'messaging.destination.name': streamName,
    'aws.kinesis.stream.name': streamName,
    'aws.kinesis.shard.id': shardId,
  };
  if (batchSize != null) m['messaging.batch.message_count'] = batchSize;
  if (region != null) m['cloud.region'] = region;

  final span = _tracer().startSpan(
    'receive $streamName',
    kind: SpanKind.consumer,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}
