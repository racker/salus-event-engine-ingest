/*
 * Copyright 2020 Rackspace US, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.rackspace.salus.event.ingest.services;

import static com.rackspace.salus.telemetry.model.LabelNamespaces.MONITORING_SYSTEM_METADATA;

import com.google.protobuf.Timestamp;
import com.rackspace.monplat.protocol.UniversalMetricFrame;
import com.rackspace.salus.event.common.InfluxScope;
import com.rackspace.salus.event.common.Tags;
import com.rackspace.salus.event.discovery.EngineInstance;
import com.rackspace.salus.event.discovery.EventEnginePicker;
import com.rackspace.salus.event.discovery.NoPartitionsAvailableException;
import com.rackspace.salus.event.ingest.config.EventIngestProperties;
import com.rackspace.salus.telemetry.model.LabelNamespaces;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
@Slf4j
public class IngestService implements Closeable {

  private final EventIngestProperties eventIngestProperties;
  private final EventEnginePicker eventEnginePicker;
  private final KapacitorConnectionPool kapacitorConnectionPool;
  private final ConcurrentHashMap<EngineInstance, InfluxDB> influxConnections =
      new ConcurrentHashMap<>();
  private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_INSTANT;
  private final Counter metricsConsumed;
  private final Counter metricsWrittenToKapacitor;

  @Autowired
  public IngestService(EventIngestProperties eventIngestProperties,
                       EventEnginePicker eventEnginePicker,
                       KapacitorConnectionPool kapacitorConnectionPool,
                       MeterRegistry meterRegistry) {
    this.eventIngestProperties = eventIngestProperties;
    this.eventEnginePicker = eventEnginePicker;
    this.kapacitorConnectionPool = kapacitorConnectionPool;
    metricsConsumed = meterRegistry.counter("ingest","operation", "consume");
    metricsWrittenToKapacitor = meterRegistry.counter("ingest","operation", "written");
    }

  public List<String> getTopics() {
    return eventIngestProperties.getTopics();
  }

  @KafkaListener(topics = "#{__listener.topics}")
  public void consumeMetric(UniversalMetricFrame metric) {
    log.trace("Ingesting metric={}", metric);
    metricsConsumed.increment();

    final String tenant = metric.getTenantId();

    final EngineInstance engineInstance;
    final String resourceId = metric.getDevice();
    try {
      engineInstance = eventEnginePicker
          .pickRecipient(tenant, resourceId, metric.getMetrics(0).getGroup());
    } catch (NoPartitionsAvailableException e) {
      log.warn("No instances were available for routing of metric={}", metric);
      return;
    }

    // Kapacitor provides a write endpoint that is compatible with InfluxDB, which is why
    // a native InfluxDB client is used here.
    final InfluxDB kapacitorWriter = kapacitorConnectionPool.getConnection(engineInstance);
    Timestamp protobufTimestamp = metric.getMetrics(0).getTimestamp();
    final Instant timestamp = Instant.ofEpochSecond(protobufTimestamp.getSeconds(), protobufTimestamp.getNanos());
    final Builder pointBuilder = Point.measurement(metric.getMetrics(0).getGroup())
        .time(timestamp.toEpochMilli(), TimeUnit.MILLISECONDS);

    metric.getSystemMetadata()
        .forEach((name, value) ->
            pointBuilder.tag(
                LabelNamespaces.applyNamespace(MONITORING_SYSTEM_METADATA, name),
                value
            ));
    metric.getMetrics(0).getMetadataMap().forEach(pointBuilder::tag);
    metric.getDeviceMetadataMap().forEach(pointBuilder::tag);
    pointBuilder.tag(Tags.TENANT, tenant);
    pointBuilder.tag(Tags.RESOURCE_ID, resourceId);
    pointBuilder.tag(Tags.MONITORING_SYSTEM, metric.getMonitoringSystem().toString());

    metric.getMetricsList().stream().forEach(val -> {
      if(!StringUtils.isEmpty(val.getString())) {
        pointBuilder.addField(val.getName(), val.getString());
      } else if(val.getFloat()!=0.0) {
        pointBuilder.addField(val.getName(), val.getFloat());
      } else if(val.getInt()!=0) {
        pointBuilder.addField(val.getName(), val.getInt());
      }
    });

    final Point influxPoint = pointBuilder.build();

    log.trace("Sending influxPoint={} for tenant={} resourceId={} to engine={}",
        influxPoint, tenant, resourceId, engineInstance);

    kapacitorWriter.write(
        deriveIngestDatabase(tenant),
        deriveRetentionPolicy(),
        influxPoint
    );
    metricsWrittenToKapacitor.increment();
  }

  private String deriveRetentionPolicy() {
    final String override = eventIngestProperties
        .getInfluxDbRetentionPolicyOverride();

    return StringUtils.hasText(override) ? override : InfluxScope.INGEST_RETENTION_POLICY;
  }

  private String deriveIngestDatabase(String qualifiedAccountId) {
    final String dbOverride = eventIngestProperties.getInfluxDbDatabaseOverride();
    return StringUtils.hasText(dbOverride) ? dbOverride : qualifiedAccountId;
  }

  @Override
  public void close() throws IOException {
    influxConnections.forEach((key, influxDB) -> influxDB.close());
  }
}
