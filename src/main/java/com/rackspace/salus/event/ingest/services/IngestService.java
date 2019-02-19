/*
 * Copyright 2019 Rackspace US, Inc.
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

import com.rackspace.monplat.protocol.ExternalMetric;
import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.event.common.InfluxScope;
import com.rackspace.salus.event.common.Tags;
import com.rackspace.salus.event.discovery.EngineInstance;
import com.rackspace.salus.event.discovery.EventEnginePicker;
import com.rackspace.salus.event.ingest.config.EventIngestProperties;
import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Point.Builder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
@Slf4j
public class IngestService implements Closeable {

  private final KafkaTopicProperties kafkaTopicProperties;
  private final EventIngestProperties eventIngestProperties;
  private final EventEnginePicker eventEnginePicker;
  private final ConcurrentHashMap<EngineInstance, InfluxDB> influxConnections =
      new ConcurrentHashMap<>();
  private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ISO_INSTANT;

  @Autowired
  public IngestService(KafkaTopicProperties kafkaTopicProperties,
                       EventIngestProperties eventIngestProperties,
                       EventEnginePicker eventEnginePicker) {
    this.kafkaTopicProperties = kafkaTopicProperties;
    this.eventIngestProperties = eventIngestProperties;
    this.eventEnginePicker = eventEnginePicker;
  }

  public String getTopic() {
    return kafkaTopicProperties.getMetrics();
  }

  @KafkaListener(topics = "#{__listener.topic}")
  public void consumeMetric(ExternalMetric metric) {
    log.trace("Ingesting metric={}", metric);

    final EngineInstance engineInstance = eventEnginePicker
        .pickRecipient(metric.getAccount(), metric.getCollectionName());

    log.debug("Sending measurement={} for tenant={} to engine={}",
        metric.getCollectionName(), metric.getAccount(), engineInstance);

    final InfluxDB kapacitorWriter = influxConnections.computeIfAbsent(
        engineInstance,
        key ->
            InfluxDBFactory.connect(
                String.format("http://%s:%d", key.getHost(), key.getPort())
            )
    );

    final Instant timestamp = Instant.parse(metric.getTimestamp());
    final Builder pointBuilder = Point.measurement(metric.getCollectionName())
        .time(timestamp.toEpochMilli(), TimeUnit.MILLISECONDS);

    metric.getSystemMetadata().forEach(pointBuilder::tag);
    pointBuilder.tag(Tags.ACCOUNT_TYPE, metric.getAccountType().toString());
    pointBuilder.tag(Tags.ACCOUNT, metric.getAccount());
    pointBuilder.tag(Tags.RESOURCE_ID, metric.getDevice());
    if (StringUtils.hasText(metric.getDeviceLabel())) {
      pointBuilder.tag(Tags.RESOURCE_LABEL, metric.getDeviceLabel());
    }
    pointBuilder.tag(Tags.MONITORING_SYSTEM, metric.getMonitoringSystem().toString());

    metric.getIvalues().forEach(pointBuilder::addField);
    metric.getFvalues().forEach(pointBuilder::addField);
    metric.getSvalues().forEach(pointBuilder::addField);

    kapacitorWriter.write(
        deriveIngestDatabase(metric),
        deriveRetentionPolicy(),
        pointBuilder.build());
  }

  private String deriveRetentionPolicy() {
    final String override = eventIngestProperties
        .getInfluxDbRetentionPolicyOverride();

    return StringUtils.hasText(override) ? override : InfluxScope.INGEST_RETENTION_POLICY;
  }

  private String deriveIngestDatabase(ExternalMetric metric) {
    final String dbOverride = eventIngestProperties.getInfluxDbDatabaseOverride();
    return StringUtils.hasText(dbOverride) ? dbOverride : metric.getAccount();
  }

  @Override
  public void close() throws IOException {
    influxConnections.forEach((key, influxDB) -> influxDB.close());
  }
}
