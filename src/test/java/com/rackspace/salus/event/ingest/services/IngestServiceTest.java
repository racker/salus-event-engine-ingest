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

import static com.rackspace.salus.telemetry.model.LabelNamespaces.MONITORING_SYSTEM_METADATA;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.rackspace.monplat.protocol.AccountType;
import com.rackspace.monplat.protocol.ExternalMetric;
import com.rackspace.monplat.protocol.MonitoringSystem;
import com.rackspace.salus.common.messaging.KafkaTopicProperties;
import com.rackspace.salus.event.common.InfluxScope;
import com.rackspace.salus.event.common.Tags;
import com.rackspace.salus.event.discovery.EngineInstance;
import com.rackspace.salus.event.discovery.EventEnginePicker;
import com.rackspace.salus.event.ingest.config.EventIngestProperties;
import com.rackspace.salus.telemetry.model.LabelNamespaces;
import java.util.concurrent.TimeUnit;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class IngestServiceTest {

  @Configuration
  @Import({IngestService.class})
  public static class TestConfig {
    @Bean
    public KafkaTopicProperties kafkaTopicProperties() {
      return new KafkaTopicProperties();
    }

    @Bean
    public EventIngestProperties eventIngestProperties() {
      return new EventIngestProperties();
    }
  }

  @MockBean
  EventEnginePicker eventEnginePicker;

  @MockBean
  KapacitorConnectionPool kapacitorConnectionPool;

  @Autowired
  IngestService ingestService;

  @Mock
  InfluxDB influxDB;

  @Test
  public void consumeMetric() {
    final ExternalMetric metric = ExternalMetric.newBuilder()
        .setTimestamp("2018-03-27T13:15:06.497Z")
        .setAccountType(AccountType.CORE)
        .setAccount("123456")
        .setMonitoringSystem(MonitoringSystem.SALUS)
        .setDevice("r-1")
        .setDeviceLabel("resourceLabel-1")
        .setSystemMetadata(singletonMap("skey", "sval"))
        .setDeviceMetadata(singletonMap("dkey", "dval"))
        .setCollectionName("cpu")
        .setCollectionMetadata(singletonMap("ckey", "cval"))
        .setCollectionTarget("")
        .setIvalues(singletonMap("ivalue", 5L))
        .setFvalues(singletonMap("fvalue", 3.14))
        .setSvalues(singletonMap("svalue", "testing"))
        .setUnits(emptyMap())
        .build();

    when(eventEnginePicker.pickRecipient(any(), any(), any()))
        .thenReturn(
            new EngineInstance("host", 123, 3)
        );

    when(kapacitorConnectionPool.getConnection(any()))
        .thenReturn(influxDB);

    ingestService.consumeMetric(metric);

    verify(eventEnginePicker).pickRecipient(
        eq("123456"),
        eq("r-1"),
        eq("cpu")
    );

    verify(kapacitorConnectionPool).getConnection(eq(new EngineInstance("host", 123, 3)));

    Point point = Point.measurement("cpu")
        .time(1522156506497L, TimeUnit.MILLISECONDS)
        .tag(Tags.QUALIFIED_ACCOUNT, "CORE:123456")
        .tag(Tags.MONITORING_SYSTEM, MonitoringSystem.SALUS.toString())
        .tag(Tags.RESOURCE_ID, "r-1")
        .tag(Tags.RESOURCE_LABEL, "resourceLabel-1")
        .tag(LabelNamespaces.applyNamespace(MONITORING_SYSTEM_METADATA, "skey"), "sval")
        .tag("dkey", "dval")
        .tag("ckey", "cval")
        .addField("ivalue", 5L)
        .addField("fvalue", 3.14D)
        .addField("svalue", "testing")
        .build();

    verify(influxDB).write(
        eq("123456"),
        eq(InfluxScope.INGEST_RETENTION_POLICY),
        eq(point)
    );

    verifyNoMoreInteractions(eventEnginePicker, kapacitorConnectionPool, influxDB);
  }
}