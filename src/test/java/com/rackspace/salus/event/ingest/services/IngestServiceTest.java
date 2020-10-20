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

import static com.rackspace.salus.event.ingest.services.MetricTestUtils.verifyEventEnginePicker;
import static com.rackspace.salus.event.ingest.services.MetricTestUtils.verifyInfluxPointWritten;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.rackspace.monplat.protocol.UniversalMetricFrame;
import com.rackspace.salus.event.discovery.EngineInstance;
import com.rackspace.salus.event.discovery.EventEnginePicker;
import com.rackspace.salus.event.discovery.NoPartitionsAvailableException;
import com.rackspace.salus.event.ingest.config.EventIngestProperties;
import org.influxdb.InfluxDB;
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

/**
 * These unit tests focus on the behavior of {@link IngestService} without the involvement of Kafka.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class IngestServiceTest {

  @Configuration
  @Import({IngestService.class, MeterRegistryTestConfig.class})
  public static class TestConfig {
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
  public void consumeMetric() throws NoPartitionsAvailableException {
    final UniversalMetricFrame metric = MetricTestUtils.buildMetric();

    when(eventEnginePicker.pickRecipient(any(), any(), any()))
        .thenReturn(
            new EngineInstance("host", 123, 3)
        );

    when(kapacitorConnectionPool.getConnection(any()))
        .thenReturn(influxDB);

    ingestService.consumeMetric(metric);

    verifyEventEnginePicker(eventEnginePicker, times(1));

    verify(kapacitorConnectionPool).getConnection(eq(new EngineInstance("host", 123, 3)));

    verifyInfluxPointWritten(influxDB, times(1));

    verifyNoMoreInteractions(eventEnginePicker, kapacitorConnectionPool, influxDB);
  }

}