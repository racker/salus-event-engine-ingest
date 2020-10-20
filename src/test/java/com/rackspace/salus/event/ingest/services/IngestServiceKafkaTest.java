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
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.rackspace.monplat.protocol.UniversalMetricFrame;
import com.rackspace.salus.common.messaging.EnableSalusKafkaMessaging;
import com.rackspace.salus.event.discovery.EngineInstance;
import com.rackspace.salus.event.discovery.EventEnginePicker;
import com.rackspace.salus.event.discovery.NoPartitionsAvailableException;
import com.rackspace.salus.event.ingest.config.EventIngestProperties;
import org.influxdb.InfluxDB;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * These unit tests focus on the Kafka integration of the {@link IngestService} and not so much on the
 * functionality of that service. The {@link IngestServiceTest} focuses on functional tests.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(
    classes = {
        IngestService.class,
        EventIngestProperties.class,
        MeterRegistryTestConfig.class
    },
    properties = {
        "salus.event.ingest.topics="+ IngestServiceKafkaTest.TOPIC,
        // override app default so that we can produce before consumer is ready
        "spring.kafka.consumer.auto-offset-reset=earliest",
        "logging.level.com.rackspace.salus.common.messaging.KafkaErrorConfig=debug"
    }
)
@EnableSalusKafkaMessaging
@ImportAutoConfiguration({
    KafkaAutoConfiguration.class
})
@EmbeddedKafka(topics = IngestServiceKafkaTest.TOPIC)
public class IngestServiceKafkaTest {
  static {
    System.setProperty(
        EmbeddedKafkaBroker.BROKER_LIST_PROPERTY, "spring.kafka.bootstrap-servers");
  }

  static final String TOPIC = "metrics_test";

  @MockBean
  EventEnginePicker eventEnginePicker;

  @MockBean
  KapacitorConnectionPool kapacitorConnectionPool;

  @Mock
  InfluxDB influxDB;

  @Autowired
  IngestService ingestService;

  @Autowired
  private KafkaTemplate<String, Object> kafkaTemplate;

  @Test
  public void testDeserializerFailure()
      throws NoPartitionsAvailableException, InvalidProtocolBufferException {
    when(eventEnginePicker.pickRecipient(any(), any(), any()))
        .thenReturn(
            new EngineInstance("host", 123, 3)
        );

    when(kapacitorConnectionPool.getConnection(any()))
        .thenReturn(influxDB);

    // send a bogus Avro ExternalMetric json object
    kafkaTemplate.send(TOPIC, "{}");

    UniversalMetricFrame validMetric = MetricTestUtils.buildMetric();

    System.out.println("Testing!!!!!!!!!!!!!!"+ JsonFormat.printer().print(validMetric));
    // ...and send a valid one so we can assert the processing skipped over bad one
    kafkaTemplate.send(TOPIC, JsonFormat.printer().print(validMetric));

    // use timeout in verification to allow for async consumer startup
    verifyEventEnginePicker(eventEnginePicker, timeout(5000));

    verify(kapacitorConnectionPool, timeout(5000))
        .getConnection(eq(new EngineInstance("host", 123, 3)));

    verifyInfluxPointWritten(influxDB, timeout(5000));

    verifyNoMoreInteractions(eventEnginePicker, kapacitorConnectionPool, influxDB);
  }
}