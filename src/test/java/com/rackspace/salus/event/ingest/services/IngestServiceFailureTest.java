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
 *
 */

package com.rackspace.salus.event.ingest.services;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.rackspace.monplat.protocol.ExternalMetric;
import com.rackspace.salus.event.discovery.EngineInstance;
import com.rackspace.salus.event.discovery.EventEnginePicker;
import com.rackspace.salus.event.discovery.NoPartitionsAvailableException;
import com.rackspace.salus.event.ingest.config.EventIngestProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockserver.client.server.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.Header;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.client.RestClientTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@RestClientTest(KapacitorConnectionPool.class)
@Import({IngestService.class, MeterRegistryTestConfig.class, KapacitorConnectionPool.class})
public class IngestServiceFailureTest {
  @Configuration
  @Import({IngestService.class, MeterRegistryTestConfig.class})
  public static class TestConfig {
    @Bean
    public EventIngestProperties eventIngestProperties() {
      return new EventIngestProperties();
    }

    @Bean
    public RestTemplateBuilder restTemplateBuilder() {
      return new RestTemplateBuilder()
          .rootUri("");
    }
  }

  @Autowired
  KapacitorConnectionPool pool;

  @Autowired
  IngestService ingestService;

  @MockBean
  EventEnginePicker eventEnginePicker;

  @Autowired
  MeterRegistry meterRegistry;

  private static ClientAndServer mockServer;

  @BeforeClass
  public static void startServer() {
    mockServer = startClientAndServer(1080);
  }

  @Test
  public void testKapacitorWriteFailure()
      throws NoPartitionsAvailableException, InterruptedException {

    createExpectationForWriteFailure();

    EngineInstance engineInstance = new EngineInstance("127.0.0.1", 1080, 1);
    when(eventEnginePicker.pickRecipient(any(), any(), any())).thenReturn(engineInstance);

    pool.getConnection(engineInstance);

    //exceed BATCH buffer limit
    for(int i = 0; i < 10001; i++) {
      ExternalMetric metric = MetricTestUtils.buildMetric();
      ingestService.consumeMetric(metric);
    }
    Thread.sleep(1000);
    Counter counter = meterRegistry.find("errors").tag("operation", "batchFailure").counter();
    assertThat(counter.count()).isEqualTo(1);
  }


  private void createExpectationForWriteFailure() {
    new MockServerClient("127.0.0.1", 1080)
        .when(
            request()
                .withMethod("POST")
                .withPath("/kapacitor/v1/write")
                .withHeader("\"Content-type\", \"application/json\""),
            exactly(1)
        )
        .respond(
            response()
                .withStatusCode(404)
                .withHeaders(
                    new Header("Content-Type", "application/json; charset=utf-8"),
                    new Header("Cache-Control", "public, max-age=86400")
                )
                .withBody("{ message: 'incorrect username and password combination' }")
                .withDelay(TimeUnit.SECONDS,1)
        );
  }

  @AfterClass
  public static void stopServer() {
    mockServer.stop();
  }
}
