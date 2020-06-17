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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;

import com.rackspace.salus.event.discovery.EngineInstance;
import com.rackspace.salus.event.discovery.EventEnginePicker;
import com.rackspace.salus.event.ingest.config.EventIngestProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.client.AutoConfigureMockRestServiceServer;
import org.springframework.boot.test.autoconfigure.web.client.RestClientTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.MockRestServiceServer;

@RunWith(SpringRunner.class)
@RestClientTest(KapacitorConnectionPool.class)
@Import({IngestService.class, MeterRegistryTestConfig.class, KapacitorConnectionPool.class})
public class IngestServiceFailureTest {
  String baseURI = "";
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
  private MockRestServiceServer mockServer;

  @Autowired
  KapacitorConnectionPool pool;

  @Autowired
  IngestService ingestService;

  @MockBean
  EventEnginePicker eventEnginePicker;

  @Test
  public void testGetPolicyMonitor_doesntExist() {
    mockServer.expect(requestTo(baseURI+"/kapacitor/v1/write"))
        .andRespond(withStatus(HttpStatus.NOT_FOUND));

    //probably going to need to figure out how to mock out the original connection to the server too.
    EngineInstance eventEngine = eventEnginePicker.pickAll().stream().findFirst().get();
    pool.getConnection(eventEngine);

    //assertThat(result, nullValue());
  }

}
