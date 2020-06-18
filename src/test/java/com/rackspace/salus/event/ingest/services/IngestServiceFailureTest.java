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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;

import com.rackspace.salus.event.discovery.EngineInstance;
import com.rackspace.salus.event.discovery.EventEnginePicker;
import com.rackspace.salus.event.discovery.NoPartitionsAvailableException;
import com.rackspace.salus.event.ingest.config.EventIngestProperties;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.influxdb.InfluxDB;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.client.RestClientTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

//@RunWith(SpringRunner.class)
@RestClientTest
@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(SpringRunner.class)
@PrepareForTest(InetAddress.class)
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
    public RestTemplate restTemplate(RestTemplateBuilder restTemplateBuilder) {
      return restTemplateBuilder.build();
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

  @Mock
  InfluxDB influxDB;

  @Test
  public void testGetPolicyMonitor_doesntExist()
      throws NoPartitionsAvailableException, UnknownHostException {
    mockServer.expect(requestTo(baseURI+"/kapacitor/v1/write"))
        .andRespond(withStatus(HttpStatus.NOT_FOUND));
    when(eventEnginePicker.pickRecipient(any(), any(), any()))
        .thenReturn(
            new EngineInstance("host", 123, 3)
        );

    InetAddress address = InetAddress.getLocalHost();

    PowerMockito.mockStatic(InetAddress.class);
    PowerMockito.when(InetAddress.getByName(any()))
        .thenReturn(InetAddress.getLocalHost()/*return a new InetAddress*/);

    // I need to setup InetAddress.getByName(String host);

    // We need to create an InfluxDB Object to return
    // So the problem is that InfluxDBFacory isn't Mocked
    //when(InfluxDBFactory.connect(anyString())).thenReturn(influxDB);

    // probably going to need to figure out how to mock out the original connection to the server too.
    EngineInstance eventEngine = eventEnginePicker.pickRecipient("t-1", "r-1", "c-1");
    InfluxDB kapacitorWriter = pool.getConnection(eventEngine);
    kapacitorWriter.write("This should fail I hope");

    ingestService.consumeMetric(MetricTestUtils.buildMetric());
    //assertThat(result, nullValue());
  }

}
