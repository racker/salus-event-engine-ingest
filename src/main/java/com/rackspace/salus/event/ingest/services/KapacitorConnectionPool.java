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

import com.rackspace.salus.event.discovery.EngineInstance;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.Closeable;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KapacitorConnectionPool implements Closeable {
  private final ConcurrentHashMap<EngineInstance, InfluxDB> influxConnections =
      new ConcurrentHashMap<>();

  private final Counter batchIngestFailure;

  public KapacitorConnectionPool(MeterRegistry meterRegistry) {
    this.batchIngestFailure = meterRegistry.counter("errors","operation", "batchFailure");
  }

  public InfluxDB getConnection(EngineInstance engineInstance) {
    return influxConnections.computeIfAbsent(
        engineInstance,
        key -> {
            InfluxDB influxDB = InfluxDBFactory.connect(
                String.format("http://%s:%d", key.getHost(), key.getPort()));
            influxDB.enableBatch(BatchOptions.DEFAULTS.exceptionHandler((points, throwable) -> {
              batchIngestFailure.increment();
              log.error("Kafka Ingestion error with Batch: {}", points, throwable);
            }));
            return influxDB;
        }
    );
  }

  @Override
  public void close() {
    influxConnections.forEachValue(1, InfluxDB::close);
  }
}
