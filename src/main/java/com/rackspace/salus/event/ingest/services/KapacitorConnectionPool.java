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

import com.rackspace.salus.event.discovery.EngineInstance;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.springframework.stereotype.Service;

@Service
public class KapacitorConnectionPool implements Closeable {
  private final ConcurrentHashMap<EngineInstance, InfluxDB> influxConnections =
      new ConcurrentHashMap<>();

  public InfluxDB getConnection(EngineInstance engineInstance) {
    return influxConnections.computeIfAbsent(
        engineInstance,
        key ->
            InfluxDBFactory.connect(
                String.format("http://%s:%d", key.getHost(), key.getPort())
            )
    );
  }

  @Override
  public void close() throws IOException {
    influxConnections.forEachValue(1, InfluxDB::close);
  }
}
