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
import static java.util.Collections.singletonMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import com.google.protobuf.Timestamp;
import com.rackspace.monplat.protocol.Metric;
import com.rackspace.monplat.protocol.MonitoringSystem;
import com.rackspace.monplat.protocol.UniversalMetricFrame;
import com.rackspace.salus.event.common.InfluxScope;
import com.rackspace.salus.event.common.Tags;
import com.rackspace.salus.event.discovery.EventEnginePicker;
import com.rackspace.salus.event.discovery.NoPartitionsAvailableException;
import com.rackspace.salus.telemetry.model.LabelNamespaces;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.mockito.verification.VerificationMode;

class MetricTestUtils {

  static UniversalMetricFrame buildMetric() {
    final Timestamp timestamp = Timestamp.newBuilder()
        .setSeconds(1598458510).build();
    List<Metric> metricList = new ArrayList<>();
    metricList.add(Metric.newBuilder()
        .setGroup("cpu")
        .setTimestamp(timestamp)
        .setName("fvalue")
        .setFloat(3.14)
        .putAllMetadata(singletonMap("ckey", "cval"))
        .build());
    metricList.add(Metric.newBuilder()
        .setGroup("cpu")
        .setTimestamp(timestamp)
        .setName("svalue")
        .setString("testing")
        .putAllMetadata(singletonMap("ckey", "cval"))
        .build());
    metricList.add(Metric.newBuilder()
        .setGroup("cpu")
        .setTimestamp(timestamp)
        .setName("ivalue")
        .setInt(5)
        .putAllMetadata(singletonMap("ckey", "cval"))
        .build());
    return UniversalMetricFrame.newBuilder()
        .setTenantId("123456")
        .setAccountType(UniversalMetricFrame.AccountType.MANAGED_HOSTING)
        .setMonitoringSystem(UniversalMetricFrame.MonitoringSystem.SALUS)
        .putAllSystemMetadata(singletonMap("skey", "sval"))
        .putAllDeviceMetadata(singletonMap("dkey", "dval"))
        .setDevice("r-1")
        .addAllMetrics(metricList)
        .build();
  }

  static void verifyInfluxPointWritten(InfluxDB influxDB, VerificationMode verificationMode) {
    Point point = Point.measurement("cpu")
        .time(1598458510000L, TimeUnit.MILLISECONDS)
        .tag(Tags.TENANT, "123456")
        .tag(Tags.MONITORING_SYSTEM, MonitoringSystem.SALUS.toString())
        .tag(Tags.RESOURCE_ID, "r-1")
        .tag(LabelNamespaces.applyNamespace(MONITORING_SYSTEM_METADATA, "skey"), "sval")
        .tag("dkey", "dval")
        .tag("ckey", "cval")
        .addField("ivalue", 5L)
        .addField("fvalue", 3.14D)
        .addField("svalue", "testing")
        .build();

    verify(influxDB, verificationMode).write(
        eq("123456"),
        eq(InfluxScope.INGEST_RETENTION_POLICY),
        eq(point)
    );
  }

  static void verifyEventEnginePicker(EventEnginePicker eventEnginePicker,
                                      VerificationMode verificationMode)
      throws NoPartitionsAvailableException {
    verify(eventEnginePicker, verificationMode).pickRecipient(
        eq("123456"),
        eq("r-1"),
        eq("cpu")
    );
  }
}
