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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import com.rackspace.monplat.protocol.AccountType;
import com.rackspace.monplat.protocol.ExternalMetric;
import com.rackspace.monplat.protocol.MonitoringSystem;
import com.rackspace.salus.event.common.InfluxScope;
import com.rackspace.salus.event.common.Tags;
import com.rackspace.salus.event.discovery.EventEnginePicker;
import com.rackspace.salus.event.discovery.NoPartitionsAvailableException;
import com.rackspace.salus.telemetry.model.LabelNamespaces;
import java.util.concurrent.TimeUnit;
import org.influxdb.InfluxDB;
import org.influxdb.dto.Point;
import org.mockito.verification.VerificationMode;

class MetricTestUtils {

  static ExternalMetric buildMetric() {
    return ExternalMetric.newBuilder()
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
  }

  static void verifyInfluxPointWritten(InfluxDB influxDB, VerificationMode verificationMode) {
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

    verify(influxDB, verificationMode).write(
        eq("CORE:123456"),
        eq(InfluxScope.INGEST_RETENTION_POLICY),
        eq(point)
    );
  }

  static void verifyEventEnginePicker(EventEnginePicker eventEnginePicker,
                                      VerificationMode verificationMode)
      throws NoPartitionsAvailableException {
    verify(eventEnginePicker, verificationMode).pickRecipient(
        eq("CORE:123456"),
        eq("r-1"),
        eq("cpu")
    );
  }
}