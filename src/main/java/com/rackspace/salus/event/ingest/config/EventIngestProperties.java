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

package com.rackspace.salus.event.ingest.config;

import java.util.List;
import javax.validation.constraints.NotEmpty;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

@ConfigurationProperties("salus.event.ingest")
@Component
@Data
@Validated
public class EventIngestProperties {

  String influxDbDatabaseOverride;

  String influxDbRetentionPolicyOverride;

  /**
   * The delimiter to use when constructing a qualified account value.
   */
  @NotEmpty
  String qualifiedAccountDelimiter = ":";

  /**
   * Topics to be ingested that contain com.rackspace.monplat.protocol.ExternalMetric messages encoded as JSON.
   */
  @NotEmpty
  List<String> topics = List.of("telemetry.metrics.json");
}
