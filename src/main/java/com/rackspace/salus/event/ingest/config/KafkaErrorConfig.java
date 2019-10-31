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

package com.rackspace.salus.event.ingest.config;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.ConsumerAwareErrorHandler;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer2;

@Configuration
@Slf4j
public class KafkaErrorConfig {

  /**
   * Gets picked up by Spring Boot Kafka autoconfig and registered with the default.
   * This handler provides additional debug logging when {@link ErrorHandlingDeserializer2} is
   * configured as the value deserializer since the original cause and payload can be extracted
   * from the header set by that deserializer.
   * {@link ConcurrentKafkaListenerContainerFactoryConfigurer} bean.
   */
  @Bean
  public ConsumerAwareErrorHandler listenerContainerErrorHandler() {
    return (e, data, consumer) -> {
      final TopicPartition topicPartition = new TopicPartition(data.topic(), data.partition());
      log.warn("Handling listener container error by skipping offset={} in={}", data.offset(), topicPartition);
      consumer.seek(topicPartition, data.offset()+1);

      if (log.isDebugEnabled()) {

        final Iterable<Header> headerValues = data.headers()
            .headers(ErrorHandlingDeserializer2.VALUE_DESERIALIZER_EXCEPTION_HEADER);
        // headerValues will be empty if the exception header isn't present, so extra logging is skipped
        for (Header headerValue : headerValues) {
          try {
            final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(headerValue.value()));
            final Object exceptionObj = ois.readObject();
            if (exceptionObj instanceof DeserializationException) {

              final DeserializationException deserEx = (DeserializationException) exceptionObj;
              log.debug("Deserialization failed at offset={} in={} with raw data='{}'",
                  data.offset(), topicPartition, new String(deserEx.getData(), StandardCharsets.UTF_8),
                  // and include the exception itself in log to reveal root cause
                  deserEx);

            } else {
              log.debug("Deserializer exception was unexpected type: {}", exceptionObj);
            }
          } catch (IOException | ClassNotFoundException ex) {
            log.debug("Unable to build or read from ObjectInputStream for inspecting deserializer exception", ex);
          }
        }

      }
    };
  }
}
