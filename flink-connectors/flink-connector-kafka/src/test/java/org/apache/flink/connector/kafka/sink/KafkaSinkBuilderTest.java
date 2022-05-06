/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connectors.test.common.junit.extensions.TestLoggerExtension;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Tests for {@link KafkaSinkBuilder}. */
@ExtendWith(TestLoggerExtension.class)
public class KafkaSinkBuilderTest {

    @Test
    public void testBootstrapServerSettingWithProperties() {
        Properties testConf = new Properties();
        testConf.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "testServer");
        KafkaSinkBuilder<String> builder =
                new KafkaSinkBuilder<String>()
                        .setKafkaProducerConfig(testConf)
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic("topic")
                                        .setValueSerializationSchema(new SimpleStringSchema())
                                        .build());

        assertDoesNotThrow(builder::build);
    }
}
