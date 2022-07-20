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
import org.apache.flink.util.TestLogger;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KafkaSinkBuilder}. */
public class KafkaSinkBuilderTest extends TestLogger {

    private static final String[] DEFAULT_KEYS =
            new String[] {
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ProducerConfig.TRANSACTION_TIMEOUT_CONFIG
            };

    @Test
    public void testPropertyHandling() {
        validateProducerConfig(
                getBasicBuilder(),
                p -> {
                    Arrays.stream(DEFAULT_KEYS).forEach(k -> assertThat(p).containsKey(k));
                });

        validateProducerConfig(
                getBasicBuilder().setProperty("k1", "v1"),
                p -> {
                    Arrays.stream(DEFAULT_KEYS).forEach(k -> assertThat(p).containsKey(k));
                    p.containsKey("k1");
                });

        Properties testConf = new Properties();
        testConf.put("k1", "v1");
        testConf.put("k2", "v2");

        validateProducerConfig(
                getBasicBuilder().setKafkaProducerConfig(testConf),
                p -> {
                    Arrays.stream(DEFAULT_KEYS).forEach(k -> assertThat(p).containsKey(k));
                    testConf.forEach((k, v) -> assertThat(p.get(k)).isEqualTo(v));
                });

        validateProducerConfig(
                getBasicBuilder()
                        .setProperty("k1", "incorrect")
                        .setKafkaProducerConfig(testConf)
                        .setProperty("k2", "correct"),
                p -> {
                    Arrays.stream(DEFAULT_KEYS).forEach(k -> assertThat(p).containsKey(k));
                    assertThat(p).containsEntry("k1", "v1").containsEntry("k2", "correct");
                });
    }

    @Test
    public void testBootstrapServerSetting() {
        Properties testConf1 = new Properties();
        testConf1.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "testServer");

        validateProducerConfig(
                getNoServerBuilder().setKafkaProducerConfig(testConf1),
                p -> assertThat(p).containsKeys(DEFAULT_KEYS));
    }

    private void validateProducerConfig(
            KafkaSinkBuilder<?> builder, Consumer<Properties> validator) {
        validator.accept(builder.build().getKafkaProducerConfig());
    }

    private KafkaSinkBuilder<String> getBasicBuilder() {
        return new KafkaSinkBuilder<String>()
                .setBootstrapServers("testServer")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("topic")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build());
    }

    private KafkaSinkBuilder<String> getNoServerBuilder() {
        return new KafkaSinkBuilder<String>()
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic("topic")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build());
    }
}
