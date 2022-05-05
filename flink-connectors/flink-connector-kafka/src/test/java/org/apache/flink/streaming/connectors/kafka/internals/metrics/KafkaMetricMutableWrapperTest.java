/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals.metrics;

import org.apache.flink.util.TestLoggerExtension;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.createKafkaContainer;
import static org.apache.flink.util.DockerImageVersions.KAFKA;

@Testcontainers
@ExtendWith(TestLoggerExtension.class)
class KafkaMetricMutableWrapperTest {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMetricMutableWrapperTest.class);
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();

    @Container
    public static final KafkaContainer KAFKA_CONTAINER =
            createKafkaContainer(KAFKA, LOG)
                    .withEmbeddedZookeeper()
                    .withNetwork(NETWORK)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @Test
    void testOnlyMeasurableMetricsAreRegistered() {
        final Collection<KafkaMetricMutableWrapper> metricWrappers = new ArrayList<>();
        final KafkaConsumer<?, ?> consumer = new KafkaConsumer<>(getKafkaClientConfiguration());
        final KafkaProducer<?, ?> producer = new KafkaProducer<>(getKafkaClientConfiguration());
        consumer.metrics()
                .forEach(
                        (name, metric) ->
                                metricWrappers.add(new KafkaMetricMutableWrapper(metric)));
        producer.metrics()
                .forEach(
                        (name, metric) ->
                                metricWrappers.add(new KafkaMetricMutableWrapper(metric)));

        // Ensure that all values are accessible and return valid double values
        metricWrappers.forEach(KafkaMetricMutableWrapper::getValue);
    }

    private static Properties getKafkaClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        standardProps.put("group.id", UUID.randomUUID().toString());
        standardProps.put("enable.auto.commit", false);
        standardProps.put("key.deserializer", ByteArrayDeserializer.class.getName());
        standardProps.put("value.deserializer", ByteArrayDeserializer.class.getName());
        standardProps.put("key.serializer", ByteArraySerializer.class.getName());
        standardProps.put("value.serializer", ByteArraySerializer.class.getName());
        standardProps.put("auto.offset.reset", "earliest");
        standardProps.put("max.partition.fetch.bytes", 256);
        return standardProps;
    }
}
