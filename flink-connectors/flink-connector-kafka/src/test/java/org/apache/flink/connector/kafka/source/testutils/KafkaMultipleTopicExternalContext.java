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

package org.apache.flink.connector.kafka.source.testutils;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testcontainers.containers.KafkaContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

/**
 * Kafka external context that will create multiple topics with only one partitions as source
 * splits.
 */
public class KafkaMultipleTopicExternalContext extends KafkaSingleTopicExternalContext {

    private int numTopics = 0;

    private final String topicPattern;

    private final Map<String, SourceSplitDataWriter<String>> topicNameToSplitWriters =
            new HashMap<>();

    public KafkaMultipleTopicExternalContext(String bootstrapServers) {
        super(bootstrapServers);
        this.topicPattern =
                "kafka-multiple-topic-[0-9]+-"
                        + ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    }

    @Override
    public SourceSplitDataWriter<String> createSourceSplitDataWriter() {
        String topicName = getTopicName();
        createTopic(topicName, 1, (short) 1);
        final KafkaPartitionDataWriter splitWriter =
                new KafkaPartitionDataWriter(
                        getKafkaProducerProperties(numTopics), new TopicPartition(topicName, 0));
        topicNameToSplitWriters.put(topicName, splitWriter);
        numTopics++;
        return splitWriter;
    }

    @Override
    public Source<String, ?, ?> createSource(Boundedness boundedness) {
        KafkaSourceBuilder<String> builder = KafkaSource.builder();

        if (boundedness == Boundedness.BOUNDED) {
            builder = builder.setBounded(OffsetsInitializer.latest());
        }

        return builder.setGroupId("flink-kafka-multiple-topic-test")
                .setBootstrapServers(bootstrapServers)
                .setTopicPattern(Pattern.compile(topicPattern))
                .setDeserializer(
                        KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();
    }

    @Override
    public void close() {
        topicNameToSplitWriters.forEach(
                (topicName, splitWriter) -> {
                    try {
                        splitWriter.close();
                        deleteTopic(topicName);
                    } catch (Exception e) {
                        kafkaAdminClient.close();
                        throw new RuntimeException("Cannot close split writer", e);
                    }
                });
        topicNameToSplitWriters.clear();
        kafkaAdminClient.close();
    }

    private String getTopicName() {
        return topicPattern.replace("[0-9]+", String.valueOf(numTopics));
    }

    @Override
    public String toString() {
        return "Multiple-topics Kafka";
    }

    /** Factory of {@link KafkaSingleTopicExternalContext}. */
    public static class Factory extends KafkaSingleTopicExternalContext.Factory {

        public Factory(KafkaContainer kafkaContainer) {
            super(kafkaContainer);
        }

        @Override
        public ExternalContext<String> createExternalContext() {
            return new KafkaMultipleTopicExternalContext(getBootstrapServer());
        }
    }
}
