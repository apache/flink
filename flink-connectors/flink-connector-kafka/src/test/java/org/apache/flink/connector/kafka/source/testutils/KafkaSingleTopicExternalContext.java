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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A Kafka external context that will create only one topic and use partitions in that topic as
 * source splits.
 */
public class KafkaSingleTopicExternalContext implements ExternalContext<String> {

    private static final Logger LOG =
            LoggerFactory.getLogger(KafkaSingleTopicExternalContext.class);

    private static final String TOPIC_NAME_PREFIX = "kafka-single-topic";
    private static final int DEFAULT_TIMEOUT = 30;
    private static final int NUM_RECORDS_UPPER_BOUND = 500;
    private static final int NUM_RECORDS_LOWER_BOUND = 100;

    protected String bootstrapServers;
    private final String topicName;

    private final Map<Integer, SourceSplitDataWriter<String>> partitionToSplitWriter =
            new HashMap<>();

    private int numSplits = 0;

    protected final AdminClient kafkaAdminClient;

    public KafkaSingleTopicExternalContext(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        this.topicName =
                TOPIC_NAME_PREFIX + "-" + ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        kafkaAdminClient = createAdminClient();
    }

    protected void createTopic(String topicName, int numPartitions, short replicationFactor) {
        LOG.debug(
                "Creating new Kafka topic {} with {} partitions and {} replicas",
                topicName,
                numPartitions,
                replicationFactor);
        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
        try {
            kafkaAdminClient
                    .createTopics(Collections.singletonList(newTopic))
                    .all()
                    .get(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(String.format("Cannot create topic '%s'", topicName), e);
        }
    }

    protected void deleteTopic(String topicName) {
        LOG.debug("Deleting Kafka topic {}", topicName);
        try {
            kafkaAdminClient
                    .deleteTopics(Collections.singletonList(topicName))
                    .all()
                    .get(DEFAULT_TIMEOUT, TimeUnit.SECONDS);
        } catch (Exception e) {
            if (ExceptionUtils.getRootCause(e) instanceof UnknownTopicOrPartitionException) {
                throw new RuntimeException(String.format("Cannot delete topic '%s'", topicName), e);
            }
        }
    }

    private AdminClient createAdminClient() {
        Properties config = new Properties();
        config.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return AdminClient.create(config);
    }

    @Override
    public Source<String, ?, ?> createSource(Boundedness boundedness) {
        KafkaSourceBuilder<String> builder = KafkaSource.builder();

        if (boundedness == Boundedness.BOUNDED) {
            builder = builder.setBounded(OffsetsInitializer.latest());
        }
        return builder.setGroupId("flink-kafka-test")
                .setDeserializer(
                        KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .setTopics(topicName)
                .setBootstrapServers(bootstrapServers)
                .build();
    }

    @Override
    public SourceSplitDataWriter<String> createSourceSplitDataWriter() {
        if (numSplits == 0) {
            createTopic(topicName, 1, (short) 1);
            numSplits++;
        } else {
            LOG.debug("Creating new partition for topic {}", topicName);
            kafkaAdminClient.createPartitions(
                    Collections.singletonMap(topicName, NewPartitions.increaseTo(++numSplits)));
        }
        KafkaPartitionDataWriter splitWriter =
                new KafkaPartitionDataWriter(
                        getKafkaProducerProperties(numSplits - 1),
                        new TopicPartition(topicName, numSplits - 1));
        partitionToSplitWriter.put(numSplits - 1, splitWriter);
        return splitWriter;
    }

    @Override
    public List<String> generateTestData(int splitIndex, long seed) {
        Random random = new Random(seed);
        List<String> randomStringRecords = new ArrayList<>();
        int recordNum =
                random.nextInt(NUM_RECORDS_UPPER_BOUND - NUM_RECORDS_LOWER_BOUND)
                        + NUM_RECORDS_LOWER_BOUND;
        for (int i = 0; i < recordNum; i++) {
            int stringLength = random.nextInt(50) + 1;
            randomStringRecords.add(generateRandomString(splitIndex, stringLength, random));
        }
        return randomStringRecords;
    }

    private String generateRandomString(int splitIndex, int length, Random random) {
        String alphaNumericString =
                "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "abcdefghijklmnopqrstuvwxyz" + "0123456789";
        StringBuilder sb = new StringBuilder().append(splitIndex).append("-");
        for (int i = 0; i < length; ++i) {
            sb.append(alphaNumericString.charAt(random.nextInt(alphaNumericString.length())));
        }
        return sb.toString();
    }

    protected Properties getKafkaProducerProperties(int producerId) {
        Properties kafkaProducerProperties = new Properties();
        kafkaProducerProperties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        kafkaProducerProperties.setProperty(
                ProducerConfig.CLIENT_ID_CONFIG,
                String.join(
                        "-",
                        "flink-kafka-split-writer",
                        Integer.toString(producerId),
                        Long.toString(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))));
        kafkaProducerProperties.setProperty(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProducerProperties.setProperty(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return kafkaProducerProperties;
    }

    @Override
    public void close() {
        deleteTopic(topicName);
        partitionToSplitWriter.forEach(
                (partitionId, splitWriter) -> {
                    try {
                        splitWriter.close();
                    } catch (Exception e) {
                        kafkaAdminClient.close();
                        throw new RuntimeException("Cannot close split writer", e);
                    }
                });
        partitionToSplitWriter.clear();
        kafkaAdminClient.close();
    }

    @Override
    public String toString() {
        return "Single-topic Kafka";
    }

    /** Factory of {@link KafkaSingleTopicExternalContext}. */
    public static class Factory implements ExternalContext.Factory<String> {

        private final KafkaContainer kafkaContainer;

        public Factory(KafkaContainer kafkaContainer) {
            this.kafkaContainer = kafkaContainer;
        }

        protected String getBootstrapServer() {
            final String internalEndpoints =
                    kafkaContainer.getNetworkAliases().stream()
                            .map(host -> String.join(":", host, Integer.toString(9092)))
                            .collect(Collectors.joining(","));
            return String.join(",", kafkaContainer.getBootstrapServers(), internalEndpoints);
        }

        @Override
        public ExternalContext<String> createExternalContext() {
            return new KafkaSingleTopicExternalContext(getBootstrapServer());
        }
    }
}
