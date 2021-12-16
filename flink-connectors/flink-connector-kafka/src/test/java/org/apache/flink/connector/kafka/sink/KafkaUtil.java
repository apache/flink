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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/** Collection of methods to interact with a Kafka cluster. */
public class KafkaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtil.class);
    private static final Duration CONSUMER_POLL_DURATION = Duration.ofSeconds(1);

    private KafkaUtil() {}

    /**
     * This method helps to set commonly used Kafka configurations and aligns the internal Kafka log
     * levels with the ones used by the capturing logger.
     *
     * @param dockerImageVersion describing the Kafka image
     * @param logger to derive the log level from
     * @return configured Kafka container
     */
    public static KafkaContainer createKafkaContainer(String dockerImageVersion, Logger logger) {
        String logLevel;
        if (logger.isTraceEnabled()) {
            logLevel = "TRACE";
        } else if (logger.isDebugEnabled()) {
            logLevel = "DEBUG";
        } else if (logger.isInfoEnabled()) {
            logLevel = "INFO";
        } else if (logger.isWarnEnabled()) {
            logLevel = "WARN";
        } else if (logger.isErrorEnabled()) {
            logLevel = "ERROR";
        } else {
            logLevel = "OFF";
        }

        return new KafkaContainer(DockerImageName.parse(dockerImageVersion))
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE", "false")
                .withEnv("KAFKA_LOG4J_ROOT_LOGLEVEL", logLevel)
                .withEnv("KAFKA_LOG4J_LOGGERS", "state.change.logger=" + logLevel)
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
                .withEnv("KAFKA_CONFLUENT_SUPPORT_METRICS_ENABLE", "false")
                .withEnv(
                        "KAFKA_TRANSACTION_MAX_TIMEOUT_MS",
                        String.valueOf(Duration.ofHours(2).toMillis()))
                .withEnv("KAFKA_LOG4J_TOOLS_ROOT_LOGLEVEL", logLevel)
                .withLogConsumer(new Slf4jLogConsumer(logger));
    }

    /**
     * Drain all records available from the given topic from the beginning until the current highest
     * offset.
     *
     * <p>This method will fetch the latest offsets for the partitions once and only return records
     * until that point.
     *
     * @param topic to fetch from
     * @param properties used to configure the created {@link KafkaConsumer}
     * @param committed determines the mode {@link ConsumerConfig#ISOLATION_LEVEL_CONFIG} with which
     *     the consumer reads the records.
     * @return all {@link ConsumerRecord} in the topic
     * @throws KafkaException
     */
    public static List<ConsumerRecord<byte[], byte[]>> drainAllRecordsFromTopic(
            String topic, Properties properties, boolean committed) throws KafkaException {
        final Properties consumerConfig = new Properties();
        consumerConfig.putAll(properties);
        consumerConfig.put(
                ConsumerConfig.ISOLATION_LEVEL_CONFIG,
                committed ? "read_committed" : "read_uncommitted");
        return drainAllRecordsFromTopic(topic, consumerConfig);
    }

    /**
     * Drain all records available from the given topic from the beginning until the current highest
     * offset.
     *
     * <p>This method will fetch the latest offsets for the partitions once and only return records
     * until that point.
     *
     * @param topic to fetch from
     * @param properties used to configure the created {@link KafkaConsumer}
     * @return all {@link ConsumerRecord} in the topic
     * @throws KafkaException
     */
    public static List<ConsumerRecord<byte[], byte[]>> drainAllRecordsFromTopic(
            String topic, Properties properties) throws KafkaException {
        final Properties consumerConfig = new Properties();
        consumerConfig.putAll(properties);
        consumerConfig.put("key.deserializer", ByteArrayDeserializer.class.getName());
        consumerConfig.put("value.deserializer", ByteArrayDeserializer.class.getName());
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfig)) {
            Set<TopicPartition> topicPartitions = getAllPartitions(consumer, topic);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(topicPartitions);
            consumer.assign(topicPartitions);
            consumer.seekToBeginning(topicPartitions);

            final List<ConsumerRecord<byte[], byte[]>> consumerRecords = new ArrayList<>();
            while (!topicPartitions.isEmpty()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(CONSUMER_POLL_DURATION);
                LOG.debug("Fetched {} records from topic {}.", records.count(), topic);

                // Remove partitions from polling which have reached its end.
                final List<TopicPartition> finishedPartitions = new ArrayList<>();
                for (final TopicPartition topicPartition : topicPartitions) {
                    final long position = consumer.position(topicPartition);
                    final long endOffset = endOffsets.get(topicPartition);
                    LOG.debug(
                            "Endoffset {} and current position {} for partition {}",
                            endOffset,
                            position,
                            topicPartition.partition());
                    if (endOffset - position > 0) {
                        continue;
                    }
                    finishedPartitions.add(topicPartition);
                }
                if (topicPartitions.removeAll(finishedPartitions)) {
                    consumer.assign(topicPartitions);
                }
                for (ConsumerRecord<byte[], byte[]> r : records) {
                    consumerRecords.add(r);
                }
            }
            return consumerRecords;
        }
    }

    private static Set<TopicPartition> getAllPartitions(
            KafkaConsumer<byte[], byte[]> consumer, String topic) {
        return consumer.partitionsFor(topic).stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .collect(Collectors.toSet());
    }
}
