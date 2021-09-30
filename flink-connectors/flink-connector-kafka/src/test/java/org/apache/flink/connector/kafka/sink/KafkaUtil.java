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
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Collection of methods to interact with a Kafka cluster. */
public class KafkaUtil {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaUtil.class);
    private static final Duration CONSUMER_POLL_DURATION = Duration.ofSeconds(1);

    private KafkaUtil() {}

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
            Map<Integer, TopicPartition> assignments = getAllPartitions(consumer, topic);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignments.values());
            consumer.assign(assignments.values());
            consumer.seekToBeginning(assignments.values());

            final List<ConsumerRecord<byte[], byte[]>> consumerRecords = new ArrayList<>();
            while (!assignments.isEmpty()) {
                consumer.assign(assignments.values());
                ConsumerRecords<byte[], byte[]> records = consumer.poll(CONSUMER_POLL_DURATION);
                LOG.info("Fetched {} records from topic {}.", records.count(), topic);

                // Remove partitions from polling which have reached its end.
                final Iterator<Map.Entry<Integer, TopicPartition>> assignmentIterator =
                        assignments.entrySet().iterator();
                while (assignmentIterator.hasNext()) {
                    final Map.Entry<Integer, TopicPartition> assignment = assignmentIterator.next();
                    final TopicPartition topicPartition = assignment.getValue();
                    final long position = consumer.position(topicPartition);
                    final long endOffset = endOffsets.get(topicPartition);
                    LOG.info(
                            "Endoffset {} and current position {} for partition {}",
                            endOffset,
                            position,
                            assignment.getKey());
                    if (endOffset - position > 0) {
                        continue;
                    }
                    assignmentIterator.remove();
                }
                for (ConsumerRecord<byte[], byte[]> r : records) {
                    consumerRecords.add(r);
                }
            }
            return consumerRecords;
        }
    }

    private static Map<Integer, TopicPartition> getAllPartitions(
            KafkaConsumer<byte[], byte[]> consumer, String topic) {
        final List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        return partitionInfos.stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .collect(Collectors.toMap(TopicPartition::partition, Function.identity()));
    }
}
