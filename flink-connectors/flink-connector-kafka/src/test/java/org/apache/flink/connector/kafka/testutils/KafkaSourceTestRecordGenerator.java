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

package org.apache.flink.connector.kafka.testutils;

import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Util class for generating records in Kafka source tests. */
public class KafkaSourceTestRecordGenerator {

    public static final int NUM_RECORDS_PER_PARTITION = 10;
    public static final Class<StringSerializer> KEY_SERIALIZER = StringSerializer.class;
    public static final Class<IntegerSerializer> VALUE_SERIALIZER = IntegerSerializer.class;
    public static final Class<StringDeserializer> KEY_DESERIALIZER = StringDeserializer.class;
    public static final Class<IntegerDeserializer> VALUE_DESERIALIZER = IntegerDeserializer.class;

    /**
     * Get a mapping of subtask ID to its owning {@link KafkaPartitionSplit}s.
     *
     * @param topicPartitions partitions to assign
     * @param numSubtasks number of subtasks
     */
    public static Map<Integer, Map<String, KafkaPartitionSplit>> getSplitsByOwners(
            final Collection<TopicPartition> topicPartitions, final int numSubtasks) {
        final Map<Integer, Map<String, KafkaPartitionSplit>> splitsByOwners = new HashMap<>();
        topicPartitions.forEach(
                tp -> {
                    int ownerReader = Math.abs(tp.hashCode()) % numSubtasks;
                    KafkaPartitionSplit split =
                            new KafkaPartitionSplit(tp, tp.partition(), NUM_RECORDS_PER_PARTITION);
                    splitsByOwners
                            .computeIfAbsent(ownerReader, r -> new HashMap<>())
                            .put(KafkaPartitionSplit.toSplitId(tp), split);
                });

        return splitsByOwners;
    }

    /**
     * For a given partition {@code TOPIC-PARTITION} the {@code i}-th records looks like following.
     *
     * <pre>{@code
     * topic: TOPIC
     * partition: PARTITION
     * timestamp: 1000 * PARTITION, or null if timestamp is disabled
     * key: TOPIC-PARTITION
     * value: i
     * }</pre>
     */
    public static List<ProducerRecord<String, Integer>> getRecordsForPartition(
            TopicPartition tp, boolean withTimestamp) {
        List<ProducerRecord<String, Integer>> records = new ArrayList<>();
        for (int i = 0; i < NUM_RECORDS_PER_PARTITION; i++) {
            records.add(
                    new ProducerRecord<>(
                            tp.topic(),
                            tp.partition(),
                            withTimestamp ? i * 1000L : null,
                            tp.toString(),
                            i));
        }
        return records;
    }

    public static List<ProducerRecord<String, Integer>> getRecordsForTopic(
            String topic, int numPartitions, boolean withTimestamp) {
        List<ProducerRecord<String, Integer>> records = new ArrayList<>();
        for (int partition = 0; partition < numPartitions; partition++) {
            records.addAll(
                    getRecordsForPartition(new TopicPartition(topic, partition), withTimestamp));
        }
        return records;
    }
}
