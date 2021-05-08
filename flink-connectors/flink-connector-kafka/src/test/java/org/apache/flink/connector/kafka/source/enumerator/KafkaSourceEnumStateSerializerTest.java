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

package org.apache.flink.connector.kafka.source.enumerator;

import org.apache.flink.connector.base.source.utils.SerdeUtils;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitSerializer;

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/** Test for {@link KafkaSourceEnumStateSerializer}. */
public class KafkaSourceEnumStateSerializerTest {

    private static final int NUM_READERS = 10;
    private static final String TOPIC_PREFIX = "topic-";
    private static final int NUM_PARTITIONS_PER_TOPIC = 10;
    private static final long STARTING_OFFSET = KafkaPartitionSplit.EARLIEST_OFFSET;

    @Test
    public void testEnumStateSerde() throws IOException {
        final KafkaSourceEnumState state = new KafkaSourceEnumState(constructTopicPartitions());
        final KafkaSourceEnumStateSerializer serializer = new KafkaSourceEnumStateSerializer();

        final byte[] bytes = serializer.serialize(state);

        final KafkaSourceEnumState restoredState =
                serializer.deserialize(serializer.getVersion(), bytes);

        assertEquals(state.assignedPartitions(), restoredState.assignedPartitions());
    }

    @Test
    public void testBackwardCompatibility() throws IOException {

        final Set<TopicPartition> topicPartitions = constructTopicPartitions();
        final Map<Integer, Set<KafkaPartitionSplit>> splitAssignments =
                toSplitAssignments(topicPartitions);

        // Create bytes in the way of KafkaEnumStateSerializer version 0 doing serialization
        final byte[] bytes =
                SerdeUtils.serializeSplitAssignments(
                        splitAssignments, new KafkaPartitionSplitSerializer());

        // Deserialize above bytes with KafkaEnumStateSerializer version 1 to check backward
        // compatibility
        final KafkaSourceEnumState kafkaSourceEnumState =
                new KafkaSourceEnumStateSerializer().deserialize(0, bytes);

        assertEquals(topicPartitions, kafkaSourceEnumState.assignedPartitions());
    }

    private Set<TopicPartition> constructTopicPartitions() {
        // Create topic partitions for readers.
        // Reader i will be assigned with NUM_PARTITIONS_PER_TOPIC splits, with topic name
        // "topic-{i}" and
        // NUM_PARTITIONS_PER_TOPIC partitions.
        // Totally NUM_READERS * NUM_PARTITIONS_PER_TOPIC partitions will be created.
        Set<TopicPartition> topicPartitions = new HashSet<>();
        for (int readerId = 0; readerId < NUM_READERS; readerId++) {
            for (int partition = 0; partition < NUM_PARTITIONS_PER_TOPIC; partition++) {
                topicPartitions.add(new TopicPartition(TOPIC_PREFIX + readerId, partition));
            }
        }
        return topicPartitions;
    }

    private Map<Integer, Set<KafkaPartitionSplit>> toSplitAssignments(
            Collection<TopicPartition> topicPartitions) {
        // Assign splits to readers according to topic name. For example, topic "topic-5" will be
        // assigned to reader with ID=5
        Map<Integer, Set<KafkaPartitionSplit>> splitAssignments = new HashMap<>();
        topicPartitions.forEach(
                (tp) ->
                        splitAssignments
                                .computeIfAbsent(
                                        Integer.valueOf(
                                                tp.topic().substring(TOPIC_PREFIX.length())),
                                        HashSet::new)
                                .add(new KafkaPartitionSplit(tp, STARTING_OFFSET)));
        return splitAssignments;
    }
}
