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

package org.apache.flink.connector.kafka.source.enumerator.initializer;

import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.testutils.annotations.Kafka;
import org.apache.flink.connector.kafka.testutils.annotations.KafkaKit;
import org.apache.flink.connector.kafka.testutils.annotations.Topic;
import org.apache.flink.connector.kafka.testutils.extension.KafkaClientKit;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.flink.connector.kafka.testutils.KafkaSourceTestRecordGenerator.KEY_SERIALIZER;
import static org.apache.flink.connector.kafka.testutils.KafkaSourceTestRecordGenerator.VALUE_SERIALIZER;
import static org.apache.flink.connector.kafka.testutils.KafkaSourceTestRecordGenerator.getRecordsForTopic;
import static org.apache.flink.connector.kafka.testutils.extension.KafkaClientKit.DEFAULT_NUM_PARTITIONS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link OffsetsInitializer}. */
@ExtendWith(TestLoggerExtension.class)
@Kafka
class OffsetsInitializerTest {

    @KafkaKit static KafkaClientKit kafkaClientKit;

    @Topic private static final String TOPIC = "topic";
    @Topic private static final String TOPIC2 = "topic2";

    private static final Function<TopicPartition, Long> EARLIEST_OFFSETS_SETTER =
            (tp) -> (long) (tp.partition());
    private static final Function<TopicPartition, Long> COMMITTED_OFFSETS_SETTER =
            (tp) -> (long) (tp.partition() + 2);
    private static final String GROUP_ID = "offsets-initializer-test";
    private static KafkaSourceEnumerator.PartitionOffsetsRetrieverImpl retriever;

    @BeforeAll
    static void setup() throws Throwable {
        kafkaClientKit.produceToKafka(
                getRecordsForTopic(TOPIC, DEFAULT_NUM_PARTITIONS, true),
                KEY_SERIALIZER,
                VALUE_SERIALIZER);
        kafkaClientKit.setEarliestOffsets(TOPIC, EARLIEST_OFFSETS_SETTER);
        kafkaClientKit.setCommittedOffsets(TOPIC, GROUP_ID, COMMITTED_OFFSETS_SETTER);

        kafkaClientKit.produceToKafka(
                getRecordsForTopic(TOPIC2, DEFAULT_NUM_PARTITIONS, true),
                KEY_SERIALIZER,
                VALUE_SERIALIZER);

        retriever =
                new KafkaSourceEnumerator.PartitionOffsetsRetrieverImpl(
                        kafkaClientKit.getAdminClient(), "partition-offsets-retriever");
    }

    @AfterAll
    static void tearDown() throws Exception {
        retriever.close();
    }

    @Test
    public void testEarliestOffsetsInitializer() throws Exception {
        OffsetsInitializer initializer = OffsetsInitializer.earliest();
        Set<TopicPartition> partitions = kafkaClientKit.getPartitionsForTopics(TOPIC);
        Map<TopicPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        assertEquals(partitions.size(), offsets.size());
        assertTrue(offsets.keySet().containsAll(partitions));
        for (long offset : offsets.values()) {
            Assert.assertEquals(KafkaPartitionSplit.EARLIEST_OFFSET, offset);
        }
        assertEquals(OffsetResetStrategy.EARLIEST, initializer.getAutoOffsetResetStrategy());
    }

    @Test
    public void testLatestOffsetsInitializer() throws Exception {
        OffsetsInitializer initializer = OffsetsInitializer.latest();
        Set<TopicPartition> partitions = kafkaClientKit.getPartitionsForTopics(TOPIC);
        Map<TopicPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        assertEquals(partitions.size(), offsets.size());
        assertTrue(offsets.keySet().containsAll(partitions));
        for (long offset : offsets.values()) {
            assertEquals(KafkaPartitionSplit.LATEST_OFFSET, offset);
        }
        assertEquals(OffsetResetStrategy.LATEST, initializer.getAutoOffsetResetStrategy());
    }

    @Test
    public void testCommittedGroupOffsetsInitializer() throws Exception {
        OffsetsInitializer initializer = OffsetsInitializer.committedOffsets();
        Set<TopicPartition> partitions = kafkaClientKit.getPartitionsForTopics(TOPIC);
        Map<TopicPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        assertEquals(partitions.size(), offsets.size());
        offsets.forEach(
                (tp, offset) -> assertEquals(KafkaPartitionSplit.COMMITTED_OFFSET, (long) offset));
        assertEquals(OffsetResetStrategy.NONE, initializer.getAutoOffsetResetStrategy());
    }

    @Test
    public void testTimestampOffsetsInitializer() throws Exception {
        OffsetsInitializer initializer = OffsetsInitializer.timestamp(2001);
        Set<TopicPartition> partitions = kafkaClientKit.getPartitionsForTopics(TOPIC);
        Map<TopicPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        offsets.forEach(
                (tp, offset) -> {
                    long expectedOffset = tp.partition() > 2 ? tp.partition() : 3L;
                    assertEquals(expectedOffset, (long) offset);
                });
        assertEquals(OffsetResetStrategy.NONE, initializer.getAutoOffsetResetStrategy());
    }

    @Test
    public void testSpecificOffsetsInitializer() throws Exception {
        Map<TopicPartition, Long> specifiedOffsets = new HashMap<>();
        Map<TopicPartition, OffsetAndMetadata> committedOffsets = new HashMap<>();
        Set<TopicPartition> partitions = kafkaClientKit.getPartitionsForTopics(TOPIC);
        partitions.forEach(
                tp -> {
                    specifiedOffsets.put(tp, (long) tp.partition());
                    committedOffsets.put(
                            tp, new OffsetAndMetadata(COMMITTED_OFFSETS_SETTER.apply(tp)));
                });
        // Remove the specified offsets for partition 0.
        TopicPartition partitionSetToCommitted = new TopicPartition(TOPIC, 0);
        specifiedOffsets.remove(partitionSetToCommitted);
        OffsetsInitializer initializer = OffsetsInitializer.offsets(specifiedOffsets);

        assertEquals(OffsetResetStrategy.EARLIEST, initializer.getAutoOffsetResetStrategy());
        // The partition without committed offset should fallback to offset reset strategy.
        TopicPartition partitionSetToEarliest = new TopicPartition(TOPIC2, 0);
        partitions.add(partitionSetToEarliest);

        Map<TopicPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        for (TopicPartition tp : partitions) {
            Long offset = offsets.get(tp);
            long expectedOffset;
            if (tp.equals(partitionSetToCommitted)) {
                expectedOffset = committedOffsets.get(tp).offset();
            } else if (tp.equals(partitionSetToEarliest)) {
                expectedOffset = 0L;
            } else {
                expectedOffset = specifiedOffsets.get(tp);
            }
            assertEquals(
                    String.format("%s has incorrect offset.", tp), expectedOffset, (long) offset);
        }
    }

    @Test
    void testSpecifiedOffsetsInitializerWithoutOffsetResetStrategy() {
        OffsetsInitializer initializer =
                OffsetsInitializer.offsets(Collections.emptyMap(), OffsetResetStrategy.NONE);
        Assertions.assertThrows(
                IllegalStateException.class,
                () ->
                        initializer.getPartitionOffsets(
                                kafkaClientKit.getPartitionsForTopics(TOPIC), retriever));
    }
}
