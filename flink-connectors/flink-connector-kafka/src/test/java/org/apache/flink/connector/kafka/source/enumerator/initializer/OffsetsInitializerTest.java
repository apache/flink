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

import org.apache.flink.connector.kafka.source.KafkaSourceTestEnv;
import org.apache.flink.connector.kafka.source.enumerator.KafkaSourceEnumerator;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link OffsetsInitializer}. */
public class OffsetsInitializerTest {
    private static final String TOPIC = "topic";
    private static KafkaSourceEnumerator.PartitionOffsetsRetrieverImpl retriever;

    @BeforeClass
    public static void setup() throws Throwable {
        KafkaSourceTestEnv.setup();
        KafkaSourceTestEnv.setupTopic(TOPIC, true, true);
        retriever =
                new KafkaSourceEnumerator.PartitionOffsetsRetrieverImpl(
                        KafkaSourceTestEnv.getConsumer(),
                        KafkaSourceTestEnv.getAdminClient(),
                        KafkaSourceTestEnv.GROUP_ID);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        retriever.close();
        KafkaSourceTestEnv.tearDown();
    }

    @Test
    public void testEarliestOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.earliest();
        List<TopicPartition> partitions = KafkaSourceTestEnv.getPartitionsForTopic(TOPIC);
        Map<TopicPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        assertEquals(partitions.size(), offsets.size());
        assertTrue(offsets.keySet().containsAll(partitions));
        for (long offset : offsets.values()) {
            Assert.assertEquals(KafkaPartitionSplit.EARLIEST_OFFSET, offset);
        }
        assertEquals(OffsetResetStrategy.EARLIEST, initializer.getAutoOffsetResetStrategy());
    }

    @Test
    public void testLatestOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.latest();
        List<TopicPartition> partitions = KafkaSourceTestEnv.getPartitionsForTopic(TOPIC);
        Map<TopicPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        assertEquals(partitions.size(), offsets.size());
        assertTrue(offsets.keySet().containsAll(partitions));
        for (long offset : offsets.values()) {
            assertEquals(KafkaPartitionSplit.LATEST_OFFSET, offset);
        }
        assertEquals(OffsetResetStrategy.LATEST, initializer.getAutoOffsetResetStrategy());
    }

    @Test
    public void testCommittedGroupOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.committedOffsets();
        List<TopicPartition> partitions = KafkaSourceTestEnv.getPartitionsForTopic(TOPIC);
        Map<TopicPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        assertEquals(partitions.size(), offsets.size());
        offsets.forEach(
                (tp, offset) -> assertEquals(KafkaPartitionSplit.COMMITTED_OFFSET, (long) offset));
        assertEquals(OffsetResetStrategy.NONE, initializer.getAutoOffsetResetStrategy());
    }

    @Test
    public void testTimestampOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.timestamp(2001);
        List<TopicPartition> partitions = KafkaSourceTestEnv.getPartitionsForTopic(TOPIC);
        Map<TopicPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        offsets.forEach(
                (tp, offset) -> {
                    long expectedOffset = tp.partition() > 2 ? tp.partition() : 3L;
                    assertEquals(expectedOffset, (long) offset);
                });
        assertEquals(OffsetResetStrategy.NONE, initializer.getAutoOffsetResetStrategy());
    }

    @Test
    public void testSpecificOffsetsInitializer() {
        Map<TopicPartition, Long> specifiedOffsets = new HashMap<>();
        List<TopicPartition> partitions = KafkaSourceTestEnv.getPartitionsForTopic(TOPIC);
        Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                KafkaSourceTestEnv.getCommittedOffsets(partitions);
        committedOffsets.forEach((tp, oam) -> specifiedOffsets.put(tp, oam.offset()));
        // Remove the specified offsets for partition 0.
        TopicPartition missingPartition = new TopicPartition(TOPIC, 0);
        specifiedOffsets.remove(missingPartition);
        OffsetsInitializer initializer = OffsetsInitializer.offsets(specifiedOffsets);

        assertEquals(OffsetResetStrategy.EARLIEST, initializer.getAutoOffsetResetStrategy());

        Map<TopicPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        for (TopicPartition tp : partitions) {
            Long offset = offsets.get(tp);
            long expectedOffset =
                    tp.equals(missingPartition) ? 0L : committedOffsets.get(tp).offset();
            assertEquals(
                    String.format("%s has incorrect offset.", tp), expectedOffset, (long) offset);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testSpecifiedOffsetsInitializerWithoutOffsetResetStrategy() {
        OffsetsInitializer initializer =
                OffsetsInitializer.offsets(Collections.emptyMap(), OffsetResetStrategy.NONE);
        initializer.getPartitionOffsets(KafkaSourceTestEnv.getPartitionsForTopic(TOPIC), retriever);
    }
}
