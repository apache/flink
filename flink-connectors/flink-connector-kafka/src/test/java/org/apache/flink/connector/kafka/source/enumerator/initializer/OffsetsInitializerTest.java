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
import org.apache.flink.connector.kafka.testutils.KafkaSourceTestEnv;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link OffsetsInitializer}. */
public class OffsetsInitializerTest {
    private static final String TOPIC = "topic";
    private static final String TOPIC2 = "topic2";
    private static KafkaSourceEnumerator.PartitionOffsetsRetrieverImpl retriever;

    @BeforeClass
    public static void setup() throws Throwable {
        KafkaSourceTestEnv.setup();
        KafkaSourceTestEnv.setupTopic(TOPIC, true, true, KafkaSourceTestEnv::getRecordsForTopic);
        KafkaSourceTestEnv.setupTopic(TOPIC2, false, false, KafkaSourceTestEnv::getRecordsForTopic);
        retriever =
                new KafkaSourceEnumerator.PartitionOffsetsRetrieverImpl(
                        KafkaSourceTestEnv.getAdminClient(), KafkaSourceTestEnv.GROUP_ID);
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
        assertThat(offsets).hasSameSizeAs(partitions);
        assertThat(offsets.keySet()).containsAll(partitions);
        for (long offset : offsets.values()) {
            assertThat(offset).isEqualTo(KafkaPartitionSplit.EARLIEST_OFFSET);
        }
        assertThat(initializer.getAutoOffsetResetStrategy())
                .isEqualTo(OffsetResetStrategy.EARLIEST);
    }

    @Test
    public void testLatestOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.latest();
        List<TopicPartition> partitions = KafkaSourceTestEnv.getPartitionsForTopic(TOPIC);
        Map<TopicPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        assertThat(offsets).hasSameSizeAs(partitions);
        assertThat(offsets.keySet()).containsAll(partitions);
        for (long offset : offsets.values()) {
            assertThat(offset).isEqualTo(KafkaPartitionSplit.LATEST_OFFSET);
        }
        assertThat(initializer.getAutoOffsetResetStrategy()).isEqualTo(OffsetResetStrategy.LATEST);
    }

    @Test
    public void testCommittedGroupOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.committedOffsets();
        List<TopicPartition> partitions = KafkaSourceTestEnv.getPartitionsForTopic(TOPIC);
        Map<TopicPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        assertThat(offsets).hasSameSizeAs(partitions);
        offsets.forEach(
                (tp, offset) ->
                        assertThat((long) offset).isEqualTo(KafkaPartitionSplit.COMMITTED_OFFSET));
        assertThat(initializer.getAutoOffsetResetStrategy()).isEqualTo(OffsetResetStrategy.NONE);
    }

    @Test
    public void testTimestampOffsetsInitializer() {
        OffsetsInitializer initializer = OffsetsInitializer.timestamp(2001);
        List<TopicPartition> partitions = KafkaSourceTestEnv.getPartitionsForTopic(TOPIC);
        Map<TopicPartition, Long> offsets = initializer.getPartitionOffsets(partitions, retriever);
        offsets.forEach(
                (tp, offset) -> {
                    long expectedOffset = tp.partition() > 2 ? tp.partition() : 3L;
                    assertThat((long) offset).isEqualTo(expectedOffset);
                });
        assertThat(initializer.getAutoOffsetResetStrategy()).isEqualTo(OffsetResetStrategy.NONE);
    }

    @Test
    public void testSpecificOffsetsInitializer() {
        Map<TopicPartition, Long> specifiedOffsets = new HashMap<>();
        List<TopicPartition> partitions = KafkaSourceTestEnv.getPartitionsForTopic(TOPIC);
        Map<TopicPartition, OffsetAndMetadata> committedOffsets =
                KafkaSourceTestEnv.getCommittedOffsets(partitions);
        partitions.forEach(tp -> specifiedOffsets.put(tp, (long) tp.partition()));
        // Remove the specified offsets for partition 0.
        TopicPartition partitionSetToCommitted = new TopicPartition(TOPIC, 0);
        specifiedOffsets.remove(partitionSetToCommitted);
        OffsetsInitializer initializer = OffsetsInitializer.offsets(specifiedOffsets);

        assertThat(initializer.getAutoOffsetResetStrategy())
                .isEqualTo(OffsetResetStrategy.EARLIEST);
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
            assertThat((long) offset)
                    .as(String.format("%s has incorrect offset.", tp))
                    .isEqualTo(expectedOffset);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testSpecifiedOffsetsInitializerWithoutOffsetResetStrategy() {
        OffsetsInitializer initializer =
                OffsetsInitializer.offsets(Collections.emptyMap(), OffsetResetStrategy.NONE);
        initializer.getPartitionOffsets(KafkaSourceTestEnv.getPartitionsForTopic(TOPIC), retriever);
    }
}
