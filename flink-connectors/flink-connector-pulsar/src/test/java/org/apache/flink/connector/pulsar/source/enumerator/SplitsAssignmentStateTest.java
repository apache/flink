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

package org.apache.flink.connector.pulsar.source.enumerator;

import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.MAX_RANGE;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange.createFullRange;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.map;

/** Unit tests for {@link SplitsAssignmentState}. */
class SplitsAssignmentStateTest {

    private final Set<TopicPartition> partitions =
            Sets.newHashSet(
                    new TopicPartition("some-topic", 1, new TopicRange(1, 30)),
                    new TopicPartition("some-topic", 2, new TopicRange(31, 60)),
                    new TopicPartition("some-topic", 3, new TopicRange(61, MAX_RANGE)),
                    new TopicPartition(randomAlphabetic(10), -1, createFullRange()));

    @Test
    void assignSplitsForSharedSubscription() {
        SplitsAssignmentState state1 =
                new SplitsAssignmentState(
                        StopCursor.defaultStopCursor(), createConfig(SubscriptionType.Shared));
        state1.appendTopicPartitions(partitions);
        Optional<SplitsAssignment<PulsarPartitionSplit>> assignment1 =
                state1.assignSplits(Lists.newArrayList(0, 1, 2, 3, 4));

        assertThat(assignment1)
                .isPresent()
                .get()
                .extracting(SplitsAssignment::assignment)
                .asInstanceOf(map(Integer.class, List.class))
                .hasSize(5)
                .allSatisfy((idx, list) -> assertThat(list).hasSize(4));

        Optional<SplitsAssignment<PulsarPartitionSplit>> assignment2 =
                state1.assignSplits(Lists.newArrayList(0, 1, 2, 3, 4));
        assertThat(assignment2).isNotPresent();

        // Reassign reader 3.
        state1.putSplitsBackToPendingList(assignment1.get().assignment().get(3), 3);
        Optional<SplitsAssignment<PulsarPartitionSplit>> assignment3 =
                state1.assignSplits(Lists.newArrayList(0, 1, 2, 4));
        assertThat(assignment3).isNotPresent();

        Optional<SplitsAssignment<PulsarPartitionSplit>> assignment4 =
                state1.assignSplits(singletonList(3));
        assertThat(assignment4)
                .isPresent()
                .get()
                .extracting(SplitsAssignment::assignment)
                .asInstanceOf(map(Integer.class, List.class))
                .hasSize(1);
    }

    @Test
    void assignSplitsForExclusiveSubscription() {
        SplitsAssignmentState state1 =
                new SplitsAssignmentState(
                        StopCursor.defaultStopCursor(), createConfig(SubscriptionType.Exclusive));
        state1.appendTopicPartitions(partitions);
        Optional<SplitsAssignment<PulsarPartitionSplit>> assignment1 =
                state1.assignSplits(Lists.newArrayList(0, 1, 2, 3, 4));

        assertThat(assignment1).isPresent();
        assertThat(assignment1.get().assignment())
                .hasSize(4)
                .allSatisfy((idx, list) -> assertThat(list).hasSize(1));

        Optional<SplitsAssignment<PulsarPartitionSplit>> assignment2 =
                state1.assignSplits(Lists.newArrayList(0, 1, 2, 3, 4));
        assertThat(assignment2).isNotPresent();
    }

    private SourceConfiguration createConfig(SubscriptionType type) {
        Configuration configuration = new Configuration();
        configuration.set(PULSAR_SUBSCRIPTION_TYPE, type);

        return new SourceConfiguration(configuration);
    }
}
