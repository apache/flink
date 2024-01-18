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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;

import org.apache.flink.shaded.guava32.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava32.com.google.common.collect.Sets;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TaskBalancedPreferredSlotSharingStrategy}. */
class TaskBalancedPreferredSlotSharingStrategyTest extends AbstractSlotSharingStrategyTest {

    @Override
    protected SlotSharingStrategy getSlotSharingStrategy(
            SchedulingTopology topology,
            Set<SlotSharingGroup> slotSharingGroups,
            Set<CoLocationGroup> coLocationGroups) {
        return new TaskBalancedPreferredSlotSharingStrategy(
                topology, slotSharingGroups, coLocationGroups);
    }

    @Test
    void testVerticesInDifferentSlotSharingGroups() {
        List<Tuple2<JobVertexID, List<TestingSchedulingExecutionVertex>>> jobVertexInfos =
                new ArrayList<>();
        SlotSharingGroup slotSharingGroup1 = new SlotSharingGroup();
        SlotSharingGroup slotSharingGroup2 = new SlotSharingGroup();
        List<TestingJobVertexInfo> testingJobVertexInfos =
                Lists.newArrayList(
                        new TestingJobVertexInfo(1, slotSharingGroup1, null),
                        new TestingJobVertexInfo(2, slotSharingGroup1, null),
                        new TestingJobVertexInfo(3, slotSharingGroup1, null),
                        new TestingJobVertexInfo(1, slotSharingGroup2, null),
                        new TestingJobVertexInfo(2, slotSharingGroup2, null),
                        new TestingJobVertexInfo(2, slotSharingGroup2, null));

        setupCase(testingJobVertexInfos, jobVertexInfos);
        slotSharingGroups = Sets.newHashSet(slotSharingGroup1, slotSharingGroup2);

        final SlotSharingStrategy strategy =
                getSlotSharingStrategy(topology, slotSharingGroups, coLocationGroups);
        assertThat(strategy.getExecutionSlotSharingGroups()).hasSize(5);
        checkBalanceAtSlotsLevelWithoutCoLocation(strategy);

        List<TestingSchedulingExecutionVertex> executionVertices4 = jobVertexInfos.get(4).f1;
        List<TestingSchedulingExecutionVertex> executionVertices5 = jobVertexInfos.get(5).f1;
        assertThat(executionVertices4).hasSameSizeAs(executionVertices5);
        // Check for JVs whose parallelism is the max in the same slot sharing group.
        for (int i = 0; i < executionVertices4.size(); i++) {
            TestingSchedulingExecutionVertex executionVertex4 = executionVertices4.get(i);
            assertThat(strategy.getExecutionSlotSharingGroup(executionVertex4.getId()))
                    .isEqualTo(
                            strategy.getExecutionSlotSharingGroup(
                                    executionVertices5.get(i).getId()));
        }
    }

    private void checkBalanceAtSlotsLevelWithoutCoLocation(SlotSharingStrategy strategy) {
        strategy.getExecutionSlotSharingGroups().stream()
                .collect(Collectors.groupingBy(ExecutionSlotSharingGroup::getSlotSharingGroup))
                .forEach(
                        (slotSharingGroup, executionSlotSharingGroups) -> {
                            Optional<Integer> max =
                                    executionSlotSharingGroups.stream()
                                            .map(
                                                    executionSlotSharingGroup ->
                                                            executionSlotSharingGroup
                                                                    .getExecutionVertexIds()
                                                                    .size())
                                            .max(Comparator.comparing(i -> i));
                            Optional<Integer> min =
                                    executionSlotSharingGroups.stream()
                                            .map(
                                                    executionSlotSharingGroup ->
                                                            executionSlotSharingGroup
                                                                    .getExecutionVertexIds()
                                                                    .size())
                                            .min(Comparator.comparing(i -> i));
                            assertThat(max.get()).isCloseTo(min.get(), Offset.offset(1));
                        });
    }
}
