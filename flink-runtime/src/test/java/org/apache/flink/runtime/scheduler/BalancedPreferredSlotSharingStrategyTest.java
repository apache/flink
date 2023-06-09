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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;
import org.apache.flink.util.LogLevelExtension;

import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BalancedPreferredSlotSharingStrategy}. */
@ExtendWith(LogLevelExtension.class)
class BalancedPreferredSlotSharingStrategyTest {

    @Test
    void testVerticesInDifferentSlotSharingGroups() {
        TestingSchedulingTopology topology = new TestingSchedulingTopology();
        List<Tuple2<JobVertexID, List<TestingSchedulingExecutionVertex>>> jobVertexInfos =
                new ArrayList<>();
        Set<SlotSharingGroup> slotSharingGroups = new HashSet<>();

        setupCase(topology, jobVertexInfos, slotSharingGroups);

        final SlotSharingStrategy strategy =
                new BalancedPreferredSlotSharingStrategy(
                        topology, slotSharingGroups, Collections.emptySet());

        assertThat(strategy.getExecutionSlotSharingGroups()).hasSize(6);

        // Test for ExecutionSlotSharingGroup level balance.
        Map<SlotSharingGroup, List<ExecutionSlotSharingGroup>> logicalSsgToESSGs =
                strategy.getExecutionSlotSharingGroups().stream()
                        .collect(Collectors.groupingBy(strategy::getSlotSharingGroup));
        logicalSsgToESSGs.forEach(
                (ssg, eSSGs) -> {
                    Optional<Integer> max =
                            eSSGs.stream()
                                    .map(eSsg -> eSsg.getExecutionVertexIds().size())
                                    .max(Comparator.comparing(i -> i));
                    Optional<Integer> min =
                            eSSGs.stream()
                                    .map(eSsg -> eSsg.getExecutionVertexIds().size())
                                    .min(Comparator.comparing(i -> i));
                    assertThat(max.get()).isCloseTo(min.get(), Offset.offset(1));
                });

        // Test same indexed-subTask of JV whose parallelism is the number of the current
        // SlotSharingGroup should be assigned into the same ExecutionSlotSharingGroup
        List<TestingSchedulingExecutionVertex> tSEVsOfJV5 = jobVertexInfos.get(5).f1;
        List<TestingSchedulingExecutionVertex> tSEVsOfJV6 = jobVertexInfos.get(6).f1;
        for (int i = 0; i < tSEVsOfJV5.size(); i++) {
            ExecutionVertexID evIDOfJV5 = tSEVsOfJV5.get(i).getId();
            ExecutionVertexID evIDOfJV6 = tSEVsOfJV6.get(i).getId();
            assertThat(evIDOfJV5.getSubtaskIndex()).isEqualTo(evIDOfJV6.getSubtaskIndex());
            assertThat(strategy.getExecutionSlotSharingGroup(evIDOfJV5))
                    .isEqualTo(strategy.getExecutionSlotSharingGroup(evIDOfJV6));
        }
    }

    @Test
    void testSetSlotSharingGroupResource() {
        TestingSchedulingTopology topology = new TestingSchedulingTopology();
        final JobVertexID jobVertexId1 = new JobVertexID();
        final JobVertexID jobVertexId2 = new JobVertexID();
        final TestingSchedulingExecutionVertex ev11 = topology.newExecutionVertex(jobVertexId1, 0);
        final TestingSchedulingExecutionVertex ev12 = topology.newExecutionVertex(jobVertexId1, 1);
        final TestingSchedulingExecutionVertex ev21 = topology.newExecutionVertex(jobVertexId2, 0);

        final SlotSharingGroup slotSharingGroup1 = new SlotSharingGroup();
        final ResourceProfile resourceProfile1 = ResourceProfile.fromResources(1, 10);
        slotSharingGroup1.addVertexToGroup(jobVertexId1);
        slotSharingGroup1.setResourceProfile(resourceProfile1);

        final SlotSharingGroup slotSharingGroup2 = new SlotSharingGroup();
        final ResourceProfile resourceProfile2 = ResourceProfile.fromResources(2, 20);
        slotSharingGroup2.addVertexToGroup(jobVertexId2);
        slotSharingGroup2.setResourceProfile(resourceProfile2);

        final Set<SlotSharingGroup> slotSharingGroups =
                Sets.newHashSet(slotSharingGroup1, slotSharingGroup2);

        final SlotSharingStrategy strategy =
                new BalancedPreferredSlotSharingStrategy(
                        topology, slotSharingGroups, Collections.emptySet());

        assertThat(strategy.getExecutionSlotSharingGroups()).hasSize(3);
        assertThat(strategy.getExecutionSlotSharingGroup(ev11.getId()).getResourceProfile())
                .isEqualTo(resourceProfile1);
        assertThat(strategy.getExecutionSlotSharingGroup(ev12.getId()).getResourceProfile())
                .isEqualTo(resourceProfile1);
        assertThat(strategy.getExecutionSlotSharingGroup(ev21.getId()).getResourceProfile())
                .isEqualTo(resourceProfile2);
    }

    private void setupCase(
            TestingSchedulingTopology topology,
            List<Tuple2<JobVertexID, List<TestingSchedulingExecutionVertex>>> jobVertexInfos,
            Set<SlotSharingGroup> slotSharingGroups) {
        // Logical SSG1(JV0(p=1),JV1(p=2), JV2(p=4)), SSG2(JV3(p=1),JV4(p=2), JV5(p=4), JV6(p=4))
        int[] parallelism = {1, 2, 3, 1, 1, 3, 3};
        for (int i = 0; i < 7; i++) {
            JobVertexID jobVertexID = new JobVertexID();
            List<TestingSchedulingExecutionVertex> tSEVs = new ArrayList<>();
            for (int subIndex = 0; subIndex < parallelism[i]; subIndex++) {
                tSEVs.add(topology.newExecutionVertex(jobVertexID, subIndex));
            }
            jobVertexInfos.add(Tuple2.of(jobVertexID, tSEVs));
        }

        SlotSharingGroup slotSharingGroup1 = new SlotSharingGroup();
        slotSharingGroup1.addVertexToGroup(jobVertexInfos.get(0).f0);
        slotSharingGroup1.addVertexToGroup(jobVertexInfos.get(1).f0);
        slotSharingGroup1.addVertexToGroup(jobVertexInfos.get(2).f0);

        SlotSharingGroup slotSharingGroup2 = new SlotSharingGroup();
        slotSharingGroup2.addVertexToGroup(jobVertexInfos.get(3).f0);
        slotSharingGroup2.addVertexToGroup(jobVertexInfos.get(4).f0);
        slotSharingGroup2.addVertexToGroup(jobVertexInfos.get(5).f0);
        slotSharingGroup2.addVertexToGroup(jobVertexInfos.get(6).f0);

        slotSharingGroups.add(slotSharingGroup1);
        slotSharingGroups.add(slotSharingGroup2);
    }
}
