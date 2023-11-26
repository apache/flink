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
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupImpl;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava32.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava32.com.google.common.collect.Sets;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Abstract test class base for {@link SlotSharingStrategy}. */
abstract class AbstractSlotSharingStrategyTest {

    protected SlotSharingGroup defaultSlotSharingGroup;

    protected TestingSchedulingTopology topology;
    protected Set<SlotSharingGroup> slotSharingGroups;
    protected Set<CoLocationGroup> coLocationGroups;

    @BeforeEach
    void setUp() {
        topology = new TestingSchedulingTopology();
        defaultSlotSharingGroup = new SlotSharingGroup();
        slotSharingGroups = Sets.newHashSet(defaultSlotSharingGroup);
        coLocationGroups = Sets.newHashSet();
    }

    @Test
    void testSetSlotSharingGroupResource() {
        final JobVertexID jobVertexId1 = new JobVertexID();
        final JobVertexID jobVertexId2 = new JobVertexID();
        final TestingSchedulingExecutionVertex ev10 = topology.newExecutionVertex(jobVertexId1, 0);
        final TestingSchedulingExecutionVertex ev11 = topology.newExecutionVertex(jobVertexId1, 1);
        final TestingSchedulingExecutionVertex ev20 = topology.newExecutionVertex(jobVertexId2, 0);

        final SlotSharingGroup slotSharingGroup1 = new SlotSharingGroup();
        final ResourceProfile resourceProfile1 = ResourceProfile.fromResources(1, 10);
        slotSharingGroup1.addVertexToGroup(jobVertexId1);
        slotSharingGroup1.setResourceProfile(resourceProfile1);

        final SlotSharingGroup slotSharingGroup2 = new SlotSharingGroup();
        final ResourceProfile resourceProfile2 = ResourceProfile.fromResources(2, 20);
        slotSharingGroup2.addVertexToGroup(jobVertexId2);
        slotSharingGroup2.setResourceProfile(resourceProfile2);

        slotSharingGroups = Sets.newHashSet(slotSharingGroup1, slotSharingGroup2);

        final SlotSharingStrategy strategy =
                getSlotSharingStrategy(topology, slotSharingGroups, coLocationGroups);

        assertThat(strategy.getExecutionSlotSharingGroups()).hasSize(3);
        assertThat(strategy.getExecutionSlotSharingGroup(ev10.getId()).getResourceProfile())
                .isEqualTo(resourceProfile1);
        assertThat(strategy.getExecutionSlotSharingGroup(ev11.getId()).getResourceProfile())
                .isEqualTo(resourceProfile1);
        assertThat(strategy.getExecutionSlotSharingGroup(ev20.getId()).getResourceProfile())
                .isEqualTo(resourceProfile2);
    }

    @Test
    void testCoLocationConstraintIsRespected() {
        List<Tuple2<JobVertexID, List<TestingSchedulingExecutionVertex>>> jobVertexInfos =
                new ArrayList<>();
        CoLocationGroup coLocationGroup1 = new CoLocationGroupImpl();
        CoLocationGroup coLocationGroup2 = new CoLocationGroupImpl();
        List<TestingJobVertexInfo> mockedJobVertices =
                Lists.newArrayList(
                        new TestingJobVertexInfo(1, defaultSlotSharingGroup, null),
                        new TestingJobVertexInfo(2, defaultSlotSharingGroup, coLocationGroup1),
                        new TestingJobVertexInfo(2, defaultSlotSharingGroup, coLocationGroup1),
                        new TestingJobVertexInfo(3, defaultSlotSharingGroup, coLocationGroup2),
                        new TestingJobVertexInfo(3, defaultSlotSharingGroup, coLocationGroup2));
        setupCase(mockedJobVertices, jobVertexInfos);
        coLocationGroups.add(coLocationGroup1);
        coLocationGroups.add(coLocationGroup2);

        final SlotSharingStrategy strategy =
                getSlotSharingStrategy(topology, slotSharingGroups, coLocationGroups);
        List<TestingSchedulingExecutionVertex> executionVertices1 = jobVertexInfos.get(1).f1;
        List<TestingSchedulingExecutionVertex> executionVertices2 = jobVertexInfos.get(2).f1;

        assertThat(executionVertices1).hasSameSizeAs(executionVertices2);
        for (int i = 0; i < executionVertices1.size(); i++) {
            ExecutionSlotSharingGroup executionSlotSharingGroup =
                    strategy.getExecutionSlotSharingGroup(executionVertices1.get(i).getId());
            assertThat(executionSlotSharingGroup)
                    .isEqualTo(
                            strategy.getExecutionSlotSharingGroup(
                                    executionVertices2.get(i).getId()));
        }

        List<TestingSchedulingExecutionVertex> executionVertices3 = jobVertexInfos.get(3).f1;
        List<TestingSchedulingExecutionVertex> executionVertices4 = jobVertexInfos.get(4).f1;
        assertThat(executionVertices3).hasSameSizeAs(executionVertices4);
        for (int i = 0; i < executionVertices3.size(); i++) {
            assertThat(strategy.getExecutionSlotSharingGroup(executionVertices3.get(i).getId()))
                    .isEqualTo(
                            strategy.getExecutionSlotSharingGroup(
                                    executionVertices4.get(i).getId()));
        }
    }

    protected abstract SlotSharingStrategy getSlotSharingStrategy(
            final SchedulingTopology topology,
            final Set<SlotSharingGroup> slotSharingGroups,
            final Set<CoLocationGroup> coLocationGroups);

    protected void setupCase(
            List<TestingJobVertexInfo> mockedJobVertices,
            List<Tuple2<JobVertexID, List<TestingSchedulingExecutionVertex>>> jobVertexInfos) {
        for (TestingJobVertexInfo testingJobVertexInfo : mockedJobVertices) {
            List<TestingSchedulingExecutionVertex> tSEVs = new ArrayList<>();
            for (int subIndex = 0; subIndex < testingJobVertexInfo.parallelism; subIndex++) {
                tSEVs.add(
                        topology.newExecutionVertex(
                                testingJobVertexInfo.jobVertex.getID(), subIndex));
            }
            jobVertexInfos.add(Tuple2.of(testingJobVertexInfo.jobVertex.getID(), tSEVs));
        }
    }

    /** Util class to represent the simple job vertex information. */
    protected static class TestingJobVertexInfo {
        final JobVertex jobVertex = new JobVertex(null, new JobVertexID());
        @Nonnull SlotSharingGroup slotSharingGroup;
        @Nullable CoLocationGroup coLocationGroup;
        int parallelism;

        public TestingJobVertexInfo(
                int parallelism,
                @Nonnull SlotSharingGroup slotSharingGroup,
                @Nullable CoLocationGroup coLocationGroup) {
            Preconditions.checkArgument(parallelism > 0);
            this.parallelism = parallelism;
            this.slotSharingGroup = slotSharingGroup;
            this.coLocationGroup = coLocationGroup;

            this.slotSharingGroup.addVertexToGroup(jobVertex.getID());
            if (this.coLocationGroup != null) {
                ((CoLocationGroupImpl) this.coLocationGroup).addVertex(jobVertex);
            }
        }
    }
}
