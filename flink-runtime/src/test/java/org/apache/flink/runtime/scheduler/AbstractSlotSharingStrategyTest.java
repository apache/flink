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

import org.apache.flink.shaded.guava33.com.google.common.collect.Sets;

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

    protected TestingSchedulingTopology topology;

    protected JobVertexID jobVertexId1;
    protected JobVertexID jobVertexId2;

    protected SlotSharingGroup slotSharingGroup;
    protected SlotSharingGroup slotSharingGroup1;
    protected SlotSharingGroup slotSharingGroup2;

    @BeforeEach
    protected void setup() {
        this.topology = new TestingSchedulingTopology();
        this.jobVertexId1 = new JobVertexID();
        this.jobVertexId2 = new JobVertexID();
        this.slotSharingGroup = new SlotSharingGroup();
        this.slotSharingGroup1 = new SlotSharingGroup();
        this.slotSharingGroup2 = new SlotSharingGroup();
    }

    @Test
    void testSetSlotSharingGroupResource() {
        final TestingSchedulingExecutionVertex ev10 = topology.newExecutionVertex(jobVertexId1, 0);
        final TestingSchedulingExecutionVertex ev11 = topology.newExecutionVertex(jobVertexId1, 1);
        final TestingSchedulingExecutionVertex ev20 = topology.newExecutionVertex(jobVertexId2, 0);

        final ResourceProfile resourceProfile1 = ResourceProfile.fromResources(1, 10);
        slotSharingGroup1.addVertexToGroup(jobVertexId1);
        slotSharingGroup1.setResourceProfile(resourceProfile1);

        final ResourceProfile resourceProfile2 = ResourceProfile.fromResources(2, 20);
        slotSharingGroup2.addVertexToGroup(jobVertexId2);
        slotSharingGroup2.setResourceProfile(resourceProfile2);

        final SlotSharingStrategy strategy =
                getSlotSharingStrategy(
                        topology,
                        Sets.newHashSet(slotSharingGroup1, slotSharingGroup2),
                        Sets.newHashSet());

        assertThat(strategy.getExecutionSlotSharingGroups()).hasSize(3);
        assertThat(strategy.getExecutionSlotSharingGroup(ev10.getId()).getResourceProfile())
                .isEqualTo(resourceProfile1);
        assertThat(strategy.getExecutionSlotSharingGroup(ev11.getId()).getResourceProfile())
                .isEqualTo(resourceProfile1);
        assertThat(strategy.getExecutionSlotSharingGroup(ev20.getId()).getResourceProfile())
                .isEqualTo(resourceProfile2);
    }

    protected abstract SlotSharingStrategy getSlotSharingStrategy(
            final SchedulingTopology topology,
            final Set<SlotSharingGroup> slotSharingGroups,
            final Set<CoLocationGroup> coLocationGroups);

    protected void renderTopology(
            TestingSchedulingTopology topology,
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
