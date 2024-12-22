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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroupImpl;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.apache.flink.shaded.guava32.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava32.com.google.common.collect.Sets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.util.JobVertexConnectionUtils.connectNewDataSetAsInput;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LocalInputPreferredSlotSharingStrategy}. */
class LocalInputPreferredSlotSharingStrategyTest extends AbstractSlotSharingStrategyTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    private final JobVertexID jobVertexId3 = new JobVertexID();

    private TestingSchedulingExecutionVertex ev11;
    private TestingSchedulingExecutionVertex ev12;
    private TestingSchedulingExecutionVertex ev21;
    private TestingSchedulingExecutionVertex ev22;
    private TestingSchedulingExecutionVertex ev23;

    @Override
    protected SlotSharingStrategy getSlotSharingStrategy(
            SchedulingTopology topology,
            Set<SlotSharingGroup> slotSharingGroups,
            Set<CoLocationGroup> coLocationGroups) {
        return new LocalInputPreferredSlotSharingStrategy(
                topology, slotSharingGroups, coLocationGroups);
    }

    @Test
    void testInputLocalityIsRespectedWithRescaleEdge() {
        createTwoExeVerticesPerJv1AndJv2(slotSharingGroup);
        ev23 = topology.newExecutionVertex(jobVertexId2, 2);

        topology.connect(ev11, ev21);
        topology.connect(ev11, ev22);
        topology.connect(ev12, ev23);

        final SlotSharingStrategy strategy =
                getSlotSharingStrategy(
                        topology, Sets.newHashSet(slotSharingGroup), Collections.emptySet());

        assertThat(strategy.getExecutionSlotSharingGroups()).hasSize(3);
        assertThat(strategy.getExecutionSlotSharingGroup(ev21.getId()).getExecutionVertexIds())
                .contains(ev11.getId(), ev21.getId());
        assertThat(strategy.getExecutionSlotSharingGroup(ev22.getId()).getExecutionVertexIds())
                .contains(ev22.getId());
        assertThat(strategy.getExecutionSlotSharingGroup(ev23.getId()).getExecutionVertexIds())
                .contains(ev12.getId(), ev23.getId());
    }

    private void createTwoExeVerticesPerJv1AndJv2(SlotSharingGroup sharingGroup) {
        ev11 = topology.newExecutionVertex(jobVertexId1, 0);
        ev12 = topology.newExecutionVertex(jobVertexId1, 1);

        ev21 = topology.newExecutionVertex(jobVertexId2, 0);
        ev22 = topology.newExecutionVertex(jobVertexId2, 1);
        sharingGroup.addVertexToGroup(jobVertexId1);
        sharingGroup.addVertexToGroup(jobVertexId2);
    }

    @Test
    void testInputLocalityIsRespectedWithAllToAllEdge() {
        slotSharingGroup.addVertexToGroup(jobVertexId1);
        slotSharingGroup.addVertexToGroup(jobVertexId2);

        final List<TestingSchedulingExecutionVertex> producer =
                topology.addExecutionVertices()
                        .withParallelism(2)
                        .withJobVertexID(jobVertexId1)
                        .finish();
        final List<TestingSchedulingExecutionVertex> consumer =
                topology.addExecutionVertices()
                        .withParallelism(2)
                        .withJobVertexID(jobVertexId2)
                        .finish();

        topology.connectAllToAll(producer, consumer)
                .withResultPartitionType(ResultPartitionType.BLOCKING)
                .finish();

        ev11 = producer.get(0);
        ev12 = producer.get(1);

        ev21 = consumer.get(0);
        ev22 = consumer.get(1);

        final SlotSharingStrategy strategy =
                getSlotSharingStrategy(
                        topology, Sets.newHashSet(slotSharingGroup), Collections.emptySet());
        assertThat(strategy.getExecutionSlotSharingGroups()).hasSize(2);
        assertThat(strategy.getExecutionSlotSharingGroup(ev21.getId()).getExecutionVertexIds())
                .contains(ev11.getId(), ev21.getId());
        assertThat(strategy.getExecutionSlotSharingGroup(ev22.getId()).getExecutionVertexIds())
                .contains(ev12.getId(), ev22.getId());
    }

    @Test
    void testCoLocationConstraintIsRespected() {
        List<Tuple2<JobVertexID, List<TestingSchedulingExecutionVertex>>> jobVertexInfos =
                new ArrayList<>();
        CoLocationGroup coLocationGroup1 = new CoLocationGroupImpl();
        CoLocationGroup coLocationGroup2 = new CoLocationGroupImpl();
        List<TestingJobVertexInfo> mockedJobVertices =
                Lists.newArrayList(
                        new TestingJobVertexInfo(1, slotSharingGroup, null),
                        new TestingJobVertexInfo(2, slotSharingGroup, coLocationGroup1),
                        new TestingJobVertexInfo(2, slotSharingGroup, coLocationGroup1),
                        new TestingJobVertexInfo(3, slotSharingGroup, coLocationGroup2),
                        new TestingJobVertexInfo(3, slotSharingGroup, coLocationGroup2));
        renderTopology(topology, mockedJobVertices, jobVertexInfos);

        final SlotSharingStrategy strategy =
                getSlotSharingStrategy(
                        topology,
                        Sets.newHashSet(slotSharingGroup),
                        Sets.newHashSet(coLocationGroup1, coLocationGroup2));
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

    @Test
    void testDisjointVerticesInOneGroup() {
        createTwoExeVerticesPerJv1AndJv2(slotSharingGroup);
        final SlotSharingStrategy strategy =
                getSlotSharingStrategy(
                        topology, Sets.newHashSet(slotSharingGroup), Collections.emptySet());

        assertThat(strategy.getExecutionSlotSharingGroups()).hasSize(2);
        assertThat(strategy.getExecutionSlotSharingGroup(ev11.getId()).getExecutionVertexIds())
                .contains(ev11.getId(), ev21.getId());
        assertThat(strategy.getExecutionSlotSharingGroup(ev12.getId()).getExecutionVertexIds())
                .contains(ev12.getId(), ev22.getId());
    }

    @Test
    void testVerticesInDifferentSlotSharingGroups() {

        ev11 = topology.newExecutionVertex(jobVertexId1, 0);
        ev12 = topology.newExecutionVertex(jobVertexId1, 1);
        ev21 = topology.newExecutionVertex(jobVertexId2, 0);
        ev22 = topology.newExecutionVertex(jobVertexId2, 1);

        slotSharingGroup1.addVertexToGroup(jobVertexId1);
        slotSharingGroup2.addVertexToGroup(jobVertexId2);

        final SlotSharingStrategy strategy =
                getSlotSharingStrategy(
                        topology,
                        Sets.newHashSet(slotSharingGroup1, slotSharingGroup2),
                        Collections.emptySet());

        assertThat(strategy.getExecutionSlotSharingGroups()).hasSize(4);
        assertThat(strategy.getExecutionSlotSharingGroup(ev11.getId()).getExecutionVertexIds())
                .contains(ev11.getId());
        assertThat(strategy.getExecutionSlotSharingGroup(ev12.getId()).getExecutionVertexIds())
                .contains(ev12.getId());
        assertThat(strategy.getExecutionSlotSharingGroup(ev21.getId()).getExecutionVertexIds())
                .contains(ev21.getId());
        assertThat(strategy.getExecutionSlotSharingGroup(ev22.getId()).getExecutionVertexIds())
                .contains(ev22.getId());
    }

    /**
     * In this test case, there are two JobEdges between two JobVertices. There will be no
     * ExecutionSlotSharingGroup that contains two vertices with the same JobVertexID.
     */
    @Test
    void testInputLocalityIsRespectedWithTwoEdgesBetweenTwoVertices() throws Exception {
        createTwoExeVerticesPerJv1AndJv2(slotSharingGroup);
        int parallelism = 4;

        JobVertex v1 = createJobVertex("v1", jobVertexId1, parallelism);
        JobVertex v2 = createJobVertex("v2", jobVertexId2, parallelism);

        connectNewDataSetAsInput(
                v2, v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
        connectNewDataSetAsInput(
                v2, v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        assertThat(v1.getProducedDataSets()).hasSize(2);
        assertThat(v2.getInputs()).hasSize(2);

        final JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(v1, v2);
        final ExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .build(EXECUTOR_EXTENSION.getExecutor());
        final SchedulingTopology topology = executionGraph.getSchedulingTopology();

        final SlotSharingStrategy strategy =
                getSlotSharingStrategy(
                        topology, Sets.newHashSet(slotSharingGroup), Collections.emptySet());

        assertThat(strategy.getExecutionSlotSharingGroups()).hasSize(4);

        ExecutionVertex[] ev1 =
                Objects.requireNonNull(executionGraph.getJobVertex(jobVertexId1)).getTaskVertices();
        ExecutionVertex[] ev2 =
                Objects.requireNonNull(executionGraph.getJobVertex(jobVertexId2)).getTaskVertices();
        for (int i = 0; i < parallelism; i++) {
            assertThat(
                            strategy.getExecutionSlotSharingGroup(ev1[i].getID())
                                    .getExecutionVertexIds())
                    .contains(ev1[i].getID(), ev2[i].getID());
        }
    }

    @Test
    void testGetExecutionSlotSharingGroupOfLateAttachedVertices() {
        slotSharingGroup1.addVertexToGroup(jobVertexId1);
        slotSharingGroup1.addVertexToGroup(jobVertexId2);
        slotSharingGroup2.addVertexToGroup(jobVertexId3);

        TestingSchedulingExecutionVertex ev1 = topology.newExecutionVertex(jobVertexId1, 0);
        TestingSchedulingExecutionVertex ev2 = topology.newExecutionVertex(jobVertexId2, 0);
        topology.connect(ev1, ev2);

        final LocalInputPreferredSlotSharingStrategy strategy =
                (LocalInputPreferredSlotSharingStrategy)
                        getSlotSharingStrategy(
                                topology,
                                new HashSet<>(Arrays.asList(slotSharingGroup1, slotSharingGroup2)),
                                Collections.emptySet());

        assertThat(strategy.getExecutionSlotSharingGroups()).hasSize(1);
        assertThat(strategy.getExecutionSlotSharingGroup(ev1.getId()).getExecutionVertexIds())
                .contains(ev1.getId(), ev2.getId());
        assertThat(strategy.getExecutionSlotSharingGroup(ev2.getId()).getExecutionVertexIds())
                .contains(ev1.getId(), ev2.getId());

        // add new job vertices and notify scheduling topology updated
        TestingSchedulingExecutionVertex ev3 = topology.newExecutionVertex(jobVertexId3, 0);
        topology.connect(ev2, ev3, ResultPartitionType.BLOCKING);
        strategy.notifySchedulingTopologyUpdated(topology, Collections.singletonList(ev3.getId()));

        assertThat(strategy.getExecutionSlotSharingGroups()).hasSize(2);
        assertThat(strategy.getExecutionSlotSharingGroup(ev1.getId()).getExecutionVertexIds())
                .contains(ev1.getId(), ev2.getId());
        assertThat(strategy.getExecutionSlotSharingGroup(ev2.getId()).getExecutionVertexIds())
                .contains(ev1.getId(), ev2.getId());
        assertThat(strategy.getExecutionSlotSharingGroup(ev3.getId()).getExecutionVertexIds())
                .contains(ev3.getId());
    }

    private static JobVertex createJobVertex(
            String vertexName, JobVertexID vertexId, int parallelism) {
        JobVertex jobVertex = new JobVertex(vertexName, vertexId);
        jobVertex.setParallelism(parallelism);
        jobVertex.setInvokableClass(NoOpInvokable.class);
        return jobVertex;
    }
}
