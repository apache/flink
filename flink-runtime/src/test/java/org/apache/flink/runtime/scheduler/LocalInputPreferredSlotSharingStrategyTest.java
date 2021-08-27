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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
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
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;

/** Tests for {@link LocalInputPreferredSlotSharingStrategy}. */
public class LocalInputPreferredSlotSharingStrategyTest extends TestLogger {

    private TestingSchedulingTopology topology;

    private static final JobVertexID JOB_VERTEX_ID_1 = new JobVertexID();
    private static final JobVertexID JOB_VERTEX_ID_2 = new JobVertexID();

    private TestingSchedulingExecutionVertex ev11;
    private TestingSchedulingExecutionVertex ev12;
    private TestingSchedulingExecutionVertex ev21;
    private TestingSchedulingExecutionVertex ev22;

    private Set<SlotSharingGroup> slotSharingGroups;

    @Before
    public void setUp() throws Exception {
        topology = new TestingSchedulingTopology();

        ev11 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 0);
        ev12 = topology.newExecutionVertex(JOB_VERTEX_ID_1, 1);

        ev21 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 0);
        ev22 = topology.newExecutionVertex(JOB_VERTEX_ID_2, 1);

        final SlotSharingGroup slotSharingGroup = new SlotSharingGroup();
        slotSharingGroup.addVertexToGroup(JOB_VERTEX_ID_1);
        slotSharingGroup.addVertexToGroup(JOB_VERTEX_ID_2);
        slotSharingGroups = Collections.singleton(slotSharingGroup);
    }

    @Test
    public void testCoLocationConstraintIsRespected() {
        topology.connect(ev11, ev22);
        topology.connect(ev12, ev21);

        final CoLocationGroup coLocationGroup =
                new TestingCoLocationGroup(JOB_VERTEX_ID_1, JOB_VERTEX_ID_2);
        final Set<CoLocationGroup> coLocationGroups = Collections.singleton(coLocationGroup);

        final SlotSharingStrategy strategy =
                new LocalInputPreferredSlotSharingStrategy(
                        topology, slotSharingGroups, coLocationGroups);

        assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(2));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev11.getId()).getExecutionVertexIds(),
                containsInAnyOrder(ev11.getId(), ev21.getId()));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev12.getId()).getExecutionVertexIds(),
                containsInAnyOrder(ev12.getId(), ev22.getId()));
    }

    @Test
    public void testInputLocalityIsRespectedWithRescaleEdge() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final TestingSchedulingExecutionVertex ev11 =
                topology.newExecutionVertex(JOB_VERTEX_ID_1, 0);
        final TestingSchedulingExecutionVertex ev12 =
                topology.newExecutionVertex(JOB_VERTEX_ID_1, 1);

        final TestingSchedulingExecutionVertex ev21 =
                topology.newExecutionVertex(JOB_VERTEX_ID_2, 0);
        final TestingSchedulingExecutionVertex ev22 =
                topology.newExecutionVertex(JOB_VERTEX_ID_2, 1);
        final TestingSchedulingExecutionVertex ev23 =
                topology.newExecutionVertex(JOB_VERTEX_ID_2, 2);

        topology.connect(ev11, ev21);
        topology.connect(ev11, ev22);
        topology.connect(ev12, ev23);

        final SlotSharingStrategy strategy =
                new LocalInputPreferredSlotSharingStrategy(
                        topology, slotSharingGroups, Collections.emptySet());

        assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(3));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev21.getId()).getExecutionVertexIds(),
                containsInAnyOrder(ev11.getId(), ev21.getId()));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev22.getId()).getExecutionVertexIds(),
                containsInAnyOrder(ev22.getId()));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev23.getId()).getExecutionVertexIds(),
                containsInAnyOrder(ev12.getId(), ev23.getId()));
    }

    @Test
    public void testInputLocalityIsRespectedWithAllToAllEdge() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> producer =
                topology.addExecutionVertices()
                        .withParallelism(2)
                        .withJobVertexID(JOB_VERTEX_ID_1)
                        .finish();
        final List<TestingSchedulingExecutionVertex> consumer =
                topology.addExecutionVertices()
                        .withParallelism(2)
                        .withJobVertexID(JOB_VERTEX_ID_2)
                        .finish();

        topology.connectAllToAll(producer, consumer)
                .withResultPartitionType(ResultPartitionType.BLOCKING)
                .finish();

        ev11 = producer.get(0);
        ev12 = producer.get(1);

        ev21 = consumer.get(0);
        ev22 = consumer.get(1);

        final SlotSharingStrategy strategy =
                new LocalInputPreferredSlotSharingStrategy(
                        topology, slotSharingGroups, Collections.emptySet());
        assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(2));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev21.getId()).getExecutionVertexIds(),
                containsInAnyOrder(ev11.getId(), ev21.getId()));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev22.getId()).getExecutionVertexIds(),
                containsInAnyOrder(ev12.getId(), ev22.getId()));
    }

    @Test
    public void testDisjointVerticesInOneGroup() {
        final SlotSharingStrategy strategy =
                new LocalInputPreferredSlotSharingStrategy(
                        topology, slotSharingGroups, Collections.emptySet());

        assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(2));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev11.getId()).getExecutionVertexIds(),
                containsInAnyOrder(ev11.getId(), ev21.getId()));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev12.getId()).getExecutionVertexIds(),
                containsInAnyOrder(ev12.getId(), ev22.getId()));
    }

    @Test
    public void testVerticesInDifferentSlotSharingGroups() {
        final SlotSharingGroup slotSharingGroup1 = new SlotSharingGroup();
        slotSharingGroup1.addVertexToGroup(JOB_VERTEX_ID_1);
        final SlotSharingGroup slotSharingGroup2 = new SlotSharingGroup();
        slotSharingGroup2.addVertexToGroup(JOB_VERTEX_ID_2);

        final Set<SlotSharingGroup> slotSharingGroups = new HashSet<>();
        slotSharingGroups.add(slotSharingGroup1);
        slotSharingGroups.add(slotSharingGroup2);

        final SlotSharingStrategy strategy =
                new LocalInputPreferredSlotSharingStrategy(
                        topology, slotSharingGroups, Collections.emptySet());

        assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(4));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev11.getId()).getExecutionVertexIds(),
                containsInAnyOrder(ev11.getId()));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev12.getId()).getExecutionVertexIds(),
                containsInAnyOrder(ev12.getId()));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev21.getId()).getExecutionVertexIds(),
                containsInAnyOrder(ev21.getId()));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev22.getId()).getExecutionVertexIds(),
                containsInAnyOrder(ev22.getId()));
    }

    @Test
    public void testSetSlotSharingGroupResource() {
        final SlotSharingGroup slotSharingGroup1 = new SlotSharingGroup();
        final ResourceProfile resourceProfile1 = ResourceProfile.fromResources(1, 10);
        slotSharingGroup1.addVertexToGroup(JOB_VERTEX_ID_1);
        slotSharingGroup1.setResourceProfile(resourceProfile1);
        final SlotSharingGroup slotSharingGroup2 = new SlotSharingGroup();
        final ResourceProfile resourceProfile2 = ResourceProfile.fromResources(2, 20);
        slotSharingGroup2.addVertexToGroup(JOB_VERTEX_ID_2);
        slotSharingGroup2.setResourceProfile(resourceProfile2);

        final Set<SlotSharingGroup> slotSharingGroups = new HashSet<>();
        slotSharingGroups.add(slotSharingGroup1);
        slotSharingGroups.add(slotSharingGroup2);

        final SlotSharingStrategy strategy =
                new LocalInputPreferredSlotSharingStrategy(
                        topology, slotSharingGroups, Collections.emptySet());

        assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(4));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev11.getId()).getResourceProfile(),
                equalTo(resourceProfile1));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev12.getId()).getResourceProfile(),
                equalTo(resourceProfile1));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev21.getId()).getResourceProfile(),
                equalTo(resourceProfile2));
        assertThat(
                strategy.getExecutionSlotSharingGroup(ev22.getId()).getResourceProfile(),
                equalTo(resourceProfile2));
    }

    /**
     * In this test case, there are two JobEdges between two JobVertices. There will be no
     * ExecutionSlotSharingGroup that contains two vertices with the same JobVertexID.
     */
    @Test
    public void testInputLocalityIsRespectedWithTwoEdgesBetweenTwoVertices() throws Exception {
        int parallelism = 4;

        JobVertex v1 = createJobVertex("v1", JOB_VERTEX_ID_1, parallelism);
        JobVertex v2 = createJobVertex("v2", JOB_VERTEX_ID_2, parallelism);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        assertEquals(2, v1.getProducedDataSets().size());
        assertEquals(2, v2.getInputs().size());

        final JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(v1, v2);
        final ExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder().setJobGraph(jobGraph).build();
        final SchedulingTopology topology = executionGraph.getSchedulingTopology();

        final SlotSharingStrategy strategy =
                new LocalInputPreferredSlotSharingStrategy(
                        topology, slotSharingGroups, Collections.emptySet());

        assertThat(strategy.getExecutionSlotSharingGroups(), hasSize(4));

        ExecutionVertex[] ev1 =
                Objects.requireNonNull(executionGraph.getJobVertex(JOB_VERTEX_ID_1))
                        .getTaskVertices();
        ExecutionVertex[] ev2 =
                Objects.requireNonNull(executionGraph.getJobVertex(JOB_VERTEX_ID_2))
                        .getTaskVertices();
        for (int i = 0; i < parallelism; i++) {
            assertThat(
                    strategy.getExecutionSlotSharingGroup(ev1[i].getID()).getExecutionVertexIds(),
                    containsInAnyOrder(ev1[i].getID(), ev2[i].getID()));
        }
    }

    private static JobVertex createJobVertex(
            String vertexName, JobVertexID vertexId, int parallelism) {
        JobVertex jobVertex = new JobVertex(vertexName, vertexId);
        jobVertex.setParallelism(parallelism);
        jobVertex.setInvokableClass(NoOpInvokable.class);
        return jobVertex;
    }

    private static class TestingCoLocationGroup extends CoLocationGroupImpl {

        private final List<JobVertexID> vertexIDs;

        private TestingCoLocationGroup(JobVertexID... vertexIDs) {
            this.vertexIDs = Arrays.asList(vertexIDs);
        }

        @Override
        public List<JobVertexID> getVertexIds() {
            return vertexIDs;
        }
    }
}
