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

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.IterableUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ExecutionGraphToInputsLocationsRetrieverAdapter}. */
class ExecutionGraphToInputsLocationsRetrieverAdapterTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testGetConsumedPartitionGroupsAndProducers() throws Exception {
        final JobVertex producer1 = ExecutionGraphTestUtils.createNoOpVertex(1);
        final JobVertex producer2 = ExecutionGraphTestUtils.createNoOpVertex(1);
        final JobVertex consumer = ExecutionGraphTestUtils.createNoOpVertex(1);
        final IntermediateDataSet dataSet1 =
                consumer.connectNewDataSetAsInput(
                                producer1,
                                DistributionPattern.ALL_TO_ALL,
                                ResultPartitionType.PIPELINED)
                        .getSource();
        final IntermediateDataSet dataSet2 =
                consumer.connectNewDataSetAsInput(
                                producer2,
                                DistributionPattern.ALL_TO_ALL,
                                ResultPartitionType.PIPELINED)
                        .getSource();

        final ExecutionGraph eg =
                ExecutionGraphTestUtils.createExecutionGraph(
                        EXECUTOR_EXTENSION.getExecutor(), producer1, producer2, consumer);
        final ExecutionGraphToInputsLocationsRetrieverAdapter inputsLocationsRetriever =
                new ExecutionGraphToInputsLocationsRetrieverAdapter(eg);

        ExecutionVertexID evIdOfProducer1 = new ExecutionVertexID(producer1.getID(), 0);
        ExecutionVertexID evIdOfProducer2 = new ExecutionVertexID(producer2.getID(), 0);
        ExecutionVertexID evIdOfConsumer = new ExecutionVertexID(consumer.getID(), 0);

        Collection<ConsumedPartitionGroup> consumedPartitionGroupsOfProducer1 =
                inputsLocationsRetriever.getConsumedPartitionGroups(evIdOfProducer1);
        Collection<ConsumedPartitionGroup> consumedPartitionGroupsOfProducer2 =
                inputsLocationsRetriever.getConsumedPartitionGroups(evIdOfProducer2);
        Collection<ConsumedPartitionGroup> consumedPartitionGroupsOfConsumer =
                inputsLocationsRetriever.getConsumedPartitionGroups(evIdOfConsumer);

        IntermediateResultPartitionID partitionId1 =
                new IntermediateResultPartitionID(dataSet1.getId(), 0);
        IntermediateResultPartitionID partitionId2 =
                new IntermediateResultPartitionID(dataSet2.getId(), 0);
        assertThat(consumedPartitionGroupsOfProducer1).isEmpty();
        assertThat(consumedPartitionGroupsOfProducer2).isEmpty();
        assertThat(consumedPartitionGroupsOfConsumer).hasSize(2);
        assertThat(
                        consumedPartitionGroupsOfConsumer.stream()
                                .flatMap(IterableUtils::toStream)
                                .collect(Collectors.toSet()))
                .containsExactlyInAnyOrder(partitionId1, partitionId2);

        for (ConsumedPartitionGroup consumedPartitionGroup : consumedPartitionGroupsOfConsumer) {
            if (consumedPartitionGroup.getFirst().equals(partitionId1)) {
                assertThat(
                                inputsLocationsRetriever.getProducersOfConsumedPartitionGroup(
                                        consumedPartitionGroup))
                        .containsExactly(evIdOfProducer1);
            } else {
                assertThat(
                                inputsLocationsRetriever.getProducersOfConsumedPartitionGroup(
                                        consumedPartitionGroup))
                        .containsExactly(evIdOfProducer2);
            }
        }
    }

    /** Tests that it will get empty task manager location if vertex is not scheduled. */
    @Test
    void testGetEmptyTaskManagerLocationIfVertexNotScheduled() throws Exception {
        final JobVertex jobVertex = ExecutionGraphTestUtils.createNoOpVertex(1);

        final ExecutionGraph eg =
                ExecutionGraphTestUtils.createExecutionGraph(
                        EXECUTOR_EXTENSION.getExecutor(), jobVertex);
        final ExecutionGraphToInputsLocationsRetrieverAdapter inputsLocationsRetriever =
                new ExecutionGraphToInputsLocationsRetrieverAdapter(eg);

        ExecutionVertexID executionVertexId = new ExecutionVertexID(jobVertex.getID(), 0);
        Optional<CompletableFuture<TaskManagerLocation>> taskManagerLocation =
                inputsLocationsRetriever.getTaskManagerLocation(executionVertexId);

        assertThat(taskManagerLocation).isNotPresent();
    }

    /** Tests that it can get the task manager location in an Execution. */
    @Test
    void testGetTaskManagerLocationWhenScheduled() throws Exception {
        final JobVertex jobVertex = ExecutionGraphTestUtils.createNoOpVertex(1);

        final TestingLogicalSlot testingLogicalSlot =
                new TestingLogicalSlotBuilder().createTestingLogicalSlot();
        final ExecutionGraph eg =
                ExecutionGraphTestUtils.createExecutionGraph(
                        EXECUTOR_EXTENSION.getExecutor(), jobVertex);
        final ExecutionGraphToInputsLocationsRetrieverAdapter inputsLocationsRetriever =
                new ExecutionGraphToInputsLocationsRetrieverAdapter(eg);

        final ExecutionVertex onlyExecutionVertex = eg.getAllExecutionVertices().iterator().next();
        onlyExecutionVertex.getCurrentExecutionAttempt().transitionState(ExecutionState.SCHEDULED);
        onlyExecutionVertex.deployToSlot(testingLogicalSlot);

        ExecutionVertexID executionVertexId = new ExecutionVertexID(jobVertex.getID(), 0);
        Optional<CompletableFuture<TaskManagerLocation>> taskManagerLocationOptional =
                inputsLocationsRetriever.getTaskManagerLocation(executionVertexId);

        assertThat(taskManagerLocationOptional).isPresent();

        final CompletableFuture<TaskManagerLocation> taskManagerLocationFuture =
                taskManagerLocationOptional.get();
        assertThat(taskManagerLocationFuture.get())
                .isEqualTo(testingLogicalSlot.getTaskManagerLocation());
    }

    /**
     * Tests that it will throw exception when getting the task manager location of a non existing
     * execution.
     */
    @Test
    void testGetNonExistingExecutionVertexWillThrowException() throws Exception {
        final JobVertex jobVertex = ExecutionGraphTestUtils.createNoOpVertex(1);

        final ExecutionGraph eg =
                ExecutionGraphTestUtils.createExecutionGraph(
                        EXECUTOR_EXTENSION.getExecutor(), jobVertex);
        final ExecutionGraphToInputsLocationsRetrieverAdapter inputsLocationsRetriever =
                new ExecutionGraphToInputsLocationsRetrieverAdapter(eg);

        ExecutionVertexID invalidExecutionVertexId = new ExecutionVertexID(new JobVertexID(), 0);
        assertThatThrownBy(
                        () ->
                                inputsLocationsRetriever.getTaskManagerLocation(
                                        invalidExecutionVertexId),
                        "Should throw exception if execution vertex doesn't exist!")
                .isInstanceOf(IllegalStateException.class);
    }
}
