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

package org.apache.flink.runtime.jobmanager.scheduler;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.TestExecutionSlotAllocatorFactory;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.UnknownShuffleDescriptor;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

/** Tests for the updating of consumers depending on the producers result. */
public class UpdatePartitionConsumersTest extends TestLogger {

    private static final long TIMEOUT = 5000L;

    private JobGraph jobGraph;
    private JobVertex v1;
    private JobVertex v2;
    private JobVertex v3;
    private JobVertex v4;

    @Before
    public void setUp() {
        buildJobGraphWithBlockingEdgeWithinRegion();
    }

    /**
     * Build a graph which allows consumer vertex v4 to be deployed before its BLOCKING input v3
     * finishes.
     *
     * <pre>
     *                         +----+
     *        +-- pipelined -> | v2 | -- pipelined -+
     * +----+ |                +----+               |    +----+
     * | v1 |-|                                     | -> | v4 |
     * +----+ |                +----+               |    +----+
     *        +-- pipelined -> | v3 | -- blocking --+
     *                         +----+
     * </pre>
     */
    private void buildJobGraphWithBlockingEdgeWithinRegion() {
        v1 = new JobVertex("v1");
        v1.setInvokableClass(AbstractInvokable.class);
        v1.setParallelism(1);

        v2 = new JobVertex("v2");
        v2.setInvokableClass(AbstractInvokable.class);
        v2.setParallelism(1);

        v3 = new JobVertex("v3");
        v3.setInvokableClass(AbstractInvokable.class);
        v3.setParallelism(1);

        v4 = new JobVertex("v4");
        v4.setInvokableClass(AbstractInvokable.class);
        v4.setParallelism(1);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v3.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(
                v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(
                v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        jobGraph = JobGraphTestUtils.batchJobGraph(v1, v2, v3, v4);
    }

    /**
     * Test BLOCKING partition information are properly updated to consumers when its producer
     * finishes.
     */
    @Test
    public void testUpdatePartitionConsumers() throws Exception {
        final SimpleAckingTaskManagerGateway taskManagerGateway =
                new SimpleAckingTaskManagerGateway();

        final SchedulerBase scheduler =
                SchedulerTestingUtils.newSchedulerBuilder(
                                jobGraph, ComponentMainThreadExecutorServiceAdapter.forMainThread())
                        .setExecutionSlotAllocatorFactory(
                                new TestExecutionSlotAllocatorFactory(taskManagerGateway))
                        .build();

        final ExecutionVertex ev1 =
                scheduler.getExecutionVertex(new ExecutionVertexID(v1.getID(), 0));
        final ExecutionVertex ev2 =
                scheduler.getExecutionVertex(new ExecutionVertexID(v2.getID(), 0));
        final ExecutionVertex ev3 =
                scheduler.getExecutionVertex(new ExecutionVertexID(v3.getID(), 0));
        final ExecutionVertex ev4 =
                scheduler.getExecutionVertex(new ExecutionVertexID(v4.getID(), 0));

        final CompletableFuture<TaskDeploymentDescriptor> ev4TddFuture = new CompletableFuture<>();
        taskManagerGateway.setSubmitConsumer(
                tdd -> {
                    if (tdd.getExecutionAttemptId()
                            .equals(ev4.getCurrentExecutionAttempt().getAttemptId())) {
                        ev4TddFuture.complete(tdd);
                    }
                });

        scheduler.startScheduling();

        assertThat(ev1.getExecutionState(), is(ExecutionState.DEPLOYING));
        assertThat(ev2.getExecutionState(), is(ExecutionState.DEPLOYING));
        assertThat(ev3.getExecutionState(), is(ExecutionState.DEPLOYING));
        assertThat(ev4.getExecutionState(), is(ExecutionState.DEPLOYING));

        updateState(scheduler, ev1, ExecutionState.INITIALIZING);
        updateState(scheduler, ev1, ExecutionState.RUNNING);
        updateState(scheduler, ev2, ExecutionState.INITIALIZING);
        updateState(scheduler, ev2, ExecutionState.RUNNING);
        updateState(scheduler, ev3, ExecutionState.INITIALIZING);
        updateState(scheduler, ev3, ExecutionState.RUNNING);
        updateState(scheduler, ev4, ExecutionState.INITIALIZING);
        updateState(scheduler, ev4, ExecutionState.RUNNING);

        final InputGateDeploymentDescriptor ev4Igdd2 =
                ev4TddFuture.get(TIMEOUT, TimeUnit.MILLISECONDS).getInputGates().get(1);
        assertThat(ev4Igdd2.getShuffleDescriptors()[0], instanceOf(UnknownShuffleDescriptor.class));

        final CompletableFuture<Void> updatePartitionFuture = new CompletableFuture<>();
        taskManagerGateway.setUpdatePartitionsConsumer(
                (attemptId, partitionInfos, time) -> {
                    assertThat(attemptId, equalTo(ev4.getCurrentExecutionAttempt().getAttemptId()));
                    final List<PartitionInfo> partitionInfoList =
                            IterableUtils.toStream(partitionInfos).collect(Collectors.toList());
                    assertThat(partitionInfoList, hasSize(1));
                    final PartitionInfo partitionInfo = partitionInfoList.get(0);
                    assertThat(
                            partitionInfo.getIntermediateDataSetID(),
                            equalTo(v3.getProducedDataSets().get(0).getId()));
                    assertThat(
                            partitionInfo.getShuffleDescriptor(),
                            instanceOf(NettyShuffleDescriptor.class));
                    updatePartitionFuture.complete(null);
                });

        updateState(scheduler, ev1, ExecutionState.FINISHED);
        updateState(scheduler, ev3, ExecutionState.FINISHED);

        updatePartitionFuture.get(TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private void updateState(
            SchedulerBase scheduler, ExecutionVertex vertex, ExecutionState state) {
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(vertex.getCurrentExecutionAttempt().getAttemptId(), state));
    }
}
