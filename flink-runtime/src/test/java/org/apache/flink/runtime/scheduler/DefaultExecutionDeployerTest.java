/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.TestingJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.TestingShuffleMaster;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DefaultExecutionDeployer}. */
@ExtendWith(TestLoggerExtension.class)
class DefaultExecutionDeployerTest {

    private ScheduledExecutorService executor;
    private ComponentMainThreadExecutor mainThreadExecutor;
    private TestExecutionOperationsDecorator testExecutionOperations;
    private ExecutionVertexVersioner executionVertexVersioner;
    private TestExecutionSlotAllocator testExecutionSlotAllocator;
    private TestingShuffleMaster shuffleMaster;
    private TestingJobMasterPartitionTracker partitionTracker;
    private Time partitionRegistrationTimeout;

    @BeforeEach
    void setUp() {
        executor = Executors.newSingleThreadScheduledExecutor();
        mainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forMainThread();
        testExecutionOperations =
                new TestExecutionOperationsDecorator(
                        new ExecutionOperations() {
                            @Override
                            public void deploy(Execution execution) {}

                            @Override
                            public CompletableFuture<?> cancel(Execution execution) {
                                return null;
                            }

                            @Override
                            public void markFailed(Execution execution, Throwable cause) {}
                        });
        executionVertexVersioner = new ExecutionVertexVersioner();
        testExecutionSlotAllocator = new TestExecutionSlotAllocator();
        shuffleMaster = new TestingShuffleMaster();
        partitionTracker = new TestingJobMasterPartitionTracker();
        partitionRegistrationTimeout = Time.milliseconds(5000);
    }

    @AfterEach
    void tearDown() {
        if (executor != null) {
            ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
        }
    }

    @Test
    void testDeployTasks() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final ExecutionGraph executionGraph = createExecutionGraph(jobGraph);
        final ExecutionDeployer executionDeployer = createExecutionDeployer();

        deployTasks(executionDeployer, executionGraph);

        assertThat(testExecutionOperations.getDeployedExecutions())
                .containsExactly(getAnyExecution(executionGraph).getAttemptId());
    }

    @Test
    void testDeployTasksOnlyIfAllSlotRequestsAreFulfilled() throws Exception {
        final JobGraph jobGraph = singleJobVertexJobGraph(4);
        final ExecutionGraph executionGraph = createExecutionGraph(jobGraph);
        final ExecutionDeployer executionDeployer = createExecutionDeployer();

        testExecutionSlotAllocator.disableAutoCompletePendingRequests();

        deployTasks(executionDeployer, executionGraph);

        assertThat(testExecutionOperations.getDeployedExecutions()).isEmpty();

        final ExecutionAttemptID attemptId = getAnyExecution(executionGraph).getAttemptId();
        testExecutionSlotAllocator.completePendingRequest(attemptId);
        assertThat(testExecutionOperations.getDeployedExecutions()).isEmpty();

        testExecutionSlotAllocator.completePendingRequests();
        assertThat(testExecutionOperations.getDeployedExecutions()).hasSize(4);
    }

    @Test
    void testDeploymentFailures() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();

        testExecutionOperations.enableFailDeploy();

        final ExecutionGraph executionGraph = createExecutionGraph(jobGraph);
        final ExecutionDeployer executionDeployer = createExecutionDeployer();
        deployTasks(executionDeployer, executionGraph);

        assertThat(testExecutionOperations.getFailedExecutions())
                .containsExactly(getAnyExecution(executionGraph).getAttemptId());
    }

    @Test
    void testSlotAllocationTimeout() throws Exception {
        final JobGraph jobGraph = singleJobVertexJobGraph(2);

        testExecutionSlotAllocator.disableAutoCompletePendingRequests();

        final ExecutionGraph executionGraph = createExecutionGraph(jobGraph);
        final ExecutionDeployer executionDeployer = createExecutionDeployer();
        deployTasks(executionDeployer, executionGraph);

        assertThat(testExecutionSlotAllocator.getPendingRequests()).hasSize(2);

        final ExecutionAttemptID attemptId = getAnyExecution(executionGraph).getAttemptId();
        testExecutionSlotAllocator.timeoutPendingRequest(attemptId);

        assertThat(testExecutionOperations.getFailedExecutions()).containsExactly(attemptId);
    }

    @Test
    void testSkipDeploymentIfVertexVersionOutdated() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();

        testExecutionSlotAllocator.disableAutoCompletePendingRequests();

        final ExecutionGraph executionGraph = createExecutionGraph(jobGraph);
        final ExecutionDeployer executionDeployer = createExecutionDeployer();
        deployTasks(executionDeployer, executionGraph);

        final ExecutionAttemptID attemptId = getAnyExecution(executionGraph).getAttemptId();

        executionVertexVersioner.recordModification(attemptId.getExecutionVertexId());
        testExecutionSlotAllocator.completePendingRequests();

        assertThat(testExecutionOperations.getDeployedVertices()).isEmpty();
    }

    @Test
    void testReleaseSlotIfVertexVersionOutdated() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();

        testExecutionSlotAllocator.disableAutoCompletePendingRequests();

        final ExecutionGraph executionGraph = createExecutionGraph(jobGraph);
        final ExecutionDeployer executionDeployer = createExecutionDeployer();
        deployTasks(executionDeployer, executionGraph);

        final ExecutionAttemptID attemptId = getAnyExecution(executionGraph).getAttemptId();

        executionVertexVersioner.recordModification(attemptId.getExecutionVertexId());
        testExecutionSlotAllocator.completePendingRequests();

        assertThat(testExecutionSlotAllocator.getReturnedSlots()).hasSize(1);
    }

    @Test
    void testDeployOnlyIfVertexIsCreated() throws Exception {
        final JobGraph jobGraph = singleNonParallelJobVertexJobGraph();
        final ExecutionGraph executionGraph = createExecutionGraph(jobGraph);
        final ExecutionDeployer executionDeployer = createExecutionDeployer();

        // deploy once to transition the tasks out from CREATED state
        deployTasks(executionDeployer, executionGraph);

        // The deploying of a non-CREATED vertex will result in IllegalStateException
        assertThatThrownBy(() -> deployTasks(executionDeployer, executionGraph))
                .as("IllegalStateException should happen")
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testDeploymentWaitForProducedPartitionRegistration() throws Exception {
        shuffleMaster.setAutoCompleteRegistration(false);

        final List<ResultPartitionID> trackedPartitions = new ArrayList<>();
        partitionTracker.setStartTrackingPartitionsConsumer(
                (resourceID, resultPartitionDeploymentDescriptor) ->
                        trackedPartitions.add(
                                resultPartitionDeploymentDescriptor
                                        .getShuffleDescriptor()
                                        .getResultPartitionID()));

        final JobGraph jobGraph = nonParallelSourceSinkJobGraph();
        final ExecutionGraph executionGraph = createExecutionGraph(jobGraph);
        final ExecutionDeployer executionDeployer = createExecutionDeployer();

        deployTasks(executionDeployer, executionGraph);

        assertThat(trackedPartitions).isEmpty();
        assertThat(testExecutionOperations.getDeployedExecutions()).isEmpty();

        shuffleMaster.completeAllPendingRegistrations();
        assertThat(trackedPartitions).hasSize(1);
        assertThat(testExecutionOperations.getDeployedExecutions()).hasSize(2);
    }

    @Test
    void testFailedProducedPartitionRegistration() throws Exception {
        shuffleMaster.setAutoCompleteRegistration(false);

        final JobGraph jobGraph = nonParallelSourceSinkJobGraph();
        final ExecutionGraph executionGraph = createExecutionGraph(jobGraph);
        final ExecutionDeployer executionDeployer = createExecutionDeployer();

        deployTasks(executionDeployer, executionGraph);

        assertThat(testExecutionOperations.getFailedExecutions()).isEmpty();

        shuffleMaster.failAllPendingRegistrations();
        assertThat(testExecutionOperations.getFailedExecutions()).hasSize(1);
    }

    @Test
    void testDirectExceptionOnProducedPartitionRegistration() throws Exception {
        shuffleMaster.setThrowExceptionalOnRegistration(true);

        final JobGraph jobGraph = nonParallelSourceSinkJobGraph();
        final ExecutionGraph executionGraph = createExecutionGraph(jobGraph);
        final ExecutionDeployer executionDeployer = createExecutionDeployer();

        deployTasks(executionDeployer, executionGraph);

        assertThat(testExecutionOperations.getFailedExecutions()).hasSize(1);
    }

    @Test
    void testProducedPartitionRegistrationTimeout() throws Exception {
        ScheduledExecutorService scheduledExecutorService = null;
        try {
            partitionRegistrationTimeout = Time.milliseconds(1);

            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
            mainThreadExecutor =
                    ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                            scheduledExecutorService);

            shuffleMaster.setAutoCompleteRegistration(false);

            final JobGraph jobGraph = nonParallelSourceSinkJobGraph();
            final ExecutionGraph executionGraph = createExecutionGraph(jobGraph);
            final ExecutionDeployer executionDeployer = createExecutionDeployer();

            deployTasks(executionDeployer, executionGraph);

            testExecutionOperations.awaitFailedExecutions(1);
        } finally {
            if (scheduledExecutorService != null) {
                scheduledExecutorService.shutdown();
            }
        }
    }

    private static JobGraph singleNonParallelJobVertexJobGraph() {
        return singleJobVertexJobGraph(1);
    }

    private static JobGraph singleJobVertexJobGraph(final int parallelism) {
        final JobVertex vertex = new JobVertex("source");
        vertex.setInvokableClass(NoOpInvokable.class);
        vertex.setParallelism(parallelism);
        return JobGraphTestUtils.streamingJobGraph(vertex);
    }

    private static JobGraph nonParallelSourceSinkJobGraph() {
        final JobVertex source = new JobVertex("source");
        source.setInvokableClass(NoOpInvokable.class);

        final JobVertex sink = new JobVertex("sink");
        sink.setInvokableClass(NoOpInvokable.class);

        sink.connectNewDataSetAsInput(
                source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        return JobGraphTestUtils.streamingJobGraph(source, sink);
    }

    private ExecutionGraph createExecutionGraph(final JobGraph jobGraph) throws Exception {
        final ExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .setShuffleMaster(shuffleMaster)
                        .setPartitionTracker(partitionTracker)
                        .build(executor);

        executionGraph.setInternalTaskFailuresListener(new TestingInternalFailuresListener());
        executionGraph.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

        return executionGraph;
    }

    private ExecutionDeployer createExecutionDeployer() {
        return new DefaultExecutionDeployer.Factory()
                .createInstance(
                        LoggerFactory.getLogger(DefaultExecutionDeployer.class),
                        testExecutionSlotAllocator,
                        testExecutionOperations,
                        executionVertexVersioner,
                        partitionRegistrationTimeout,
                        (ignored1, ignored2) -> {},
                        mainThreadExecutor);
    }

    private void deployTasks(ExecutionDeployer executionDeployer, ExecutionGraph executionGraph) {
        deployTasks(
                executionDeployer,
                IterableUtils.toStream(executionGraph.getAllExecutionVertices())
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .collect(Collectors.toList()));
    }

    private void deployTasks(ExecutionDeployer executionDeployer, List<Execution> executions) {
        final Set<ExecutionVertexID> executionVertexIds =
                executions.stream()
                        .map(Execution::getAttemptId)
                        .map(ExecutionAttemptID::getExecutionVertexId)
                        .collect(Collectors.toSet());

        executionDeployer.allocateSlotsAndDeploy(
                executions, executionVertexVersioner.recordVertexModifications(executionVertexIds));
    }

    private static Execution getAnyExecution(ExecutionGraph executionGraph) {
        return executionGraph.getRegisteredExecutions().values().iterator().next();
    }
}
