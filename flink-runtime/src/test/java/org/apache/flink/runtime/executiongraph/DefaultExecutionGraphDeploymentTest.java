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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobService;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.BatchTask;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlotProvider;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.FunctionUtils;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Tests for {@link DefaultExecutionGraph} deployment. */
public class DefaultExecutionGraphDeploymentTest extends TestLogger {

    /** BLOB server instance to use for the job graph. */
    protected BlobWriter blobWriter = VoidBlobWriter.getInstance();

    /**
     * Permanent BLOB cache instance to use for the actor gateway that handles the {@link
     * TaskDeploymentDescriptor} loading (may be <tt>null</tt>).
     */
    protected PermanentBlobService blobCache = null;

    /**
     * Checks that the job information for the given ID has been offloaded successfully (if
     * offloading is used).
     *
     * @param eg the execution graph that was created
     */
    protected void checkJobOffloaded(DefaultExecutionGraph eg) throws Exception {
        assertTrue(eg.getJobInformationOrBlobKey().isLeft());
    }

    /**
     * Checks that the task information for the job vertex has been offloaded successfully (if
     * offloading is used).
     *
     * @param eg the execution graph that was created
     * @param jobVertexId job vertex ID
     */
    protected void checkTaskOffloaded(ExecutionGraph eg, JobVertexID jobVertexId) throws Exception {
        assertTrue(eg.getJobVertex(jobVertexId).getTaskInformationOrBlobKey().isLeft());
    }

    @Test
    public void testBuildDeploymentDescriptor() throws Exception {

        final JobVertexID jid1 = new JobVertexID();
        final JobVertexID jid2 = new JobVertexID();
        final JobVertexID jid3 = new JobVertexID();
        final JobVertexID jid4 = new JobVertexID();

        JobVertex v1 = new JobVertex("v1", jid1);
        JobVertex v2 = new JobVertex("v2", jid2);
        JobVertex v3 = new JobVertex("v3", jid3);
        JobVertex v4 = new JobVertex("v4", jid4);

        v1.setParallelism(10);
        v2.setParallelism(10);
        v3.setParallelism(10);
        v4.setParallelism(10);

        v1.setInvokableClass(BatchTask.class);
        v2.setInvokableClass(BatchTask.class);
        v3.setInvokableClass(BatchTask.class);
        v4.setInvokableClass(BatchTask.class);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v3.connectNewDataSetAsInput(
                v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(
                v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

        final JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(v1, v2, v3, v4);
        final JobID jobId = jobGraph.getJobID();

        DirectScheduledExecutorService executor = new DirectScheduledExecutorService();
        DefaultExecutionGraph eg =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .setFutureExecutor(executor)
                        .setIoExecutor(executor)
                        .setBlobWriter(blobWriter)
                        .build();

        eg.start(ComponentMainThreadExecutorServiceAdapter.forMainThread());

        checkJobOffloaded(eg);

        ExecutionJobVertex ejv = eg.getAllVertices().get(jid2);
        ExecutionVertex vertex = ejv.getTaskVertices()[3];

        final SimpleAckingTaskManagerGateway taskManagerGateway =
                new SimpleAckingTaskManagerGateway();
        final CompletableFuture<TaskDeploymentDescriptor> tdd = new CompletableFuture<>();

        taskManagerGateway.setSubmitConsumer(
                FunctionUtils.uncheckedConsumer(
                        taskDeploymentDescriptor -> {
                            taskDeploymentDescriptor.loadBigData(blobCache);
                            tdd.complete(taskDeploymentDescriptor);
                        }));

        final LogicalSlot slot =
                new TestingLogicalSlotBuilder()
                        .setTaskManagerGateway(taskManagerGateway)
                        .createTestingLogicalSlot();

        assertEquals(ExecutionState.CREATED, vertex.getExecutionState());

        vertex.getCurrentExecutionAttempt()
                .registerProducedPartitions(slot.getTaskManagerLocation(), true)
                .get();
        vertex.deployToSlot(slot);

        assertEquals(ExecutionState.DEPLOYING, vertex.getExecutionState());
        checkTaskOffloaded(eg, vertex.getJobvertexId());

        TaskDeploymentDescriptor descr = tdd.get();
        assertNotNull(descr);

        JobInformation jobInformation =
                descr.getSerializedJobInformation().deserializeValue(getClass().getClassLoader());
        TaskInformation taskInformation =
                descr.getSerializedTaskInformation().deserializeValue(getClass().getClassLoader());

        assertEquals(jobId, descr.getJobId());
        assertEquals(jobId, jobInformation.getJobId());
        assertEquals(jid2, taskInformation.getJobVertexId());
        assertEquals(3, descr.getSubtaskIndex());
        assertEquals(10, taskInformation.getNumberOfSubtasks());
        assertEquals(BatchTask.class.getName(), taskInformation.getInvokableClassName());
        assertEquals("v2", taskInformation.getTaskName());

        Collection<ResultPartitionDeploymentDescriptor> producedPartitions =
                descr.getProducedPartitions();
        Collection<InputGateDeploymentDescriptor> consumedPartitions = descr.getInputGates();

        assertEquals(2, producedPartitions.size());
        assertEquals(1, consumedPartitions.size());

        Iterator<ResultPartitionDeploymentDescriptor> iteratorProducedPartitions =
                producedPartitions.iterator();
        Iterator<InputGateDeploymentDescriptor> iteratorConsumedPartitions =
                consumedPartitions.iterator();

        assertEquals(10, iteratorProducedPartitions.next().getNumberOfSubpartitions());
        assertEquals(10, iteratorProducedPartitions.next().getNumberOfSubpartitions());
        assertEquals(10, iteratorConsumedPartitions.next().getShuffleDescriptors().length);
    }

    @Test
    public void testRegistrationOfExecutionsFinishing() {
        try {
            final JobVertexID jid1 = new JobVertexID();
            final JobVertexID jid2 = new JobVertexID();

            JobVertex v1 = new JobVertex("v1", jid1);
            JobVertex v2 = new JobVertex("v2", jid2);

            SchedulerBase scheduler = setupScheduler(v1, 7650, v2, 2350);
            Collection<Execution> executions =
                    new ArrayList<>(
                            scheduler.getExecutionGraph().getRegisteredExecutions().values());

            for (Execution e : executions) {
                e.markFinished();
            }

            assertEquals(0, scheduler.getExecutionGraph().getRegisteredExecutions().size());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testRegistrationOfExecutionsFailing() {
        try {

            final JobVertexID jid1 = new JobVertexID();
            final JobVertexID jid2 = new JobVertexID();

            JobVertex v1 = new JobVertex("v1", jid1);
            JobVertex v2 = new JobVertex("v2", jid2);

            SchedulerBase scheduler = setupScheduler(v1, 7, v2, 6);
            Collection<Execution> executions =
                    new ArrayList<>(
                            scheduler.getExecutionGraph().getRegisteredExecutions().values());

            for (Execution e : executions) {
                e.markFailed(null);
            }

            assertEquals(0, scheduler.getExecutionGraph().getRegisteredExecutions().size());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testRegistrationOfExecutionsFailedExternally() {
        try {

            final JobVertexID jid1 = new JobVertexID();
            final JobVertexID jid2 = new JobVertexID();

            JobVertex v1 = new JobVertex("v1", jid1);
            JobVertex v2 = new JobVertex("v2", jid2);

            SchedulerBase scheduler = setupScheduler(v1, 7, v2, 6);
            Collection<Execution> executions =
                    new ArrayList<>(
                            scheduler.getExecutionGraph().getRegisteredExecutions().values());

            for (Execution e : executions) {
                e.fail(null);
            }

            assertEquals(0, scheduler.getExecutionGraph().getRegisteredExecutions().size());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     * Verifies that {@link SchedulerNG#updateTaskExecutionState(TaskExecutionState)} updates the
     * accumulators and metrics for an execution that failed or was canceled.
     */
    @Test
    public void testAccumulatorsAndMetricsForwarding() throws Exception {
        final JobVertexID jid1 = new JobVertexID();
        final JobVertexID jid2 = new JobVertexID();

        JobVertex v1 = new JobVertex("v1", jid1);
        JobVertex v2 = new JobVertex("v2", jid2);

        SchedulerBase scheduler = setupScheduler(v1, 1, v2, 1);
        ExecutionGraph graph = scheduler.getExecutionGraph();
        Map<ExecutionAttemptID, Execution> executions = graph.getRegisteredExecutions();

        // verify behavior for canceled executions
        Execution execution1 = executions.values().iterator().next();

        IOMetrics ioMetrics = new IOMetrics(0, 0, 0, 0);
        Map<String, Accumulator<?, ?>> accumulators = new HashMap<>();
        accumulators.put("acc", new IntCounter(4));
        AccumulatorSnapshot accumulatorSnapshot =
                new AccumulatorSnapshot(graph.getJobID(), execution1.getAttemptId(), accumulators);

        TaskExecutionState state =
                new TaskExecutionState(
                        execution1.getAttemptId(),
                        ExecutionState.CANCELED,
                        null,
                        accumulatorSnapshot,
                        ioMetrics);

        scheduler.updateTaskExecutionState(state);

        assertEquals(ioMetrics, execution1.getIOMetrics());
        assertNotNull(execution1.getUserAccumulators());
        assertEquals(4, execution1.getUserAccumulators().get("acc").getLocalValue());

        // verify behavior for failed executions
        Execution execution2 = executions.values().iterator().next();

        IOMetrics ioMetrics2 = new IOMetrics(0, 0, 0, 0);
        Map<String, Accumulator<?, ?>> accumulators2 = new HashMap<>();
        accumulators2.put("acc", new IntCounter(8));
        AccumulatorSnapshot accumulatorSnapshot2 =
                new AccumulatorSnapshot(graph.getJobID(), execution2.getAttemptId(), accumulators2);

        TaskExecutionState state2 =
                new TaskExecutionState(
                        execution2.getAttemptId(),
                        ExecutionState.FAILED,
                        null,
                        accumulatorSnapshot2,
                        ioMetrics2);

        scheduler.updateTaskExecutionState(state2);

        assertEquals(ioMetrics2, execution2.getIOMetrics());
        assertNotNull(execution2.getUserAccumulators());
        assertEquals(8, execution2.getUserAccumulators().get("acc").getLocalValue());
    }

    /**
     * Verifies that {@link Execution#completeCancelling(Map, IOMetrics, boolean)} and {@link
     * Execution#markFailed(Throwable, boolean, Map, IOMetrics, boolean, boolean)} store the given
     * accumulators and metrics correctly.
     */
    @Test
    public void testAccumulatorsAndMetricsStorage() throws Exception {
        final JobVertexID jid1 = new JobVertexID();
        final JobVertexID jid2 = new JobVertexID();

        JobVertex v1 = new JobVertex("v1", jid1);
        JobVertex v2 = new JobVertex("v2", jid2);

        SchedulerBase scheduler = setupScheduler(v1, 1, v2, 1);
        Map<ExecutionAttemptID, Execution> executions =
                scheduler.getExecutionGraph().getRegisteredExecutions();

        IOMetrics ioMetrics = new IOMetrics(0, 0, 0, 0);
        Map<String, Accumulator<?, ?>> accumulators = Collections.emptyMap();

        Execution execution1 = executions.values().iterator().next();
        execution1.cancel();
        execution1.completeCancelling(accumulators, ioMetrics, false);

        assertEquals(ioMetrics, execution1.getIOMetrics());
        assertEquals(accumulators, execution1.getUserAccumulators());

        Execution execution2 = executions.values().iterator().next();
        execution2.markFailed(new Throwable(), false, accumulators, ioMetrics, false, true);

        assertEquals(ioMetrics, execution2.getIOMetrics());
        assertEquals(accumulators, execution2.getUserAccumulators());
    }

    @Test
    public void testRegistrationOfExecutionsCanceled() {
        try {

            final JobVertexID jid1 = new JobVertexID();
            final JobVertexID jid2 = new JobVertexID();

            JobVertex v1 = new JobVertex("v1", jid1);
            JobVertex v2 = new JobVertex("v2", jid2);

            SchedulerBase scheduler = setupScheduler(v1, 19, v2, 37);
            Collection<Execution> executions =
                    new ArrayList<>(
                            scheduler.getExecutionGraph().getRegisteredExecutions().values());

            for (Execution e : executions) {
                e.cancel();
                e.completeCancelling();
            }

            assertEquals(0, scheduler.getExecutionGraph().getRegisteredExecutions().size());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     * Tests that a blocking batch job fails if there are not enough resources left to schedule the
     * succeeding tasks. This test case is related to [FLINK-4296] where finished producing tasks
     * swallow the fail exception when scheduling a consumer task.
     */
    @Test
    public void testNoResourceAvailableFailure() throws Exception {
        JobVertex v1 = new JobVertex("source");
        JobVertex v2 = new JobVertex("sink");

        int dop1 = 2;
        int dop2 = 2;

        v1.setParallelism(dop1);
        v2.setParallelism(dop2);

        v1.setInvokableClass(BatchTask.class);
        v2.setInvokableClass(BatchTask.class);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

        final JobGraph graph = JobGraphTestUtils.batchJobGraph(v1, v2);

        DirectScheduledExecutorService directExecutor = new DirectScheduledExecutorService();

        // execution graph that executes actions synchronously
        final SchedulerBase scheduler =
                SchedulerTestingUtils.newSchedulerBuilder(
                                graph, ComponentMainThreadExecutorServiceAdapter.forMainThread())
                        .setExecutionSlotAllocatorFactory(
                                SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory(
                                        TestingPhysicalSlotProvider
                                                .createWithLimitedAmountOfPhysicalSlots(1)))
                        .setFutureExecutor(directExecutor)
                        .setBlobWriter(blobWriter)
                        .build();

        final ExecutionGraph eg = scheduler.getExecutionGraph();

        checkJobOffloaded((DefaultExecutionGraph) eg);

        // schedule, this triggers mock deployment
        scheduler.startScheduling();

        ExecutionAttemptID attemptID =
                eg.getJobVertex(v1.getID())
                        .getTaskVertices()[0]
                        .getCurrentExecutionAttempt()
                        .getAttemptId();
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(attemptID, ExecutionState.RUNNING));
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(attemptID, ExecutionState.FINISHED, null));

        assertEquals(JobStatus.FAILED, eg.getState());
    }

    // ------------------------------------------------------------------------
    //  retained checkpoints config test
    // ------------------------------------------------------------------------

    @Test
    public void testSettingDefaultMaxNumberOfCheckpointsToRetain() throws Exception {
        final Configuration jobManagerConfig = new Configuration();

        final ExecutionGraph eg = createExecutionGraph(jobManagerConfig);

        assertEquals(
                CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue().intValue(),
                eg.getCheckpointCoordinator()
                        .getCheckpointStore()
                        .getMaxNumberOfRetainedCheckpoints());
    }

    private SchedulerBase setupScheduler(JobVertex v1, int dop1, JobVertex v2, int dop2)
            throws Exception {
        v1.setParallelism(dop1);
        v2.setParallelism(dop2);

        v1.setInvokableClass(BatchTask.class);
        v2.setInvokableClass(BatchTask.class);

        DirectScheduledExecutorService executorService = new DirectScheduledExecutorService();

        // execution graph that executes actions synchronously
        final SchedulerBase scheduler =
                SchedulerTestingUtils.newSchedulerBuilder(
                                JobGraphTestUtils.streamingJobGraph(v1, v2),
                                ComponentMainThreadExecutorServiceAdapter.forMainThread())
                        .setExecutionSlotAllocatorFactory(
                                SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory())
                        .setFutureExecutor(executorService)
                        .setBlobWriter(blobWriter)
                        .build();
        final ExecutionGraph eg = scheduler.getExecutionGraph();

        checkJobOffloaded((DefaultExecutionGraph) eg);

        // schedule, this triggers mock deployment
        scheduler.startScheduling();

        Map<ExecutionAttemptID, Execution> executions = eg.getRegisteredExecutions();
        assertEquals(dop1 + dop2, executions.size());

        return scheduler;
    }

    @Test
    public void testSettingIllegalMaxNumberOfCheckpointsToRetain() throws Exception {

        final int negativeMaxNumberOfCheckpointsToRetain = -10;

        final Configuration jobManagerConfig = new Configuration();
        jobManagerConfig.setInteger(
                CheckpointingOptions.MAX_RETAINED_CHECKPOINTS,
                negativeMaxNumberOfCheckpointsToRetain);

        final ExecutionGraph eg = createExecutionGraph(jobManagerConfig);

        assertNotEquals(
                negativeMaxNumberOfCheckpointsToRetain,
                eg.getCheckpointCoordinator()
                        .getCheckpointStore()
                        .getMaxNumberOfRetainedCheckpoints());

        assertEquals(
                CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.defaultValue().intValue(),
                eg.getCheckpointCoordinator()
                        .getCheckpointStore()
                        .getMaxNumberOfRetainedCheckpoints());
    }

    /** Tests that the {@link ExecutionGraph} is deployed in topological order. */
    @Test
    public void testExecutionGraphIsDeployedInTopologicalOrder() throws Exception {
        final int sourceParallelism = 2;
        final int sinkParallelism = 1;

        final JobVertex sourceVertex = new JobVertex("source");
        sourceVertex.setInvokableClass(NoOpInvokable.class);
        sourceVertex.setParallelism(sourceParallelism);

        final JobVertex sinkVertex = new JobVertex("sink");
        sinkVertex.setInvokableClass(NoOpInvokable.class);
        sinkVertex.setParallelism(sinkParallelism);

        sinkVertex.connectNewDataSetAsInput(
                sourceVertex, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        final int numberTasks = sourceParallelism + sinkParallelism;
        final ArrayBlockingQueue<ExecutionAttemptID> submittedTasksQueue =
                new ArrayBlockingQueue<>(numberTasks);
        TestingTaskExecutorGatewayBuilder testingTaskExecutorGatewayBuilder =
                new TestingTaskExecutorGatewayBuilder();
        testingTaskExecutorGatewayBuilder.setSubmitTaskConsumer(
                (taskDeploymentDescriptor, jobMasterId) -> {
                    submittedTasksQueue.offer(taskDeploymentDescriptor.getExecutionAttemptId());
                    return CompletableFuture.completedFuture(Acknowledge.get());
                });

        final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        final TestingTaskExecutorGateway taskExecutorGateway =
                testingTaskExecutorGatewayBuilder.createTestingTaskExecutorGateway();
        final RpcTaskManagerGateway taskManagerGateway =
                new RpcTaskManagerGateway(taskExecutorGateway, JobMasterId.generate());

        final JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(sourceVertex, sinkVertex);

        final TestingPhysicalSlotProvider physicalSlotProvider =
                TestingPhysicalSlotProvider.createWithoutImmediatePhysicalSlotCreation();
        final SchedulerBase scheduler =
                SchedulerTestingUtils.newSchedulerBuilder(
                                jobGraph, ComponentMainThreadExecutorServiceAdapter.forMainThread())
                        .setExecutionSlotAllocatorFactory(
                                SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory(
                                        physicalSlotProvider))
                        .setFutureExecutor(new DirectScheduledExecutorService())
                        .build();
        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        scheduler.startScheduling();

        // change the order in which the futures are completed
        final List<CompletableFuture<TestingPhysicalSlot>> shuffledFutures =
                new ArrayList<>(physicalSlotProvider.getResponses().values());
        Collections.shuffle(shuffledFutures);

        for (CompletableFuture<TestingPhysicalSlot> slotFuture : shuffledFutures) {
            slotFuture.complete(
                    TestingPhysicalSlot.builder()
                            .withTaskManagerLocation(taskManagerLocation)
                            .withTaskManagerGateway(taskManagerGateway)
                            .build());
        }

        final List<ExecutionAttemptID> submittedTasks = new ArrayList<>(numberTasks);

        for (int i = 0; i < numberTasks; i++) {
            submittedTasks.add(submittedTasksQueue.take());
        }

        final Collection<ExecutionAttemptID> firstStage = new ArrayList<>(sourceParallelism);
        for (ExecutionVertex taskVertex :
                executionGraph.getJobVertex(sourceVertex.getID()).getTaskVertices()) {
            firstStage.add(taskVertex.getCurrentExecutionAttempt().getAttemptId());
        }

        final Collection<ExecutionAttemptID> secondStage = new ArrayList<>(sinkParallelism);
        for (ExecutionVertex taskVertex :
                executionGraph.getJobVertex(sinkVertex.getID()).getTaskVertices()) {
            secondStage.add(taskVertex.getCurrentExecutionAttempt().getAttemptId());
        }

        assertThat(
                submittedTasks, new ExecutionStageMatcher(Arrays.asList(firstStage, secondStage)));
    }

    private ExecutionGraph createExecutionGraph(Configuration configuration) throws Exception {
        final JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();
        jobGraph.setSnapshotSettings(
                new JobCheckpointingSettings(
                        new CheckpointCoordinatorConfiguration(
                                100,
                                10 * 60 * 1000,
                                0,
                                1,
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
                                false,
                                false,
                                false,
                                0),
                        null));

        return TestingDefaultExecutionGraphBuilder.newBuilder()
                .setJobGraph(jobGraph)
                .setJobMasterConfig(configuration)
                .setBlobWriter(blobWriter)
                .build();
    }

    private static final class ExecutionStageMatcher
            extends TypeSafeMatcher<List<ExecutionAttemptID>> {
        private final List<Collection<ExecutionAttemptID>> executionStages;

        private ExecutionStageMatcher(List<Collection<ExecutionAttemptID>> executionStages) {
            this.executionStages = executionStages;
        }

        @Override
        protected boolean matchesSafely(List<ExecutionAttemptID> submissionOrder) {
            final Iterator<ExecutionAttemptID> submissionIterator = submissionOrder.iterator();

            for (Collection<ExecutionAttemptID> stage : executionStages) {
                final Collection<ExecutionAttemptID> currentStage = new ArrayList<>(stage);

                while (!currentStage.isEmpty() && submissionIterator.hasNext()) {
                    if (!currentStage.remove(submissionIterator.next())) {
                        return false;
                    }
                }

                if (!currentStage.isEmpty()) {
                    return false;
                }
            }

            return !submissionIterator.hasNext();
        }

        @Override
        public void describeTo(Description description) {
            description.appendValueList("<[", ", ", "]>", executionStages);
        }
    }
}
