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
 * limitations under the License
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.TestingBlobWriter;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor.MaybeOffloaded;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactoryTest.deserializeShuffleDescriptors;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Tests for removing cached {@link ShuffleDescriptor}s when the related partitions are no longer
 * valid. Currently there are two scenarios as illustrated in {@link
 * IntermediateResult#notifyPartitionChanged}.
 */
public class RemoveCachedShuffleDescriptorTest extends TestLogger {

    private static final int PARALLELISM = 4;

    private ScheduledExecutorService scheduledExecutorService;
    private ComponentMainThreadExecutor mainThreadExecutor;
    private ManuallyTriggeredScheduledExecutorService ioExecutor;

    @Before
    public void setup() {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        scheduledExecutorService);
        ioExecutor = new ManuallyTriggeredScheduledExecutorService();
    }

    @After
    public void teardown() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }

    @Test
    public void testRemoveNonOffloadedShuffleDescriptorCacheAfterFinished() throws Exception {
        testRemoveShuffleDescriptorCacheAfterFinished(
                new TestingBlobWriter(Integer.MAX_VALUE), 0, 0);
    }

    @Test
    public void testRemoveOffloadedShuffleDescriptorCacheAfterFinished() throws Exception {
        testRemoveShuffleDescriptorCacheAfterFinished(new TestingBlobWriter(0), 4, 3);
    }

    private void testRemoveShuffleDescriptorCacheAfterFinished(
            TestingBlobWriter blobWriter, int expectedBefore, int expectedAfter) throws Exception {
        final JobID jobId = new JobID();

        final JobVertex v1 = createJobVertex("v1", PARALLELISM);
        final JobVertex v2 = createJobVertex("v2", PARALLELISM);

        final DefaultScheduler scheduler =
                createSchedulerAndDeploy(jobId, v1, v2, blobWriter, mainThreadExecutor, ioExecutor);
        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        // ShuffleDescriptors should be cached during the deployment
        final ShuffleDescriptor[] shuffleDescriptors =
                deserializeShuffleDescriptors(
                        getConsumedCachedShuffleDescriptor(executionGraph, v2), jobId, blobWriter);
        assertEquals(PARALLELISM, shuffleDescriptors.length);
        assertEquals(expectedBefore, blobWriter.numberOfBlobs());

        CompletableFuture.runAsync(
                        () -> transitionTasksToFinished(executionGraph, v2.getID()),
                        mainThreadExecutor)
                .join();
        ioExecutor.triggerAll();

        // Cache should be removed when partitions are released
        assertNull(getConsumedCachedShuffleDescriptor(executionGraph, v2));
        assertEquals(expectedAfter, blobWriter.numberOfBlobs());
    }

    @Test
    public void testNonOffloadedCacheRemovedCorrectlyAfterFailover() throws Exception {
        testCacheRemovedCorrectlyAfterFailover(new TestingBlobWriter(Integer.MAX_VALUE), 0, 0);
    }

    @Test
    public void testOffloadedCacheRemovedCorrectlyAfterFailover() throws Exception {
        testCacheRemovedCorrectlyAfterFailover(new TestingBlobWriter(0), 4, 3);
    }

    private void testCacheRemovedCorrectlyAfterFailover(
            TestingBlobWriter blobWriter, int expectedBefore, int expectedAfter) throws Exception {
        final JobID jobId = new JobID();

        final JobVertex v1 = createJobVertex("v1", PARALLELISM);
        final JobVertex v2 = createJobVertex("v2", PARALLELISM);

        final DefaultScheduler scheduler =
                createSchedulerAndDeploy(jobId, v1, v2, blobWriter, mainThreadExecutor, ioExecutor);
        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        // ShuffleDescriptors should be cached during the deployment
        final ShuffleDescriptor[] shuffleDescriptors =
                deserializeShuffleDescriptors(
                        getConsumedCachedShuffleDescriptor(executionGraph, v2), jobId, blobWriter);
        assertEquals(PARALLELISM, shuffleDescriptors.length);
        assertEquals(expectedBefore, blobWriter.numberOfBlobs());

        triggerExceptionAndComplete(scheduler, v1);
        ioExecutor.triggerAll();

        // Cache should be removed during ExecutionVertex#resetForNewExecution
        assertNull(getConsumedCachedShuffleDescriptor(executionGraph, v2));
        assertEquals(expectedAfter, blobWriter.numberOfBlobs());
    }

    private static DefaultScheduler createSchedulerAndDeploy(
            JobID jobId,
            JobVertex v1,
            JobVertex v2,
            BlobWriter blobWriter,
            ComponentMainThreadExecutor mainThreadExecutor,
            ScheduledExecutorService ioExecutor)
            throws Exception {

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        final List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));
        final DefaultScheduler scheduler =
                createScheduler(jobId, ordered, blobWriter, mainThreadExecutor, ioExecutor);
        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();
        final TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();

        CompletableFuture.runAsync(
                        () -> {
                            try {
                                // Deploy upstream source vertices
                                deployTasks(executionGraph, v1.getID(), slotBuilder);
                                // Transition upstream vertices into FINISHED
                                transitionTasksToFinished(executionGraph, v1.getID());
                                // Deploy downstream sink vertices
                                deployTasks(executionGraph, v2.getID(), slotBuilder);
                            } catch (Exception e) {
                                throw new RuntimeException("Exceptions shouldn't happen here.", e);
                            }
                        },
                        mainThreadExecutor)
                .join();

        return scheduler;
    }

    private void triggerExceptionAndComplete(DefaultScheduler scheduler, JobVertex upstream)
            throws TimeoutException {

        final Throwable t = new Exception();
        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        CompletableFuture.runAsync(
                        () -> {
                            // Trigger a failover
                            scheduler.handleGlobalFailure(t);
                            // Finish the cancellation of downstream tasks and restart tasks
                            for (ExecutionVertex ev : executionGraph.getAllExecutionVertices()) {
                                ev.getCurrentExecutionAttempt().completeCancelling();
                            }
                        },
                        mainThreadExecutor)
                .join();

        // Wait until all the upstream vertices finish restarting
        for (ExecutionVertex ev :
                Objects.requireNonNull(executionGraph.getJobVertex(upstream.getID()))
                        .getTaskVertices()) {
            ExecutionGraphTestUtils.waitUntilExecutionVertexState(
                    ev, ExecutionState.DEPLOYING, 1000);
        }
    }

    // ============== Utils ==============

    private static JobVertex createJobVertex(String vertexName, int parallelism) {
        JobVertex jobVertex = new JobVertex(vertexName);
        jobVertex.setParallelism(parallelism);
        jobVertex.setInvokableClass(AbstractInvokable.class);
        return jobVertex;
    }

    private static DefaultScheduler createScheduler(
            final JobID jobId,
            final List<JobVertex> jobVertices,
            final BlobWriter blobWriter,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final ScheduledExecutorService ioExecutor)
            throws Exception {
        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder()
                        .setJobId(jobId)
                        .addJobVertices(jobVertices)
                        .build();

        return SchedulerTestingUtils.newSchedulerBuilder(jobGraph, mainThreadExecutor)
                .setRestartBackoffTimeStrategy(new TestRestartBackoffTimeStrategy(true, 0))
                .setBlobWriter(blobWriter)
                .setIoExecutor(ioExecutor)
                .build();
    }

    private static void deployTasks(
            ExecutionGraph executionGraph,
            JobVertexID jobVertexID,
            TestingLogicalSlotBuilder slotBuilder)
            throws JobException, ExecutionException, InterruptedException {

        for (ExecutionVertex vertex :
                Objects.requireNonNull(executionGraph.getJobVertex(jobVertexID))
                        .getTaskVertices()) {
            LogicalSlot slot = slotBuilder.createTestingLogicalSlot();

            Execution execution = vertex.getCurrentExecutionAttempt();
            execution.registerProducedPartitions(slot.getTaskManagerLocation(), true).get();

            vertex.tryAssignResource(slot);
            vertex.deploy();
        }
    }

    private static void transitionTasksToFinished(
            ExecutionGraph executionGraph, JobVertexID jobVertexID) {

        for (ExecutionVertex vertex :
                Objects.requireNonNull(executionGraph.getJobVertex(jobVertexID))
                        .getTaskVertices()) {
            executionGraph.updateState(
                    new TaskExecutionStateTransition(
                            new TaskExecutionState(
                                    vertex.getCurrentExecutionAttempt().getAttemptId(),
                                    ExecutionState.FINISHED)));
        }
    }

    private static MaybeOffloaded<ShuffleDescriptor[]> getConsumedCachedShuffleDescriptor(
            ExecutionGraph executionGraph, JobVertex vertex) {

        final ExecutionJobVertex ejv = executionGraph.getJobVertex(vertex.getID());
        final List<IntermediateResult> consumedResults = Objects.requireNonNull(ejv).getInputs();
        final IntermediateResult consumedResult = consumedResults.get(0);

        return consumedResult.getCachedShuffleDescriptors(
                ejv.getTaskVertices()[0].getConsumedPartitionGroup(0));
    }
}
