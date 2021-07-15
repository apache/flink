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

package org.apache.flink.runtime.deployment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
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
import org.apache.flink.util.CompressedSerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Tests for {@link TaskDeploymentDescriptorFactory}. */
public class TaskDeploymentDescriptorFactoryTest extends TestLogger {

    private static final int PARALLELISM = 4;

    public ExecutionJobVertex setupExecutionGraph(JobID jobId, BlobWriter blobWriter)
            throws JobException, JobExecutionException {
        final JobVertex v1 = createJobVertex("v1", PARALLELISM);
        final JobVertex v2 = createJobVertex("v2", PARALLELISM);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        final List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));

        final ExecutionGraph executionGraph =
                createExecutionGraphDirectly(jobId, ordered, blobWriter);

        return executionGraph.getJobVertex(v2.getID());
    }

    @Test
    public void testCacheShuffleDescriptorAsNonOffloaded() throws Exception {
        final ExecutionJobVertex ejv = setupExecutionGraph(new JobID(), new VoidBlobWriter());

        assert ejv != null;
        final IntermediateResult consumedResult = ejv.getInputs().get(0);

        final ExecutionVertex ev21 = ejv.getTaskVertices()[0];
        final InputGateDeploymentDescriptor igdd21 = createTaskDeploymentDescriptorAndGet(ev21);
        final ShuffleDescriptor[] shuffleDescriptors = igdd21.getShuffleDescriptors();

        // Test whether ShuffleDescriptors are cached or not
        final CompressedSerializedValue<ShuffleDescriptor[]> compressedSerializedValue =
                consumedResult.getCachedShuffleDescriptors(ev21.getConsumedPartitionGroup(0));

        final ShuffleDescriptor[] cachedShuffleDescriptors =
                deserializeShuffleDescriptors(compressedSerializedValue);

        assertEquals(shuffleDescriptors.length, cachedShuffleDescriptors.length);
        for (int i = 0; i < cachedShuffleDescriptors.length; i++) {
            assertEquals(
                    shuffleDescriptors[i].getResultPartitionID(),
                    cachedShuffleDescriptors[i].getResultPartitionID());
        }

        // Test whether the following TaskDeploymentDescriptor uses cached ShuffleDescriptors or not
        final ExecutionVertex ev22 = ejv.getTaskVertices()[1];
        final InputGateDeploymentDescriptor igdd22 = createTaskDeploymentDescriptorAndGet(ev22);
        final ShuffleDescriptor[] otherShuffleDescriptors = igdd22.getShuffleDescriptors();

        assertEquals(shuffleDescriptors.length, otherShuffleDescriptors.length);
        for (int i = 0; i < shuffleDescriptors.length; i++) {
            assertEquals(
                    shuffleDescriptors[i].getResultPartitionID(),
                    otherShuffleDescriptors[i].getResultPartitionID());
        }
    }

    @Test
    public void testRemoveShuffleDescriptorCacheAfterFinished() throws Exception {

        final JobVertex v1 = createJobVertex("v1", PARALLELISM);
        final JobVertex v2 = createJobVertex("v2", PARALLELISM);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        final List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));

        final ComponentMainThreadExecutor mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forMainThread();

        final DefaultScheduler scheduler = createScheduler(ordered, mainThreadExecutor);

        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        final TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();

        deployTasks(executionGraph, v1.getID(), slotBuilder);

        transitionTasksToFinished(executionGraph, v1.getID());

        deployTasks(executionGraph, v2.getID(), slotBuilder);

        // ShuffleDescriptors will be cached during the deployment
        final ShuffleDescriptor[] shuffleDescriptors =
                deserializeShuffleDescriptors(getCachedShuffleDescriptor(executionGraph, v2));
        assertEquals(PARALLELISM, shuffleDescriptors.length);

        transitionTasksToFinished(executionGraph, v2.getID());

        // Cache will be removed when partitions are released
        assertNull(getCachedShuffleDescriptor(executionGraph, v2));
    }

    @Test
    public void testCacheRemovedCorrectlyAfterFailover() throws Exception {
        final JobVertex v1 = createJobVertex("v1", PARALLELISM);
        final JobVertex v2 = createJobVertex("v2", PARALLELISM);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        final List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));

        final ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor();

        final ComponentMainThreadExecutor mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        scheduledExecutorService);

        final DefaultScheduler scheduler = createScheduler(ordered, mainThreadExecutor);

        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        final TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();

        // Deploy upstream source vertices
        CompletableFuture.runAsync(
                        () -> {
                            try {
                                deployTasks(executionGraph, v1.getID(), slotBuilder);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        },
                        mainThreadExecutor)
                .join();

        // Transition upstream vertices into FINISHED
        CompletableFuture.runAsync(
                        () -> transitionTasksToFinished(executionGraph, v1.getID()),
                        mainThreadExecutor)
                .join();

        // Deploy downstream sink vertices
        CompletableFuture.runAsync(
                        () -> {
                            try {
                                deployTasks(executionGraph, v2.getID(), slotBuilder);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        },
                        mainThreadExecutor)
                .join();

        // ShuffleDescriptors are cached
        final ShuffleDescriptor[] shuffleDescriptors =
                deserializeShuffleDescriptors(getCachedShuffleDescriptor(executionGraph, v2));
        assertEquals(PARALLELISM, shuffleDescriptors.length);

        // Generate a PartitionNotFoundException
        final ExecutionVertex ev21 = executionGraph.getJobVertex(v2.getID()).getTaskVertices()[0];
        final IntermediateResultPartitionID consumedPartitionId =
                ev21.getConsumedPartitionGroup(0).getFirst();
        final Throwable t =
                new PartitionNotFoundException(
                        new ResultPartitionID(consumedPartitionId, new ExecutionAttemptID()));

        // Pass the exception to downstream tasks
        CompletableFuture.runAsync(
                        () -> transitionTaskToFailed(scheduler, v2.getID(), 1, t),
                        mainThreadExecutor)
                .join();

        // Finish the cancellation of downstream tasks, calling DefaultScheduler#restartTasks
        CompletableFuture.runAsync(
                        () -> {
                            for (ExecutionVertex ev : executionGraph.getAllExecutionVertices()) {
                                ev.markFailed(t);
                            }
                        },
                        mainThreadExecutor)
                .join();

        // Wait until the procedure finishes
        ExecutionVertex ev11 = executionGraph.getJobVertex(v1.getID()).getTaskVertices()[0];
        ExecutionGraphTestUtils.waitUntilExecutionVertexState(ev11, ExecutionState.DEPLOYING, 1000);

        // Cache will be removed during ExecutionVertex#resetForNewExecution
        assertNull(getCachedShuffleDescriptor(executionGraph, v2));

        scheduledExecutorService.shutdownNow();
    }

    private static JobVertex createJobVertex(String vertexName, int parallelism) {
        JobVertex jobVertex = new JobVertex(vertexName);
        jobVertex.setParallelism(parallelism);
        jobVertex.setInvokableClass(AbstractInvokable.class);
        return jobVertex;
    }

    private static ExecutionGraph createExecutionGraphDirectly(
            final JobID jobId, final List<JobVertex> jobVertices, final BlobWriter blobWriter)
            throws JobException, JobExecutionException {

        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder()
                        .setJobId(jobId)
                        .addJobVertices(jobVertices)
                        .build();

        return TestingDefaultExecutionGraphBuilder.newBuilder()
                .setJobGraph(jobGraph)
                .setBlobWriter(blobWriter)
                .build();
    }

    private static DefaultScheduler createScheduler(
            final List<JobVertex> jobVertices, final ComponentMainThreadExecutor mainThreadExecutor)
            throws Exception {
        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder().addJobVertices(jobVertices).build();

        return SchedulerTestingUtils.newSchedulerBuilder(jobGraph, mainThreadExecutor)
                .setRestartBackoffTimeStrategy(new TestRestartBackoffTimeStrategy(true, 0))
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

    private static void transitionTaskToFailed(
            DefaultScheduler scheduler, JobVertexID jobVertexID, int taskNum, Throwable cause) {

        final ExecutionVertex vertex =
                Objects.requireNonNull(scheduler.getExecutionGraph().getJobVertex(jobVertexID))
                        .getTaskVertices()[taskNum];
        scheduler.updateTaskExecutionState(
                new TaskExecutionStateTransition(
                        new TaskExecutionState(
                                vertex.getCurrentExecutionAttempt().getAttemptId(),
                                ExecutionState.FAILED,
                                cause)));
    }

    private static InputGateDeploymentDescriptor createTaskDeploymentDescriptorAndGet(
            ExecutionVertex ev) throws IOException {
        final TaskDeploymentDescriptorFactory factory =
                TaskDeploymentDescriptorFactory.fromExecutionVertex(ev, 0);

        final TaskDeploymentDescriptor descriptor =
                factory.createDeploymentDescriptor(
                        new AllocationID(), null, Collections.emptyList());

        final List<InputGateDeploymentDescriptor> inputGates = descriptor.getInputGates();
        assertEquals(1, inputGates.size());

        return inputGates.get(0);
    }

    private static CompressedSerializedValue<ShuffleDescriptor[]> getCachedShuffleDescriptor(
            ExecutionGraph executionGraph, JobVertex vertex) {
        final ExecutionJobVertex ejv = executionGraph.getJobVertex(vertex.getID());

        final List<IntermediateResult> consumedResults = Objects.requireNonNull(ejv).getInputs();

        final IntermediateResult consumedResult = consumedResults.get(0);

        return consumedResult.getCachedShuffleDescriptors(
                ejv.getTaskVertices()[0].getConsumedPartitionGroup(0));
    }

    private static ShuffleDescriptor[] deserializeShuffleDescriptors(
            CompressedSerializedValue<ShuffleDescriptor[]> compressedSerializedValue)
            throws IOException, ClassNotFoundException {

        return compressedSerializedValue.deserializeValue(ClassLoader.getSystemClassLoader());
    }
}
