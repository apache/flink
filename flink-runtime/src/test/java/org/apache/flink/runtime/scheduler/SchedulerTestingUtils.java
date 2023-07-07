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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TernaryBoolean;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.finishJobVertex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** A utility class to create {@link DefaultScheduler} instances for testing. */
public class SchedulerTestingUtils {

    private static final long DEFAULT_CHECKPOINT_TIMEOUT_MS = 10 * 60 * 1000;

    private static final Time DEFAULT_TIMEOUT = Time.seconds(300);

    private SchedulerTestingUtils() {}

    public static DefaultScheduler createScheduler(
            final JobGraph jobGraph,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final ScheduledExecutorService executorService)
            throws Exception {
        return new DefaultSchedulerBuilder(jobGraph, mainThreadExecutor, executorService).build();
    }

    public static void enableCheckpointing(final JobGraph jobGraph) {
        enableCheckpointing(jobGraph, null, null);
    }

    public static void enableCheckpointing(
            final JobGraph jobGraph,
            @Nullable StateBackend stateBackend,
            @Nullable CheckpointStorage checkpointStorage) {

        final CheckpointCoordinatorConfiguration config =
                new CheckpointCoordinatorConfiguration(
                        Long.MAX_VALUE, // disable periodical checkpointing
                        DEFAULT_CHECKPOINT_TIMEOUT_MS,
                        0,
                        1,
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
                        false,
                        false,
                        0,
                        0,
                        0);

        SerializedValue<StateBackend> serializedStateBackend = null;
        if (stateBackend != null) {
            try {
                serializedStateBackend = new SerializedValue<>(stateBackend);
            } catch (IOException e) {
                throw new RuntimeException("could not serialize state backend", e);
            }
        }

        SerializedValue<CheckpointStorage> serializedCheckpointStorage = null;
        if (checkpointStorage != null) {
            try {
                serializedCheckpointStorage = new SerializedValue<>(checkpointStorage);
            } catch (IOException e) {
                throw new RuntimeException("could not serialize checkpoint storage", e);
            }
        }

        jobGraph.setSnapshotSettings(
                new JobCheckpointingSettings(
                        config,
                        serializedStateBackend,
                        TernaryBoolean.UNDEFINED,
                        serializedCheckpointStorage,
                        null));
    }

    public static Collection<ExecutionAttemptID> getAllCurrentExecutionAttempts(
            DefaultScheduler scheduler) {
        return StreamSupport.stream(
                        scheduler
                                .requestJob()
                                .getArchivedExecutionGraph()
                                .getAllExecutionVertices()
                                .spliterator(),
                        false)
                .map((vertex) -> vertex.getCurrentExecutionAttempt().getAttemptId())
                .collect(Collectors.toList());
    }

    public static ExecutionState getExecutionState(
            DefaultScheduler scheduler, JobVertexID jvid, int subtask) {
        final ExecutionJobVertex ejv = getJobVertex(scheduler, jvid);
        return ejv.getTaskVertices()[subtask].getCurrentExecutionAttempt().getState();
    }

    public static void failExecution(DefaultScheduler scheduler, JobVertexID jvid, int subtask) {
        final ExecutionAttemptID attemptID = getAttemptId(scheduler, jvid, subtask);
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        attemptID, ExecutionState.FAILED, new Exception("test task failure")));
    }

    public static void canceledExecution(
            DefaultScheduler scheduler, JobVertexID jvid, int subtask) {
        final ExecutionAttemptID attemptID = getAttemptId(scheduler, jvid, subtask);
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        attemptID, ExecutionState.CANCELED, new Exception("test task failure")));
    }

    public static void setExecutionToState(
            ExecutionState executionState,
            DefaultScheduler scheduler,
            JobVertexID jvid,
            int subtask) {
        final ExecutionAttemptID attemptID = getAttemptId(scheduler, jvid, subtask);
        scheduler.updateTaskExecutionState(new TaskExecutionState(attemptID, executionState));
    }

    public static void setAllExecutionsToRunning(final DefaultScheduler scheduler) {
        getAllCurrentExecutionAttempts(scheduler)
                .forEach(
                        (attemptId) -> {
                            scheduler.updateTaskExecutionState(
                                    new TaskExecutionState(attemptId, ExecutionState.INITIALIZING));
                            scheduler.updateTaskExecutionState(
                                    new TaskExecutionState(attemptId, ExecutionState.RUNNING));
                        });
    }

    public static void setAllExecutionsToCancelled(final DefaultScheduler scheduler) {
        for (final ExecutionAttemptID attemptId : getAllCurrentExecutionAttempts(scheduler)) {
            final boolean setToRunning =
                    scheduler.updateTaskExecutionState(
                            new TaskExecutionState(attemptId, ExecutionState.CANCELED));

            assertTrue("could not switch task to RUNNING", setToRunning);
        }
    }

    public static void acknowledgePendingCheckpoint(
            final DefaultScheduler scheduler, final long checkpointId) throws CheckpointException {
        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);
        final JobID jid = scheduler.getJobId();

        for (ExecutionAttemptID attemptId : getAllCurrentExecutionAttempts(scheduler)) {
            final AcknowledgeCheckpoint acknowledgeCheckpoint =
                    new AcknowledgeCheckpoint(jid, attemptId, checkpointId);
            checkpointCoordinator.receiveAcknowledgeMessage(
                    acknowledgeCheckpoint, "Unknown location");
        }
    }

    public static CompletableFuture<CompletedCheckpoint> triggerCheckpoint(
            DefaultScheduler scheduler) throws Exception {
        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);
        return checkpointCoordinator.triggerCheckpoint(false);
    }

    public static void acknowledgeCurrentCheckpoint(DefaultScheduler scheduler) {
        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);
        assertEquals(
                "Coordinator has not ", 1, checkpointCoordinator.getNumberOfPendingCheckpoints());

        final PendingCheckpoint pc =
                checkpointCoordinator.getPendingCheckpoints().values().iterator().next();

        // because of races against the async thread in the coordinator, we need to wait here until
        // the
        // coordinator state is acknowledged. This can be removed once the CheckpointCoordinator is
        // executes all actions in the Scheduler's main thread executor.
        while (pc.getNumberOfNonAcknowledgedOperatorCoordinators() > 0) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail("interrupted");
            }
        }

        getAllCurrentExecutionAttempts(scheduler)
                .forEach(
                        (attemptId) ->
                                scheduler.acknowledgeCheckpoint(
                                        pc.getJobId(),
                                        attemptId,
                                        pc.getCheckpointID(),
                                        new CheckpointMetrics(),
                                        null));
    }

    public static CompletedCheckpoint takeCheckpoint(DefaultScheduler scheduler) throws Exception {
        final CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(scheduler);
        checkpointCoordinator.triggerCheckpoint(false);

        assertEquals(
                "test setup inconsistent",
                1,
                checkpointCoordinator.getNumberOfPendingCheckpoints());
        final PendingCheckpoint checkpoint =
                checkpointCoordinator.getPendingCheckpoints().values().iterator().next();
        final CompletableFuture<CompletedCheckpoint> future = checkpoint.getCompletionFuture();

        acknowledgePendingCheckpoint(scheduler, checkpoint.getCheckpointID());

        CompletedCheckpoint completed = future.getNow(null);
        assertNotNull("checkpoint not complete", completed);
        return completed;
    }

    @SuppressWarnings("deprecation")
    public static CheckpointCoordinator getCheckpointCoordinator(SchedulerBase scheduler) {
        return scheduler.getCheckpointCoordinator();
    }

    private static ExecutionJobVertex getJobVertex(
            DefaultScheduler scheduler, JobVertexID jobVertexId) {
        final ExecutionVertexID id = new ExecutionVertexID(jobVertexId, 0);
        return scheduler.getExecutionVertex(id).getJobVertex();
    }

    public static ExecutionAttemptID getAttemptId(
            DefaultScheduler scheduler, JobVertexID jvid, int subtask) {
        final ExecutionJobVertex ejv = getJobVertex(scheduler, jvid);
        assert ejv != null;
        return ejv.getTaskVertices()[subtask].getCurrentExecutionAttempt().getAttemptId();
    }

    // ------------------------------------------------------------------------

    public static SlotSharingExecutionSlotAllocatorFactory
            newSlotSharingExecutionSlotAllocatorFactory() {
        return newSlotSharingExecutionSlotAllocatorFactory(
                TestingPhysicalSlotProvider.createWithInfiniteSlotCreation());
    }

    public static SlotSharingExecutionSlotAllocatorFactory
            newSlotSharingExecutionSlotAllocatorFactory(PhysicalSlotProvider physicalSlotProvider) {
        return newSlotSharingExecutionSlotAllocatorFactory(physicalSlotProvider, DEFAULT_TIMEOUT);
    }

    public static SlotSharingExecutionSlotAllocatorFactory
            newSlotSharingExecutionSlotAllocatorFactory(
                    PhysicalSlotProvider physicalSlotProvider, Time allocationTimeout) {
        return new SlotSharingExecutionSlotAllocatorFactory(
                physicalSlotProvider,
                true,
                new TestingPhysicalSlotRequestBulkChecker(),
                allocationTimeout,
                new LocalInputPreferredSlotSharingStrategy.Factory());
    }

    public static SchedulerBase createSchedulerAndDeploy(
            boolean isAdaptive,
            JobID jobId,
            JobVertex producer,
            JobVertex[] consumers,
            DistributionPattern distributionPattern,
            BlobWriter blobWriter,
            ComponentMainThreadExecutor mainThreadExecutor,
            ScheduledExecutorService ioExecutor,
            JobMasterPartitionTracker partitionTracker,
            ScheduledExecutorService scheduledExecutor,
            Configuration jobMasterConfiguration)
            throws Exception {
        final List<JobVertex> vertices = new ArrayList<>(Collections.singletonList(producer));
        IntermediateDataSetID dataSetId = new IntermediateDataSetID();
        for (JobVertex consumer : consumers) {
            consumer.connectNewDataSetAsInput(
                    producer, distributionPattern, ResultPartitionType.BLOCKING, dataSetId, false);
            vertices.add(consumer);
        }

        final SchedulerBase scheduler =
                createScheduler(
                        isAdaptive,
                        jobId,
                        vertices,
                        blobWriter,
                        mainThreadExecutor,
                        ioExecutor,
                        partitionTracker,
                        scheduledExecutor,
                        jobMasterConfiguration);
        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();
        final TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();

        CompletableFuture.runAsync(
                        () -> {
                            try {
                                if (isAdaptive) {
                                    initializeExecutionJobVertex(producer.getID(), executionGraph);
                                }
                                // Deploy upstream source vertices
                                deployTasks(executionGraph, producer.getID(), slotBuilder);
                                // Transition upstream vertices into FINISHED
                                finishJobVertex(executionGraph, producer.getID());
                                // Deploy downstream sink vertices
                                for (JobVertex consumer : consumers) {
                                    if (isAdaptive) {
                                        initializeExecutionJobVertex(
                                                consumer.getID(), executionGraph);
                                    }
                                    deployTasks(executionGraph, consumer.getID(), slotBuilder);
                                }
                            } catch (Exception e) {
                                throw new RuntimeException("Exceptions shouldn't happen here.", e);
                            }
                        },
                        mainThreadExecutor)
                .join();
        return scheduler;
    }

    private static void initializeExecutionJobVertex(
            JobVertexID jobVertex, ExecutionGraph executionGraph) {
        try {
            executionGraph.initializeJobVertex(
                    executionGraph.getJobVertex(jobVertex), System.currentTimeMillis());
            executionGraph.notifyNewlyInitializedJobVertices(
                    Collections.singletonList(executionGraph.getJobVertex(jobVertex)));
        } catch (JobException exception) {
            throw new RuntimeException(exception);
        }
    }

    private static DefaultScheduler createScheduler(
            boolean isAdaptive,
            JobID jobId,
            List<JobVertex> jobVertices,
            BlobWriter blobWriter,
            ComponentMainThreadExecutor mainThreadExecutor,
            ScheduledExecutorService ioExecutor,
            JobMasterPartitionTracker partitionTracker,
            ScheduledExecutorService scheduledExecutor,
            Configuration jobMasterConfiguration)
            throws Exception {
        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder()
                        .setJobId(jobId)
                        .addJobVertices(jobVertices)
                        .build();

        final DefaultSchedulerBuilder builder =
                new DefaultSchedulerBuilder(jobGraph, mainThreadExecutor, scheduledExecutor)
                        .setRestartBackoffTimeStrategy(new TestRestartBackoffTimeStrategy(true, 0))
                        .setBlobWriter(blobWriter)
                        .setIoExecutor(ioExecutor)
                        .setPartitionTracker(partitionTracker)
                        .setJobMasterConfiguration(jobMasterConfiguration);
        return isAdaptive ? builder.buildAdaptiveBatchJobScheduler() : builder.build();
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
            execution.registerProducedPartitions(slot.getTaskManagerLocation()).get();
            execution.transitionState(ExecutionState.SCHEDULED);

            vertex.tryAssignResource(slot);
            vertex.deploy();
        }
    }

    public static TaskExecutionState createFinishedTaskExecutionState(
            ExecutionAttemptID attemptId,
            Map<IntermediateResultPartitionID, ResultPartitionBytes> resultPartitionBytes) {
        return new TaskExecutionState(
                attemptId,
                ExecutionState.FINISHED,
                null,
                null,
                new IOMetrics(0, 0, 0, 0, 0, 0, 0, resultPartitionBytes));
    }

    public static TaskExecutionState createFinishedTaskExecutionState(
            ExecutionAttemptID attemptId) {
        return createFinishedTaskExecutionState(attemptId, Collections.emptyMap());
    }

    public static TaskExecutionState createFailedTaskExecutionState(
            ExecutionAttemptID attemptId, Throwable failureCause) {
        return new TaskExecutionState(attemptId, ExecutionState.FAILED, failureCause);
    }

    public static TaskExecutionState createFailedTaskExecutionState(ExecutionAttemptID attemptId) {
        return createFailedTaskExecutionState(attemptId, new Exception("Expected failure cause"));
    }

    public static TaskExecutionState createCanceledTaskExecutionState(
            ExecutionAttemptID attemptId) {
        return new TaskExecutionState(attemptId, ExecutionState.CANCELED);
    }
}
