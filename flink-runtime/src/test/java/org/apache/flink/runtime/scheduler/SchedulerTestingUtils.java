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
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.NoRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.DefaultExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleTestUtils;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.taskexecutor.TaskExecutorOperatorEventGateway;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testutils.TestingUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TernaryBoolean;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** A utility class to create {@link DefaultScheduler} instances for testing. */
public class SchedulerTestingUtils {

    private static final Logger LOG = LoggerFactory.getLogger(SchedulerTestingUtils.class);

    private static final long DEFAULT_CHECKPOINT_TIMEOUT_MS = 10 * 60 * 1000;

    private static final Time DEFAULT_TIMEOUT = Time.seconds(300);

    private SchedulerTestingUtils() {}

    public static DefaultSchedulerBuilder newSchedulerBuilder(
            final JobGraph jobGraph, final ComponentMainThreadExecutor mainThreadExecutor) {
        return new DefaultSchedulerBuilder(jobGraph, mainThreadExecutor);
    }

    public static DefaultScheduler createScheduler(
            final JobGraph jobGraph, final ComponentMainThreadExecutor mainThreadExecutor)
            throws Exception {
        return newSchedulerBuilder(jobGraph, mainThreadExecutor).build();
    }

    public static DefaultSchedulerBuilder createSchedulerBuilder(
            JobGraph jobGraph, ComponentMainThreadExecutor mainThreadExecutor) {

        return createSchedulerBuilder(
                jobGraph, mainThreadExecutor, new SimpleAckingTaskManagerGateway());
    }

    public static DefaultSchedulerBuilder createSchedulerBuilder(
            JobGraph jobGraph,
            ComponentMainThreadExecutor mainThreadExecutor,
            TaskExecutorOperatorEventGateway operatorEventGateway) {

        final TaskManagerGateway gateway =
                operatorEventGateway instanceof TaskManagerGateway
                        ? (TaskManagerGateway) operatorEventGateway
                        : new TaskExecutorOperatorEventGatewayAdapter(operatorEventGateway);

        return createSchedulerBuilder(jobGraph, mainThreadExecutor, gateway);
    }

    public static DefaultSchedulerBuilder createSchedulerBuilder(
            JobGraph jobGraph,
            ComponentMainThreadExecutor mainThreadExecutor,
            TaskManagerGateway taskManagerGateway) {

        return newSchedulerBuilder(jobGraph, mainThreadExecutor)
                .setSchedulingStrategyFactory(new PipelinedRegionSchedulingStrategy.Factory())
                .setRestartBackoffTimeStrategy(new TestRestartBackoffTimeStrategy(true, 0))
                .setExecutionSlotAllocatorFactory(
                        new TestExecutionSlotAllocatorFactory(taskManagerGateway));
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
                                        pc.getCheckpointId(),
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

        acknowledgePendingCheckpoint(scheduler, checkpoint.getCheckpointId());

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

    private static final class TaskExecutorOperatorEventGatewayAdapter
            extends SimpleAckingTaskManagerGateway {

        private final TaskExecutorOperatorEventGateway operatorGateway;

        TaskExecutorOperatorEventGatewayAdapter(TaskExecutorOperatorEventGateway operatorGateway) {
            this.operatorGateway = operatorGateway;
        }

        @Override
        public CompletableFuture<Acknowledge> sendOperatorEventToTask(
                ExecutionAttemptID task, OperatorID operator, SerializedValue<OperatorEvent> evt) {
            return operatorGateway.sendOperatorEventToTask(task, operator, evt);
        }
    }

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

    /** Builder for {@link DefaultScheduler}. */
    public static class DefaultSchedulerBuilder {
        private final JobGraph jobGraph;

        private final ComponentMainThreadExecutor mainThreadExecutor;

        private SchedulingStrategyFactory schedulingStrategyFactory =
                new PipelinedRegionSchedulingStrategy.Factory();

        private Logger log = LOG;
        private Executor ioExecutor = TestingUtils.defaultExecutor();
        private Configuration jobMasterConfiguration = new Configuration();
        private ScheduledExecutorService futureExecutor = TestingUtils.defaultExecutor();
        private ScheduledExecutor delayExecutor =
                new ScheduledExecutorServiceAdapter(futureExecutor);
        private ClassLoader userCodeLoader = ClassLoader.getSystemClassLoader();
        private CheckpointsCleaner checkpointCleaner = new CheckpointsCleaner();
        private CheckpointRecoveryFactory checkpointRecoveryFactory =
                new StandaloneCheckpointRecoveryFactory();
        private Time rpcTimeout = DEFAULT_TIMEOUT;
        private BlobWriter blobWriter = VoidBlobWriter.getInstance();
        private JobManagerJobMetricGroup jobManagerJobMetricGroup =
                UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup();
        private ShuffleMaster<?> shuffleMaster = ShuffleTestUtils.DEFAULT_SHUFFLE_MASTER;
        private JobMasterPartitionTracker partitionTracker = NoOpJobMasterPartitionTracker.INSTANCE;
        private FailoverStrategy.Factory failoverStrategyFactory =
                new RestartPipelinedRegionFailoverStrategy.Factory();
        private RestartBackoffTimeStrategy restartBackoffTimeStrategy =
                NoRestartBackoffTimeStrategy.INSTANCE;
        private ExecutionVertexOperations executionVertexOperations =
                new DefaultExecutionVertexOperations();
        private ExecutionVertexVersioner executionVertexVersioner = new ExecutionVertexVersioner();
        private ExecutionSlotAllocatorFactory executionSlotAllocatorFactory =
                new TestExecutionSlotAllocatorFactory();
        private JobStatusListener jobStatusListener =
                (ignoredA, ignoredB, ignoredC, ignoredD) -> {};

        public DefaultSchedulerBuilder(
                final JobGraph jobGraph, ComponentMainThreadExecutor mainThreadExecutor) {
            this.jobGraph = jobGraph;
            this.mainThreadExecutor = mainThreadExecutor;
        }

        public DefaultSchedulerBuilder setLogger(final Logger log) {
            this.log = log;
            return this;
        }

        public DefaultSchedulerBuilder setIoExecutor(final Executor ioExecutor) {
            this.ioExecutor = ioExecutor;
            return this;
        }

        public DefaultSchedulerBuilder setJobMasterConfiguration(
                final Configuration jobMasterConfiguration) {
            this.jobMasterConfiguration = jobMasterConfiguration;
            return this;
        }

        public DefaultSchedulerBuilder setFutureExecutor(
                final ScheduledExecutorService futureExecutor) {
            this.futureExecutor = futureExecutor;
            return this;
        }

        public DefaultSchedulerBuilder setDelayExecutor(final ScheduledExecutor delayExecutor) {
            this.delayExecutor = delayExecutor;
            return this;
        }

        public DefaultSchedulerBuilder setUserCodeLoader(final ClassLoader userCodeLoader) {
            this.userCodeLoader = userCodeLoader;
            return this;
        }

        public DefaultSchedulerBuilder setCheckpointCleaner(
                final CheckpointsCleaner checkpointsCleaner) {
            this.checkpointCleaner = checkpointsCleaner;
            return this;
        }

        public DefaultSchedulerBuilder setCheckpointRecoveryFactory(
                final CheckpointRecoveryFactory checkpointRecoveryFactory) {
            this.checkpointRecoveryFactory = checkpointRecoveryFactory;
            return this;
        }

        public DefaultSchedulerBuilder setRpcTimeout(final Time rpcTimeout) {
            this.rpcTimeout = rpcTimeout;
            return this;
        }

        public DefaultSchedulerBuilder setBlobWriter(final BlobWriter blobWriter) {
            this.blobWriter = blobWriter;
            return this;
        }

        public DefaultSchedulerBuilder setJobManagerJobMetricGroup(
                final JobManagerJobMetricGroup jobManagerJobMetricGroup) {
            this.jobManagerJobMetricGroup = jobManagerJobMetricGroup;
            return this;
        }

        public DefaultSchedulerBuilder setShuffleMaster(final ShuffleMaster<?> shuffleMaster) {
            this.shuffleMaster = shuffleMaster;
            return this;
        }

        public DefaultSchedulerBuilder setPartitionTracker(
                final JobMasterPartitionTracker partitionTracker) {
            this.partitionTracker = partitionTracker;
            return this;
        }

        public DefaultSchedulerBuilder setSchedulingStrategyFactory(
                final SchedulingStrategyFactory schedulingStrategyFactory) {
            this.schedulingStrategyFactory = schedulingStrategyFactory;
            return this;
        }

        public DefaultSchedulerBuilder setFailoverStrategyFactory(
                final FailoverStrategy.Factory failoverStrategyFactory) {
            this.failoverStrategyFactory = failoverStrategyFactory;
            return this;
        }

        public DefaultSchedulerBuilder setRestartBackoffTimeStrategy(
                final RestartBackoffTimeStrategy restartBackoffTimeStrategy) {
            this.restartBackoffTimeStrategy = restartBackoffTimeStrategy;
            return this;
        }

        public DefaultSchedulerBuilder setExecutionVertexOperations(
                final ExecutionVertexOperations executionVertexOperations) {
            this.executionVertexOperations = executionVertexOperations;
            return this;
        }

        public DefaultSchedulerBuilder setExecutionVertexVersioner(
                final ExecutionVertexVersioner executionVertexVersioner) {
            this.executionVertexVersioner = executionVertexVersioner;
            return this;
        }

        public DefaultSchedulerBuilder setExecutionSlotAllocatorFactory(
                final ExecutionSlotAllocatorFactory executionSlotAllocatorFactory) {
            this.executionSlotAllocatorFactory = executionSlotAllocatorFactory;
            return this;
        }

        public DefaultSchedulerBuilder setJobStatusListener(JobStatusListener jobStatusListener) {
            this.jobStatusListener = jobStatusListener;
            return this;
        }

        public DefaultScheduler build() throws Exception {
            final ExecutionGraphFactory executionGraphFactory =
                    new DefaultExecutionGraphFactory(
                            jobMasterConfiguration,
                            userCodeLoader,
                            new DefaultExecutionDeploymentTracker(),
                            futureExecutor,
                            ioExecutor,
                            rpcTimeout,
                            jobManagerJobMetricGroup,
                            blobWriter,
                            shuffleMaster,
                            partitionTracker);

            return new DefaultScheduler(
                    log,
                    jobGraph,
                    ioExecutor,
                    jobMasterConfiguration,
                    componentMainThreadExecutor -> {},
                    delayExecutor,
                    userCodeLoader,
                    checkpointCleaner,
                    checkpointRecoveryFactory,
                    jobManagerJobMetricGroup,
                    schedulingStrategyFactory,
                    failoverStrategyFactory,
                    restartBackoffTimeStrategy,
                    executionVertexOperations,
                    executionVertexVersioner,
                    executionSlotAllocatorFactory,
                    System.currentTimeMillis(),
                    mainThreadExecutor,
                    jobStatusListener,
                    executionGraphFactory,
                    shuffleMaster,
                    rpcTimeout);
        }
    }
}
