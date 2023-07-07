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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for checkpoint coordinator triggering. */
class CheckpointCoordinatorTriggeringTest extends TestLogger {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private static final String TASK_MANAGER_LOCATION_INFO = "Unknown location";

    private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor;

    @TempDir private java.nio.file.Path temporaryFolder;

    @BeforeEach
    void setUp() {
        manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();
    }

    @Test
    void testPeriodicTriggering() {
        try {
            final long start = System.currentTimeMillis();

            CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                    new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

            JobVertexID jobVertexID = new JobVertexID();
            ExecutionGraph graph =
                    new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                            .addJobVertex(jobVertexID)
                            .setTaskManagerGateway(gateway)
                            .build(EXECUTOR_RESOURCE.getExecutor());

            ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
            ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

            CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                    new CheckpointCoordinatorConfigurationBuilder()
                            .setCheckpointInterval(10) // periodic interval is 10 ms
                            .setCheckpointTimeout(200000) // timeout is very long (200 s)
                            .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                            .build();
            CheckpointCoordinator checkpointCoordinator =
                    new CheckpointCoordinatorBuilder()
                            .setCheckpointCoordinatorConfiguration(
                                    checkpointCoordinatorConfiguration)
                            .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                            .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                            .build(graph);

            checkpointCoordinator.startCheckpointScheduler();

            for (int i = 0; i < 5; ++i) {
                manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
                manuallyTriggeredScheduledExecutor.triggerAll();
            }
            checkRecordedTriggeredCheckpoints(5, start, gateway.getTriggeredCheckpoints(attemptID));

            checkpointCoordinator.stopCheckpointScheduler();

            // no further calls may come.
            manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertThat(gateway.getTriggeredCheckpoints(attemptID).size()).isEqualTo(5);

            // start another sequence of periodic scheduling
            gateway.resetCount();
            checkpointCoordinator.startCheckpointScheduler();

            for (int i = 0; i < 5; ++i) {
                manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
                manuallyTriggeredScheduledExecutor.triggerAll();
            }
            checkRecordedTriggeredCheckpoints(5, start, gateway.getTriggeredCheckpoints(attemptID));

            checkpointCoordinator.stopCheckpointScheduler();

            // no further calls may come
            manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertThat(gateway.getTriggeredCheckpoints(attemptID).size()).isEqualTo(5);

            checkpointCoordinator.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testPeriodicFlushing() {
        try {
            final long start = System.currentTimeMillis();

            CheckpointCoordinatorTestingUtils.FlushEventRecorderTaskManagerGateway gateway =
                    new CheckpointCoordinatorTestingUtils.FlushEventRecorderTaskManagerGateway();

            JobVertexID jobVertexID = new JobVertexID();
            ExecutionGraph graph =
                    new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                            .addJobVertex(jobVertexID)
                            .setTaskManagerGateway(gateway)
                            .build(EXECUTOR_RESOURCE.getExecutor());

            ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
            ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

            CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                    new CheckpointCoordinatorConfigurationBuilder()
                            .setCheckpointInterval(Long.MAX_VALUE)
                            .setAlignedCheckpointTimeout(Long.MAX_VALUE)
                            .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                            .setAllowedLatency(10) // periodic interval is 10 ms
                            .build();

            CheckpointCoordinator checkpointCoordinator =
                    new CheckpointCoordinatorBuilder()
                            .setCheckpointCoordinatorConfiguration(
                                    checkpointCoordinatorConfiguration)
                            .setFlushEventTimer(manuallyTriggeredScheduledExecutor)
                            .build(graph);

            checkpointCoordinator.startFlushEventScheduler();

            for (int i = 0; i < 5; ++i) {
                manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
                manuallyTriggeredScheduledExecutor.triggerAll();
            }
            checkRecordedTriggeredFlushEvents(5, start, gateway.getTriggeredFlushEvents(attemptID));

            checkpointCoordinator.stopFlushEventScheduler();

            // no further calls may come.
            manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertThat(gateway.getTriggeredFlushEvents(attemptID).size()).isEqualTo(5);

            // start another sequence of periodic scheduling
            gateway.resetCount();
            checkpointCoordinator.startFlushEventScheduler();

            for (int i = 0; i < 5; ++i) {
                manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
                manuallyTriggeredScheduledExecutor.triggerAll();
            }
            checkRecordedTriggeredFlushEvents(5, start, gateway.getTriggeredFlushEvents(attemptID));

            checkpointCoordinator.stopFlushEventScheduler();

            // no further calls may come
            manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertThat(gateway.getTriggeredFlushEvents(attemptID).size()).isEqualTo(5);

            checkpointCoordinator.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    private void checkRecordedTriggeredCheckpoints(
            int numTrigger,
            long start,
            List<CheckpointCoordinatorTestingUtils.TriggeredCheckpoint> checkpoints) {
        assertThat(checkpoints).hasSize(numTrigger);

        long lastId = -1;
        long lastTs = -1;

        for (CheckpointCoordinatorTestingUtils.TriggeredCheckpoint checkpoint : checkpoints) {
            assertThat(checkpoint.checkpointId)
                    .as("Trigger checkpoint id should be in increase order")
                    .isGreaterThan(lastId);
            assertThat(checkpoint.timestamp)
                    .as("Trigger checkpoint timestamp should be in increase order")
                    .isGreaterThanOrEqualTo(lastTs);
            assertThat(checkpoint.timestamp)
                    .as("Trigger checkpoint timestamp should be larger than the start time")
                    .isGreaterThanOrEqualTo(start);

            lastId = checkpoint.checkpointId;
            lastTs = checkpoint.timestamp;
        }
    }

    private void checkRecordedTriggeredFlushEvents(
            int numTrigger,
            long start,
            List<CheckpointCoordinatorTestingUtils.TriggeredFlushEvent> flushEvents) {
        assertThat(flushEvents).hasSize(numTrigger);

        long lastId = -1;
        long lastTs = -1;

        for (CheckpointCoordinatorTestingUtils.TriggeredFlushEvent flushEvent : flushEvents) {
            assertThat(flushEvent.flushEventID)
                    .as("Trigger checkpoint id should be in increase order")
                    .isGreaterThan(lastId);
            assertThat(flushEvent.timestamp)
                    .as("Trigger checkpoint timestamp should be in increase order")
                    .isGreaterThanOrEqualTo(lastTs);
            assertThat(flushEvent.timestamp)
                    .as("Trigger checkpoint timestamp should be larger than the start time")
                    .isGreaterThanOrEqualTo(start);

            lastId = flushEvent.flushEventID;
            lastTs = flushEvent.timestamp;
        }
    }

    @Test
    void testTriggeringFullSnapshotAfterJobmasterFailover() throws Exception {
        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        // create a savepoint, we can restore from later
        final CompletedCheckpoint savepoint = takeSavepoint(graph, attemptID);

        // restore from a savepoint in NO_CLAIM mode
        final StandaloneCompletedCheckpointStore checkpointStore =
                new StandaloneCompletedCheckpointStore(1);
        final StandaloneCheckpointIDCounter checkpointIDCounter =
                new StandaloneCheckpointIDCounter();
        CheckpointCoordinator checkpointCoordinator =
                createCheckpointCoordinator(graph, checkpointStore, checkpointIDCounter);
        checkpointCoordinator.restoreSavepoint(
                SavepointRestoreSettings.forPath(
                        savepoint.getExternalPointer(), true, RestoreMode.NO_CLAIM),
                graph.getAllVertices(),
                this.getClass().getClassLoader());
        checkpointCoordinator.shutdown();

        // imitate job manager failover
        gateway.resetCount();
        checkpointCoordinator =
                createCheckpointCoordinator(graph, checkpointStore, checkpointIDCounter);
        checkpointCoordinator.restoreLatestCheckpointedStateToAll(
                new HashSet<>(graph.getAllVertices().values()), true);
        checkpointCoordinator.startCheckpointScheduler();
        final CompletableFuture<CompletedCheckpoint> checkpoint =
                checkpointCoordinator.triggerCheckpoint(true);
        manuallyTriggeredScheduledExecutor.triggerAll();
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID, 2),
                TASK_MANAGER_LOCATION_INFO);
        checkpoint.get();

        assertThat(
                        gateway.getOnlyTriggeredCheckpoint(attemptID)
                                .checkpointOptions
                                .getCheckpointType())
                .isEqualTo(CheckpointType.FULL_CHECKPOINT);
    }

    @Test
    void testTriggeringFullCheckpoints() throws Exception {
        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        // create a savepoint, we can restore from later
        final CompletedCheckpoint savepoint = takeSavepoint(graph, attemptID);

        // restore from a savepoint in NO_CLAIM mode
        final StandaloneCompletedCheckpointStore checkpointStore =
                new StandaloneCompletedCheckpointStore(1);
        final StandaloneCheckpointIDCounter checkpointIDCounter =
                new StandaloneCheckpointIDCounter();
        CheckpointCoordinator checkpointCoordinator =
                createCheckpointCoordinator(graph, checkpointStore, checkpointIDCounter);
        checkpointCoordinator.restoreSavepoint(
                SavepointRestoreSettings.forPath(
                        savepoint.getExternalPointer(), true, RestoreMode.NO_CLAIM),
                graph.getAllVertices(),
                this.getClass().getClassLoader());

        // trigger a savepoint before any checkpoint completes
        // next triggered checkpoint should still be a full one
        takeSavepoint(graph, attemptID, checkpointCoordinator, 2);
        checkpointCoordinator.startCheckpointScheduler();
        gateway.resetCount();
        // the checkpoint should be a FULL_CHECKPOINT
        final CompletableFuture<CompletedCheckpoint> checkpoint =
                checkpointCoordinator.triggerCheckpoint(true);
        manuallyTriggeredScheduledExecutor.triggerAll();
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID, 3),
                TASK_MANAGER_LOCATION_INFO);
        checkpoint.get();

        assertThat(
                        gateway.getOnlyTriggeredCheckpoint(attemptID)
                                .checkpointOptions
                                .getCheckpointType())
                .isEqualTo(CheckpointType.FULL_CHECKPOINT);
    }

    @Test
    void testTriggeringCheckpointsWithNullCheckpointType() throws Exception {
        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final StandaloneCompletedCheckpointStore checkpointStore =
                new StandaloneCompletedCheckpointStore(1);
        final StandaloneCheckpointIDCounter checkpointIDCounter =
                new StandaloneCheckpointIDCounter();

        CheckpointCoordinator checkpointCoordinator =
                createCheckpointCoordinator(graph, checkpointStore, checkpointIDCounter);

        checkpointCoordinator.startCheckpointScheduler();
        gateway.resetCount();

        Assertions.assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> checkpointCoordinator.triggerCheckpoint(null));
    }

    @Test
    void testTriggeringCheckpointsWithIncrementalCheckpointType() throws Exception {
        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        final StandaloneCompletedCheckpointStore checkpointStore =
                new StandaloneCompletedCheckpointStore(1);
        final StandaloneCheckpointIDCounter checkpointIDCounter =
                new StandaloneCheckpointIDCounter();
        CheckpointCoordinator checkpointCoordinator =
                createCheckpointCoordinator(graph, checkpointStore, checkpointIDCounter);

        checkpointCoordinator.startCheckpointScheduler();
        gateway.resetCount();

        // trigger an incremental type checkpoint
        final CompletableFuture<CompletedCheckpoint> checkpoint =
                checkpointCoordinator.triggerCheckpoint(
                        org.apache.flink.core.execution.CheckpointType.INCREMENTAL);

        manuallyTriggeredScheduledExecutor.triggerAll();
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID, 1),
                TASK_MANAGER_LOCATION_INFO);
        checkpoint.get();
        Assertions.assertThat(
                        gateway.getOnlyTriggeredCheckpoint(attemptID)
                                .checkpointOptions
                                .getCheckpointType())
                .isEqualTo(CheckpointType.CHECKPOINT);
    }

    @Test
    void testTriggeringCheckpointsWithFullCheckpointType() throws Exception {
        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        final StandaloneCompletedCheckpointStore checkpointStore =
                new StandaloneCompletedCheckpointStore(1);
        final StandaloneCheckpointIDCounter checkpointIDCounter =
                new StandaloneCheckpointIDCounter();
        CheckpointCoordinator checkpointCoordinator =
                createCheckpointCoordinator(graph, checkpointStore, checkpointIDCounter);

        checkpointCoordinator.startCheckpointScheduler();
        gateway.resetCount();

        // trigger an full type checkpoint
        final CompletableFuture<CompletedCheckpoint> checkpoint =
                checkpointCoordinator.triggerCheckpoint(
                        org.apache.flink.core.execution.CheckpointType.FULL);

        manuallyTriggeredScheduledExecutor.triggerAll();
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID, 1),
                TASK_MANAGER_LOCATION_INFO);
        checkpoint.get();
        Assertions.assertThat(
                        gateway.getOnlyTriggeredCheckpoint(attemptID)
                                .checkpointOptions
                                .getCheckpointType())
                .isEqualTo(CheckpointType.FULL_CHECKPOINT);
    }

    @Test
    void testTriggeringCheckpointsWithCheckpointTypeAfterNoClaimSavepoint() throws Exception {
        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        // create a savepoint, we can restore from later
        final CompletedCheckpoint savepoint = takeSavepoint(graph, attemptID);

        // restore from a savepoint in NO_CLAIM mode
        final StandaloneCompletedCheckpointStore checkpointStore =
                new StandaloneCompletedCheckpointStore(1);
        final StandaloneCheckpointIDCounter checkpointIDCounter =
                new StandaloneCheckpointIDCounter();
        CheckpointCoordinator checkpointCoordinator =
                createCheckpointCoordinator(graph, checkpointStore, checkpointIDCounter);
        checkpointCoordinator.restoreSavepoint(
                SavepointRestoreSettings.forPath(
                        savepoint.getExternalPointer(), true, RestoreMode.NO_CLAIM),
                graph.getAllVertices(),
                this.getClass().getClassLoader());

        // trigger a savepoint before any checkpoint completes
        // next triggered checkpoint should still be a full one
        takeSavepoint(graph, attemptID, checkpointCoordinator, 2);
        checkpointCoordinator.startCheckpointScheduler();
        gateway.resetCount();
        // the checkpoint should be a FULL_CHECKPOINT even it is specified as incremental
        final CompletableFuture<CompletedCheckpoint> checkpoint =
                checkpointCoordinator.triggerCheckpoint(
                        org.apache.flink.core.execution.CheckpointType.INCREMENTAL);
        manuallyTriggeredScheduledExecutor.triggerAll();
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID, 3),
                TASK_MANAGER_LOCATION_INFO);
        checkpoint.get();

        Assertions.assertThat(
                        gateway.getOnlyTriggeredCheckpoint(attemptID)
                                .checkpointOptions
                                .getCheckpointType())
                .isEqualTo(CheckpointType.FULL_CHECKPOINT);
    }

    private CompletedCheckpoint takeSavepoint(ExecutionGraph graph, ExecutionAttemptID attemptID)
            throws Exception {
        CheckpointCoordinator checkpointCoordinator =
                createCheckpointCoordinator(
                        graph,
                        new StandaloneCompletedCheckpointStore(1),
                        new StandaloneCheckpointIDCounter());
        final CompletedCheckpoint savepoint =
                takeSavepoint(graph, attemptID, checkpointCoordinator, 1);
        checkpointCoordinator.shutdown();
        return savepoint;
    }

    private CompletedCheckpoint takeSavepoint(
            ExecutionGraph graph,
            ExecutionAttemptID attemptID,
            CheckpointCoordinator checkpointCoordinator,
            int savepointId)
            throws Exception {
        final CompletableFuture<CompletedCheckpoint> savepointFuture =
                checkpointCoordinator.triggerSavepoint(
                        TempDirUtils.newFolder(temporaryFolder).getPath(),
                        SavepointFormatType.CANONICAL);
        manuallyTriggeredScheduledExecutor.triggerAll();
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID, savepointId),
                TASK_MANAGER_LOCATION_INFO);
        return savepointFuture.get();
    }

    private CheckpointCoordinator createCheckpointCoordinator(
            ExecutionGraph graph,
            StandaloneCompletedCheckpointStore checkpointStore,
            CheckpointIDCounter checkpointIDCounter)
            throws Exception {
        CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(
                                10000) // periodic is very long, we trigger checkpoint manually
                        .setCheckpointTimeout(200000) // timeout is very long (200 s)
                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                        .build();
        return new CheckpointCoordinatorBuilder()
                .setCheckpointCoordinatorConfiguration(checkpointCoordinatorConfiguration)
                .setCompletedCheckpointStore(checkpointStore)
                .setCheckpointIDCounter(checkpointIDCounter)
                .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                .build(graph);
    }

    /**
     * This test verified that after a completed checkpoint a certain time has passed before another
     * is triggered.
     */
    @Test
    void testMinTimeBetweenCheckpointsInterval() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        final long delay = 50;
        final long checkpointInterval = 12;

        CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                new CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(checkpointInterval) // periodic interval is 12 ms
                        .setCheckpointTimeout(200_000) // timeout is very long (200 s)
                        .setMinPauseBetweenCheckpoints(delay) // 50 ms delay between checkpoints
                        .setMaxConcurrentCheckpoints(1)
                        .build();
        final CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(checkpointCoordinatorConfiguration)
                        .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                        .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        try {
            checkpointCoordinator.startCheckpointScheduler();
            manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
            manuallyTriggeredScheduledExecutor.triggerAll();

            // wait until the first checkpoint was triggered
            Long firstCallId = gateway.getTriggeredCheckpoints(attemptID).get(0).checkpointId;
            assertThat(firstCallId).isEqualTo(1L);

            AcknowledgeCheckpoint ackMsg =
                    new AcknowledgeCheckpoint(graph.getJobID(), attemptID, 1L);

            // tell the coordinator that the checkpoint is done
            final long ackTime = System.nanoTime();
            checkpointCoordinator.receiveAcknowledgeMessage(ackMsg, TASK_MANAGER_LOCATION_INFO);

            gateway.resetCount();
            manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
            manuallyTriggeredScheduledExecutor.triggerAll();
            while (gateway.getTriggeredCheckpoints(attemptID).isEmpty()) {
                // sleeps for a while to simulate periodic scheduling
                Thread.sleep(checkpointInterval);
                manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
                manuallyTriggeredScheduledExecutor.triggerAll();
            }
            // wait until the next checkpoint is triggered
            Long nextCallId = gateway.getTriggeredCheckpoints(attemptID).get(0).checkpointId;
            final long nextCheckpointTime = System.nanoTime();
            assertThat(nextCallId).isEqualTo(2L);

            final long delayMillis = (nextCheckpointTime - ackTime) / 1_000_000;

            // we need to add one ms here to account for rounding errors
            if (delayMillis + 1 < delay) {
                fail(
                        "checkpoint came too early: delay was "
                                + delayMillis
                                + " but should have been at least "
                                + delay);
            }
        } finally {
            checkpointCoordinator.stopCheckpointScheduler();
            checkpointCoordinator.shutdown();
        }
    }

    @Test
    void testStopPeriodicScheduler() throws Exception {
        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator();

        final CompletableFuture<CompletedCheckpoint> onCompletionPromise1 =
                triggerPeriodicCheckpoint(checkpointCoordinator);
        manuallyTriggeredScheduledExecutor.triggerAll();
        try {
            onCompletionPromise1.get();
            fail("The triggerCheckpoint call expected an exception");
        } catch (ExecutionException e) {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    ExceptionUtils.findThrowable(e, CheckpointException.class);
            assertThat(checkpointExceptionOptional)
                    .isPresent()
                    .map(CheckpointException::getCheckpointFailureReason)
                    .get()
                    .isEqualTo(CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN);
        }

        // Not periodic
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise2 =
                checkpointCoordinator.triggerCheckpoint(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        null,
                        false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(onCompletionPromise2).isNotCompletedExceptionally();
    }

    @Test
    void testTriggerCheckpointWithShuttingDownCoordinator() throws Exception {
        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator();
        checkpointCoordinator.startCheckpointScheduler();
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
                triggerPeriodicCheckpoint(checkpointCoordinator);

        checkpointCoordinator.shutdown();
        manuallyTriggeredScheduledExecutor.triggerAll();
        try {
            onCompletionPromise.get();
            fail("Should not reach here");
        } catch (ExecutionException e) {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    ExceptionUtils.findThrowable(e, CheckpointException.class);
            assertThat(checkpointExceptionOptional)
                    .isPresent()
                    .map(CheckpointException::getCheckpointFailureReason)
                    .get()
                    .isEqualTo(CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN);
        }
    }

    @Test
    void testTriggerFLushEventWithShuttingDownCoordinator() throws Exception {
        // set up the coordinator and validate the initial state
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(new JobVertexID())
                        .addJobVertex(new JobVertexID(), false)
                        .setTransitToRunning(false)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        CheckpointCoordinator checkpointCoordinator = new CheckpointCoordinatorBuilder()
                .setCheckpointCoordinatorConfiguration(
                        CheckpointCoordinatorConfiguration.builder()
                                .setAllowedLatency(10)
                                .setCheckpointInterval(Long.MAX_VALUE)
                                .setAlignedCheckpointTimeout(Long.MAX_VALUE)
                                .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                .build())
                .setFlushEventTimer(manuallyTriggeredScheduledExecutor)
                .build(graph);

        checkpointCoordinator.startFlushEventScheduler();
        checkpointCoordinator.triggerFlushEvent();

        checkpointCoordinator.shutdown();
        manuallyTriggeredScheduledExecutor.triggerAll();
    }

    @Test
    void testTriggerCheckpointBeforePreviousOneCompleted() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator(graph);

        checkpointCoordinator.startCheckpointScheduler();
        // start a periodic checkpoint first
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise1 =
                triggerPeriodicCheckpoint(checkpointCoordinator);
        assertThat(checkpointCoordinator.isTriggering()).isTrue();
        assertThat(checkpointCoordinator.getTriggerRequestQueue()).isEmpty();
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise2 =
                triggerPeriodicCheckpoint(checkpointCoordinator);
        // another trigger before the prior one finished

        assertThat(checkpointCoordinator.isTriggering()).isTrue();
        assertThat(checkpointCoordinator.getTriggerRequestQueue()).hasSize(1);

        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(onCompletionPromise1).isNotCompletedExceptionally();
        assertThat(onCompletionPromise2).isNotCompletedExceptionally();
        assertThat(checkpointCoordinator.isTriggering()).isFalse();
        assertThat(checkpointCoordinator.getTriggerRequestQueue()).isEmpty();
        assertThat(gateway.getTriggeredCheckpoints(attemptID)).hasSize(2);
    }

    @Test
    void testTriggerCheckpointRequestQueuedWithFailure() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointIDCounter(new UnstableCheckpointIDCounter(id -> id == 0))
                        .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        checkpointCoordinator.startCheckpointScheduler();
        // start a periodic checkpoint first
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise1 =
                triggerNonPeriodicCheckpoint(checkpointCoordinator);
        assertThat(checkpointCoordinator.isTriggering()).isTrue();
        assertThat(checkpointCoordinator.getTriggerRequestQueue()).isEmpty();

        // another trigger before the prior one finished
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise2 =
                triggerNonPeriodicCheckpoint(checkpointCoordinator);

        // another trigger before the first one finished
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise3 =
                triggerNonPeriodicCheckpoint(checkpointCoordinator);
        assertThat(checkpointCoordinator.isTriggering()).isTrue();
        assertThat(checkpointCoordinator.getTriggerRequestQueue()).hasSize(2);

        manuallyTriggeredScheduledExecutor.triggerAll();
        // the first triggered checkpoint fails by design through UnstableCheckpointIDCounter
        assertThat(onCompletionPromise1).isCompletedExceptionally();
        assertThat(onCompletionPromise2).isNotCompletedExceptionally();
        assertThat(onCompletionPromise3).isNotCompletedExceptionally();
        assertThat(checkpointCoordinator.isTriggering()).isFalse();
        assertThat(checkpointCoordinator.getTriggerRequestQueue()).isEmpty();
        assertThat(gateway.getTriggeredCheckpoints(attemptID).size()).isEqualTo(2);
    }

    @Test
    void testTriggerCheckpointRequestCancelled() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator(graph);

        final CompletableFuture<String> masterHookCheckpointFuture = new CompletableFuture<>();
        checkpointCoordinator.addMasterHook(new TestingMasterHook(masterHookCheckpointFuture));
        checkpointCoordinator.startCheckpointScheduler();
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
                triggerPeriodicCheckpoint(checkpointCoordinator);

        // checkpoint trigger will not finish since master hook checkpoint is not finished yet
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(checkpointCoordinator.isTriggering()).isTrue();

        // trigger cancellation
        manuallyTriggeredScheduledExecutor.triggerNonPeriodicScheduledTasks();
        assertThat(checkpointCoordinator.isTriggering()).isTrue();

        try {
            onCompletionPromise.get();
            fail("Should not reach here");
        } catch (ExecutionException e) {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    ExceptionUtils.findThrowable(e, CheckpointException.class);
            assertThat(checkpointExceptionOptional)
                    .isPresent()
                    .map(CheckpointException::getCheckpointFailureReason)
                    .get()
                    .isEqualTo(CheckpointFailureReason.CHECKPOINT_EXPIRED);
        }

        // continue triggering
        masterHookCheckpointFuture.complete("finish master hook");

        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(checkpointCoordinator.isTriggering()).isFalse();
        // it doesn't really trigger task manager to do checkpoint
        assertThat(gateway.getTriggeredCheckpoints(attemptID)).isEmpty();
        assertThat(checkpointCoordinator.getTriggerRequestQueue()).isEmpty();
    }

    @Test
    void testTriggerCheckpointInitializationFailed() throws Exception {
        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointIDCounter(new UnstableCheckpointIDCounter(id -> id == 0))
                        .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        checkpointCoordinator.startCheckpointScheduler();
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise1 =
                triggerPeriodicCheckpoint(checkpointCoordinator);
        assertThat(checkpointCoordinator.isTriggering()).isTrue();
        assertThat(checkpointCoordinator.getTriggerRequestQueue()).isEmpty();

        manuallyTriggeredScheduledExecutor.triggerAll();
        try {
            onCompletionPromise1.get();
            fail("This checkpoint should fail through UnstableCheckpointIDCounter");
        } catch (ExecutionException e) {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    ExceptionUtils.findThrowable(e, CheckpointException.class);
            assertThat(checkpointExceptionOptional.isPresent()).isTrue();
            assertThat(checkpointExceptionOptional.get().getCheckpointFailureReason())
                    .isEqualTo(CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE);
        }
        assertThat(checkpointCoordinator.isTriggering()).isFalse();
        assertThat(checkpointCoordinator.getTriggerRequestQueue()).isEmpty();

        final CompletableFuture<CompletedCheckpoint> onCompletionPromise2 =
                triggerPeriodicCheckpoint(checkpointCoordinator);
        assertThat(checkpointCoordinator.isTriggering()).isTrue();
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(onCompletionPromise2).isNotCompletedExceptionally();
        assertThat(checkpointCoordinator.isTriggering()).isFalse();
        assertThat(checkpointCoordinator.getTriggerRequestQueue()).isEmpty();
    }

    @Test
    void testTriggerCheckpointSnapshotMasterHookFailed() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator();

        final CompletableFuture<String> masterHookCheckpointFuture = new CompletableFuture<>();
        checkpointCoordinator.addMasterHook(new TestingMasterHook(masterHookCheckpointFuture));
        checkpointCoordinator.startCheckpointScheduler();
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
                triggerPeriodicCheckpoint(checkpointCoordinator);

        // checkpoint trigger will not finish since master hook checkpoint is not finished yet
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(checkpointCoordinator.isTriggering()).isTrue();

        // continue triggering
        masterHookCheckpointFuture.completeExceptionally(new Exception("by design"));

        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(checkpointCoordinator.isTriggering()).isFalse();

        try {
            onCompletionPromise.get();
            fail("Should not reach here");
        } catch (ExecutionException e) {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    ExceptionUtils.findThrowable(e, CheckpointException.class);
            assertThat(checkpointExceptionOptional)
                    .isPresent()
                    .map(CheckpointException::getCheckpointFailureReason)
                    .get()
                    .isEqualTo(CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE);
        }
        // it doesn't really trigger task manager to do checkpoint
        assertThat(gateway.getTriggeredCheckpoints(attemptID)).isEmpty();
        assertThat(checkpointCoordinator.getTriggerRequestQueue()).isEmpty();
    }

    /** This test only fails eventually. */
    @Test
    void discardingTriggeringCheckpointWillExecuteNextCheckpointRequest() throws Exception {
        final ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor();
        final CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder()
                        .setCheckpointTimer(new ScheduledExecutorServiceAdapter(scheduledExecutorService))
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder().build())
                        // Since timer thread != main thread we should override the default main
                        // thread executor because it initially requires triggering a checkpoint
                        // from the main test thread.
                        .build(
                                new CheckpointCoordinatorTestingUtils
                                                .CheckpointExecutionGraphBuilder()
                                        .addJobVertex(new JobVertexID())
                                        .setMainThreadExecutor(
                                                ComponentMainThreadExecutorServiceAdapter
                                                        .forSingleThreadExecutor(
                                                                new DirectScheduledExecutorService()))
                                        .build(EXECUTOR_RESOURCE.getExecutor()));

        final CompletableFuture<String> masterHookCheckpointFuture = new CompletableFuture<>();
        final OneShotLatch triggerCheckpointLatch = new OneShotLatch();
        checkpointCoordinator.addMasterHook(
                new TestingMasterHook(masterHookCheckpointFuture, triggerCheckpointLatch));

        try {
            checkpointCoordinator.triggerCheckpoint(false);
            final CompletableFuture<CompletedCheckpoint> secondCheckpoint =
                    checkpointCoordinator.triggerCheckpoint(false);

            triggerCheckpointLatch.await();
            masterHookCheckpointFuture.complete("Completed");

            // discard triggering checkpoint
            checkpointCoordinator.abortPendingCheckpoints(
                    new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED));

            try {
                // verify that the second checkpoint request will be executed and eventually times
                // out
                secondCheckpoint.get();
                fail("Expected the second checkpoint to fail.");
            } catch (ExecutionException ee) {
                assertThat(ExceptionUtils.stripExecutionException(ee))
                        .isInstanceOf(CheckpointException.class);
            }
        } finally {
            checkpointCoordinator.shutdown();
            ExecutorUtils.gracefulShutdown(10L, TimeUnit.SECONDS, scheduledExecutorService);
        }
    }

    private CheckpointCoordinator createCheckpointCoordinator() throws Exception {
        return new CheckpointCoordinatorBuilder()
                .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                .build(EXECUTOR_RESOURCE.getExecutor());
    }

    private CheckpointCoordinator createCheckpointCoordinator(ExecutionGraph graph)
            throws Exception {
        return new CheckpointCoordinatorBuilder()
                .setCheckpointTimer(manuallyTriggeredScheduledExecutor)
                .build(graph);
    }

    private CompletableFuture<CompletedCheckpoint> triggerPeriodicCheckpoint(
            CheckpointCoordinator checkpointCoordinator) {

        return checkpointCoordinator.triggerCheckpoint(
                CheckpointProperties.forCheckpoint(
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                null,
                true);
    }

    private CompletableFuture<CompletedCheckpoint> triggerNonPeriodicCheckpoint(
            CheckpointCoordinator checkpointCoordinator) {

        return checkpointCoordinator.triggerCheckpoint(
                CheckpointProperties.forCheckpoint(
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                null,
                false);
    }

    private static class TestingMasterHook implements MasterTriggerRestoreHook<String> {

        private final SimpleVersionedSerializer<String> serializer =
                new CheckpointCoordinatorTestingUtils.StringSerializer();

        private final CompletableFuture<String> checkpointFuture;
        private final OneShotLatch triggerCheckpointLatch;

        private TestingMasterHook(CompletableFuture<String> checkpointFuture) {
            this(checkpointFuture, new OneShotLatch());
        }

        private TestingMasterHook(
                CompletableFuture<String> checkpointFuture, OneShotLatch triggerCheckpointLatch) {
            this.checkpointFuture = checkpointFuture;
            this.triggerCheckpointLatch = triggerCheckpointLatch;
        }

        @Override
        public String getIdentifier() {
            return "testing master hook";
        }

        @Nullable
        @Override
        public CompletableFuture<String> triggerCheckpoint(
                long checkpointId, long timestamp, Executor executor) {
            triggerCheckpointLatch.trigger();
            return checkpointFuture;
        }

        @Override
        public void restoreCheckpoint(long checkpointId, @Nullable String checkpointData) {}

        @Nullable
        @Override
        public SimpleVersionedSerializer<String> createCheckpointDataSerializer() {
            return serializer;
        }
    }

    private static class UnstableCheckpointIDCounter implements CheckpointIDCounter {

        private final Predicate<Long> checkpointFailurePredicate;

        private long id = 0;

        public UnstableCheckpointIDCounter(Predicate<Long> checkpointFailurePredicate) {
            this.checkpointFailurePredicate = checkNotNull(checkpointFailurePredicate);
        }

        @Override
        public void start() {}

        @Override
        public CompletableFuture<Void> shutdown(JobStatus jobStatus) {
            return FutureUtils.completedVoidFuture();
        }

        @Override
        public long getAndIncrement() {
            if (checkpointFailurePredicate.test(id++)) {
                throw new RuntimeException("CheckpointIDCounter#getAndIncrement fails by design");
            }
            return id;
        }

        @Override
        public long get() {
            return id;
        }

        @Override
        public void setCount(long newId) {}
    }
}
