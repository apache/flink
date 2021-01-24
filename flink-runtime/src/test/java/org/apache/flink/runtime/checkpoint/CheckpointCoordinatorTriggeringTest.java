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
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for checkpoint coordinator triggering. */
public class CheckpointCoordinatorTriggeringTest extends TestLogger {
    private static final String TASK_MANAGER_LOCATION_INFO = "Unknown location";

    private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor;

    @Before
    public void setUp() throws Exception {
        manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();
    }

    @Test
    public void testPeriodicTriggering() {
        try {
            final long start = System.currentTimeMillis();

            CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                    new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

            JobVertexID jobVertexID = new JobVertexID();
            ExecutionGraph graph =
                    new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                            .addJobVertex(jobVertexID)
                            .setTaskManagerGateway(gateway)
                            .build();

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
                            .setExecutionGraph(graph)
                            .setCheckpointCoordinatorConfiguration(
                                    checkpointCoordinatorConfiguration)
                            .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                            .setTimer(manuallyTriggeredScheduledExecutor)
                            .build();

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
            assertEquals(5, gateway.getTriggeredCheckpoints(attemptID).size());

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
            assertEquals(5, gateway.getTriggeredCheckpoints(attemptID).size());

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
        assertEquals(numTrigger, checkpoints.size());

        long lastId = -1;
        long lastTs = -1;

        for (CheckpointCoordinatorTestingUtils.TriggeredCheckpoint checkpoint : checkpoints) {
            assertTrue(
                    "Trigger checkpoint id should be in increase order",
                    checkpoint.checkpointId > lastId);
            assertTrue(
                    "Trigger checkpoint timestamp should be in increase order",
                    checkpoint.timestamp >= lastTs);
            assertTrue(
                    "Trigger checkpoint timestamp should be larger than the start time",
                    checkpoint.timestamp >= start);

            lastId = checkpoint.checkpointId;
            lastTs = checkpoint.timestamp;
        }
    }

    /**
     * This test verified that after a completed checkpoint a certain time has passed before another
     * is triggered.
     */
    @Test
    public void testMinTimeBetweenCheckpointsInterval() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build();

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
                        .setExecutionGraph(graph)
                        .setCheckpointCoordinatorConfiguration(checkpointCoordinatorConfiguration)
                        .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        try {
            checkpointCoordinator.startCheckpointScheduler();
            manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
            manuallyTriggeredScheduledExecutor.triggerAll();

            // wait until the first checkpoint was triggered
            Long firstCallId = gateway.getTriggeredCheckpoints(attemptID).get(0).checkpointId;
            assertEquals(1L, firstCallId.longValue());

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
            assertEquals(2L, nextCallId.longValue());

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
    public void testStopPeriodicScheduler() throws Exception {
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
            assertTrue(checkpointExceptionOptional.isPresent());
            assertEquals(
                    CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN,
                    checkpointExceptionOptional.get().getCheckpointFailureReason());
        }

        // Not periodic
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise2 =
                checkpointCoordinator.triggerCheckpoint(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        null,
                        false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertFalse(onCompletionPromise2.isCompletedExceptionally());
    }

    @Test
    public void testTriggerCheckpointWithShuttingDownCoordinator() throws Exception {
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
            assertTrue(checkpointExceptionOptional.isPresent());
            assertEquals(
                    CheckpointFailureReason.CHECKPOINT_COORDINATOR_SHUTDOWN,
                    checkpointExceptionOptional.get().getCheckpointFailureReason());
        }
    }

    @Test
    public void testTriggerCheckpointBeforePreviousOneCompleted() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build();

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator = createCheckpointCoordinator(graph);

        checkpointCoordinator.startCheckpointScheduler();
        // start a periodic checkpoint first
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise1 =
                triggerPeriodicCheckpoint(checkpointCoordinator);
        assertTrue(checkpointCoordinator.isTriggering());
        assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise2 =
                triggerPeriodicCheckpoint(checkpointCoordinator);
        // another trigger before the prior one finished

        assertTrue(checkpointCoordinator.isTriggering());
        assertEquals(1, checkpointCoordinator.getTriggerRequestQueue().size());

        manuallyTriggeredScheduledExecutor.triggerAll();
        assertFalse(onCompletionPromise1.isCompletedExceptionally());
        assertFalse(onCompletionPromise2.isCompletedExceptionally());
        assertFalse(checkpointCoordinator.isTriggering());
        assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());
        assertEquals(2, gateway.getTriggeredCheckpoints(attemptID).size());
    }

    @Test
    public void testTriggerCheckpointRequestQueuedWithFailure() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build();

        ExecutionVertex vertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = vertex.getCurrentExecutionAttempt().getAttemptId();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(graph)
                        .setCheckpointIDCounter(new UnstableCheckpointIDCounter(id -> id == 0))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        checkpointCoordinator.startCheckpointScheduler();
        // start a periodic checkpoint first
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise1 =
                triggerNonPeriodicCheckpoint(checkpointCoordinator);
        assertTrue(checkpointCoordinator.isTriggering());
        assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());

        // another trigger before the prior one finished
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise2 =
                triggerNonPeriodicCheckpoint(checkpointCoordinator);

        // another trigger before the first one finished
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise3 =
                triggerNonPeriodicCheckpoint(checkpointCoordinator);
        assertTrue(checkpointCoordinator.isTriggering());
        assertEquals(2, checkpointCoordinator.getTriggerRequestQueue().size());

        manuallyTriggeredScheduledExecutor.triggerAll();
        // the first triggered checkpoint fails by design through UnstableCheckpointIDCounter
        assertTrue(onCompletionPromise1.isCompletedExceptionally());
        assertFalse(onCompletionPromise2.isCompletedExceptionally());
        assertFalse(onCompletionPromise3.isCompletedExceptionally());
        assertFalse(checkpointCoordinator.isTriggering());
        assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());
        assertEquals(2, gateway.getTriggeredCheckpoints(attemptID).size());
    }

    @Test
    public void testTriggerCheckpointRequestCancelled() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build();

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
        assertTrue(checkpointCoordinator.isTriggering());

        // trigger cancellation
        manuallyTriggeredScheduledExecutor.triggerNonPeriodicScheduledTasks();
        assertTrue(checkpointCoordinator.isTriggering());

        try {
            onCompletionPromise.get();
            fail("Should not reach here");
        } catch (ExecutionException e) {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    ExceptionUtils.findThrowable(e, CheckpointException.class);
            assertTrue(checkpointExceptionOptional.isPresent());
            assertEquals(
                    CheckpointFailureReason.CHECKPOINT_EXPIRED,
                    checkpointExceptionOptional.get().getCheckpointFailureReason());
        }

        // continue triggering
        masterHookCheckpointFuture.complete("finish master hook");

        manuallyTriggeredScheduledExecutor.triggerAll();
        assertFalse(checkpointCoordinator.isTriggering());
        // it doesn't really trigger task manager to do checkpoint
        assertEquals(0, gateway.getTriggeredCheckpoints(attemptID).size());
        assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());
    }

    @Test
    public void testTriggerCheckpointInitializationFailed() throws Exception {
        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointIDCounter(new UnstableCheckpointIDCounter(id -> id == 0))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        checkpointCoordinator.startCheckpointScheduler();
        final CompletableFuture<CompletedCheckpoint> onCompletionPromise1 =
                triggerPeriodicCheckpoint(checkpointCoordinator);
        assertTrue(checkpointCoordinator.isTriggering());
        assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());

        manuallyTriggeredScheduledExecutor.triggerAll();
        try {
            onCompletionPromise1.get();
            fail("This checkpoint should fail through UnstableCheckpointIDCounter");
        } catch (ExecutionException e) {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    ExceptionUtils.findThrowable(e, CheckpointException.class);
            assertTrue(checkpointExceptionOptional.isPresent());
            assertEquals(
                    CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE,
                    checkpointExceptionOptional.get().getCheckpointFailureReason());
        }
        assertFalse(checkpointCoordinator.isTriggering());
        assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());

        final CompletableFuture<CompletedCheckpoint> onCompletionPromise2 =
                triggerPeriodicCheckpoint(checkpointCoordinator);
        assertTrue(checkpointCoordinator.isTriggering());
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertFalse(onCompletionPromise2.isCompletedExceptionally());
        assertFalse(checkpointCoordinator.isTriggering());
        assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());
    }

    @Test
    public void testTriggerCheckpointSnapshotMasterHookFailed() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTaskManagerGateway(gateway)
                        .build();

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
        assertTrue(checkpointCoordinator.isTriggering());

        // continue triggering
        masterHookCheckpointFuture.completeExceptionally(new Exception("by design"));

        manuallyTriggeredScheduledExecutor.triggerAll();
        assertFalse(checkpointCoordinator.isTriggering());

        try {
            onCompletionPromise.get();
            fail("Should not reach here");
        } catch (ExecutionException e) {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    ExceptionUtils.findThrowable(e, CheckpointException.class);
            assertTrue(checkpointExceptionOptional.isPresent());
            assertEquals(
                    CheckpointFailureReason.TRIGGER_CHECKPOINT_FAILURE,
                    checkpointExceptionOptional.get().getCheckpointFailureReason());
        }
        // it doesn't really trigger task manager to do checkpoint
        assertEquals(0, gateway.getTriggeredCheckpoints(attemptID).size());
        assertEquals(0, checkpointCoordinator.getTriggerRequestQueue().size());
    }

    /** This test only fails eventually. */
    @Test
    public void discardingTriggeringCheckpointWillExecuteNextCheckpointRequest() throws Exception {
        final ScheduledExecutorService scheduledExecutorService =
                Executors.newSingleThreadScheduledExecutor();
        final CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder()
                        .setTimer(new ScheduledExecutorServiceAdapter(scheduledExecutorService))
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder().build())
                        .build();

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
                assertThat(
                        ExceptionUtils.stripExecutionException(ee),
                        instanceOf(CheckpointException.class));
            }
        } finally {
            checkpointCoordinator.shutdown();
            ExecutorUtils.gracefulShutdown(10L, TimeUnit.SECONDS, scheduledExecutorService);
        }
    }

    private CheckpointCoordinator createCheckpointCoordinator() throws Exception {
        return new CheckpointCoordinatorBuilder()
                .setTimer(manuallyTriggeredScheduledExecutor)
                .build();
    }

    private CheckpointCoordinator createCheckpointCoordinator(ExecutionGraph graph)
            throws Exception {
        return new CheckpointCoordinatorBuilder()
                .setExecutionGraph(graph)
                .setTimer(manuallyTriggeredScheduledExecutor)
                .build();
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
        public void shutdown(JobStatus jobStatus) throws Exception {}

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
