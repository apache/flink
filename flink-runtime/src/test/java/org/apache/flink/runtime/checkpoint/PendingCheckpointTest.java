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

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.StringSerializer;
import org.apache.flink.runtime.checkpoint.PendingCheckpoint.TaskAcknowledgeResult;
import org.apache.flink.runtime.checkpoint.hooks.MasterHooks;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.operators.coordination.TestingOperatorInfo;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.TestingStreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageLocation;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import javax.annotation.Nullable;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the {@link PendingCheckpoint}. */
class PendingCheckpointTest {

    private static final List<Execution> ACK_TASKS = new ArrayList<>();
    private static final List<ExecutionVertex> TASKS_TO_COMMIT = new ArrayList<>();
    private static final ExecutionAttemptID ATTEMPT_ID = createExecutionAttemptId();

    public static final OperatorID OPERATOR_ID = new OperatorID();

    public static final int PARALLELISM = 1;

    public static final int MAX_PARALLELISM = 128;

    static {
        ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
        when(jobVertex.getOperatorIDs())
                .thenReturn(Collections.singletonList(OperatorIDPair.generatedIDOnly(OPERATOR_ID)));

        ExecutionVertex vertex = mock(ExecutionVertex.class);
        when(vertex.getMaxParallelism()).thenReturn(MAX_PARALLELISM);
        when(vertex.getTotalNumberOfParallelSubtasks()).thenReturn(PARALLELISM);
        when(vertex.getJobVertex()).thenReturn(jobVertex);

        Execution execution = mock(Execution.class);
        when(execution.getAttemptId()).thenReturn(ATTEMPT_ID);
        when(execution.getVertex()).thenReturn(vertex);
        ACK_TASKS.add(execution);
        TASKS_TO_COMMIT.add(vertex);
    }

    @TempDir private java.nio.file.Path tmpFolder;

    /** Tests that pending checkpoints can be subsumed iff they are forced. */
    @Test
    void testCanBeSubsumed() throws Exception {
        // Forced checkpoints cannot be subsumed
        CheckpointProperties forced =
                new CheckpointProperties(
                        true,
                        SavepointType.savepoint(SavepointFormatType.CANONICAL),
                        false,
                        false,
                        false,
                        false,
                        false,
                        false);
        final PendingCheckpoint pending = createPendingCheckpoint(forced);
        assertThat(pending.canBeSubsumed()).isFalse();

        assertThatThrownBy(
                        () -> abort(pending, CheckpointFailureReason.CHECKPOINT_SUBSUMED),
                        "Did not throw expected Exception")
                .isInstanceOf(IllegalStateException.class);

        // Non-forced checkpoints can be subsumed
        CheckpointProperties subsumed =
                new CheckpointProperties(
                        false,
                        SavepointType.savepoint(SavepointFormatType.CANONICAL),
                        false,
                        false,
                        false,
                        false,
                        false,
                        false);
        assertThat(createPendingCheckpoint(subsumed).canBeSubsumed()).isFalse();
    }

    @Test
    void testSyncSavepointCannotBeSubsumed() throws Exception {
        // Forced checkpoints cannot be subsumed
        CheckpointProperties forced =
                CheckpointProperties.forSyncSavepoint(true, false, SavepointFormatType.CANONICAL);
        PendingCheckpoint pending = createPendingCheckpoint(forced);
        assertThat(pending.canBeSubsumed()).isFalse();

        assertThatThrownBy(
                        () -> abort(pending, CheckpointFailureReason.CHECKPOINT_SUBSUMED),
                        "Did not throw expected Exception")
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * Tests that the completion future is succeeded on finalize and failed on abort and failures
     * during finalize.
     */
    @Test
    void testCompletionFuture() throws Exception {
        CheckpointProperties props =
                new CheckpointProperties(
                        false,
                        SavepointType.savepoint(SavepointFormatType.CANONICAL),
                        false,
                        false,
                        false,
                        false,
                        false,
                        false);

        // Abort declined
        PendingCheckpoint pending = createPendingCheckpoint(props);
        CompletableFuture<CompletedCheckpoint> future = pending.getCompletionFuture();

        assertThat(future.isDone()).isFalse();
        abort(pending, CheckpointFailureReason.CHECKPOINT_DECLINED);
        assertThat(future.isDone()).isTrue();

        // Abort expired
        pending = createPendingCheckpoint(props);
        future = pending.getCompletionFuture();

        assertThat(future.isDone()).isFalse();
        abort(pending, CheckpointFailureReason.CHECKPOINT_DECLINED);
        assertThat(future.isDone()).isTrue();

        // Abort subsumed
        pending = createPendingCheckpoint(props);
        future = pending.getCompletionFuture();

        assertThat(future.isDone()).isFalse();
        abort(pending, CheckpointFailureReason.CHECKPOINT_DECLINED);
        assertThat(future.isDone()).isTrue();

        // Finalize (all ACK'd)
        pending = createPendingCheckpoint(props);
        future = pending.getCompletionFuture();

        assertThat(future.isDone()).isFalse();
        pending.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics());
        assertThat(pending.areTasksFullyAcknowledged()).isTrue();
        pending.finalizeCheckpoint(new CheckpointsCleaner(), () -> {}, Executors.directExecutor());
        assertThat(future.isDone()).isFalse();

        // Finalize (missing ACKs)
        PendingCheckpoint pendingCheckpoint = createPendingCheckpoint(props);
        future = pending.getCompletionFuture();

        assertThat(future.isDone()).isFalse();
        assertThatThrownBy(
                        () ->
                                pendingCheckpoint.finalizeCheckpoint(
                                        new CheckpointsCleaner(),
                                        () -> {},
                                        Executors.directExecutor()),
                        "Did not throw expected Exception")
                .isInstanceOf(IllegalStateException.class);
    }

    /** Tests that abort discards state. */
    @Test
    void testAbortDiscardsState() throws Exception {
        CheckpointProperties props =
                new CheckpointProperties(
                        false, CheckpointType.CHECKPOINT, false, false, false, false, false, false);
        QueueExecutor executor = new QueueExecutor();

        OperatorState state = mock(OperatorState.class);
        doNothing().when(state).registerSharedStates(any(SharedStateRegistry.class), eq(0L));

        // Abort declined
        PendingCheckpoint pending = createPendingCheckpoint(props, executor);
        setTaskState(pending, state);

        abort(pending, CheckpointFailureReason.CHECKPOINT_DECLINED);
        // execute asynchronous discard operation
        executor.runQueuedCommands();
        verify(state, times(1)).discardState();

        // Abort error
        Mockito.reset(state);

        pending = createPendingCheckpoint(props, executor);
        setTaskState(pending, state);

        abort(pending, CheckpointFailureReason.CHECKPOINT_DECLINED);
        // execute asynchronous discard operation
        executor.runQueuedCommands();
        verify(state, times(1)).discardState();

        // Abort expired
        Mockito.reset(state);

        pending = createPendingCheckpoint(props, executor);
        setTaskState(pending, state);

        abort(pending, CheckpointFailureReason.CHECKPOINT_EXPIRED);
        // execute asynchronous discard operation
        executor.runQueuedCommands();
        verify(state, times(1)).discardState();

        // Abort subsumed
        Mockito.reset(state);

        pending = createPendingCheckpoint(props, executor);
        setTaskState(pending, state);

        abort(pending, CheckpointFailureReason.CHECKPOINT_SUBSUMED);
        // execute asynchronous discard operation
        executor.runQueuedCommands();
        verify(state, times(1)).discardState();
    }

    /**
     * FLINK-5985.
     *
     * <p>Ensures that subtasks that acknowledge their state as 'null' are considered stateless.
     * This means that they should not appear in the task states map of the checkpoint.
     */
    @Test
    void testNullSubtaskStateLeadsToStatelessTask() throws Exception {
        PendingCheckpoint pending =
                createPendingCheckpoint(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
        pending.acknowledgeTask(ATTEMPT_ID, null, mock(CheckpointMetrics.class));
        final OperatorState expectedState =
                new OperatorState(OPERATOR_ID, PARALLELISM, MAX_PARALLELISM);
        assertThat(Collections.singletonMap(OPERATOR_ID, expectedState))
                .isEqualTo(pending.getOperatorStates());
    }

    /**
     * FLINK-5985.
     *
     * <p>This tests checks the inverse of {@link #testNullSubtaskStateLeadsToStatelessTask()}. We
     * want to test that for subtasks that acknowledge some state are given an entry in the task
     * states of the checkpoint.
     */
    @Test
    void testNonNullSubtaskStateLeadsToStatefulTask() throws Exception {
        PendingCheckpoint pending =
                createPendingCheckpoint(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
        pending.acknowledgeTask(
                ATTEMPT_ID, mock(TaskStateSnapshot.class), mock(CheckpointMetrics.class));
        assertThat(pending.getOperatorStates()).isNotEmpty();
    }

    @Test
    void testSetCanceller() throws Exception {
        final CheckpointProperties props =
                new CheckpointProperties(
                        false, CheckpointType.CHECKPOINT, true, true, true, true, true, false);

        PendingCheckpoint aborted = createPendingCheckpoint(props);
        abort(aborted, CheckpointFailureReason.CHECKPOINT_DECLINED);
        assertThat(aborted.isDisposed()).isTrue();
        assertThat(aborted.setCancellerHandle(mock(ScheduledFuture.class))).isFalse();

        PendingCheckpoint pending = createPendingCheckpoint(props);
        ScheduledFuture<?> canceller = mock(ScheduledFuture.class);

        assertThat(pending.setCancellerHandle(canceller)).isTrue();
        abort(pending, CheckpointFailureReason.CHECKPOINT_DECLINED);
        verify(canceller).cancel(false);
    }

    @Test
    void testMasterState() throws Exception {
        final TestingMasterTriggerRestoreHook masterHook =
                new TestingMasterTriggerRestoreHook("master hook");
        masterHook.addStateContent("state");

        final PendingCheckpoint pending =
                createPendingCheckpoint(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        Collections.singletonList(masterHook.getIdentifier()));

        final MasterState masterState =
                MasterHooks.triggerHook(
                                masterHook,
                                0,
                                System.currentTimeMillis(),
                                Executors.directExecutor())
                        .get();

        pending.acknowledgeMasterState(masterHook.getIdentifier(), masterState);
        assertThat(pending.areMasterStatesFullyAcknowledged()).isTrue();
        assertThat(pending.areTasksFullyAcknowledged()).isFalse();

        pending.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics());
        assertThat(pending.areTasksFullyAcknowledged()).isTrue();

        final List<MasterState> resultMasterStates = pending.getMasterStates();
        assertThat(resultMasterStates).hasSize(1);
        final String deserializedState =
                masterHook
                        .createCheckpointDataSerializer()
                        .deserialize(StringSerializer.VERSION, resultMasterStates.get(0).bytes());
        assertThat("state").isEqualTo(deserializedState);
    }

    @Test
    void testMasterStateWithNullState() throws Exception {
        final TestingMasterTriggerRestoreHook masterHook =
                new TestingMasterTriggerRestoreHook("master hook");
        masterHook.addStateContent("state");

        final TestingMasterTriggerRestoreHook nullableMasterHook =
                new TestingMasterTriggerRestoreHook("nullable master hook");

        final List<String> masterIdentifiers = new ArrayList<>(2);
        masterIdentifiers.add(masterHook.getIdentifier());
        masterIdentifiers.add(nullableMasterHook.getIdentifier());

        final PendingCheckpoint pending =
                createPendingCheckpoint(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        masterIdentifiers);

        final MasterState masterStateNormal =
                MasterHooks.triggerHook(
                                masterHook,
                                0,
                                System.currentTimeMillis(),
                                Executors.directExecutor())
                        .get();

        pending.acknowledgeMasterState(masterHook.getIdentifier(), masterStateNormal);
        assertThat(pending.areMasterStatesFullyAcknowledged()).isFalse();

        final MasterState masterStateNull =
                MasterHooks.triggerHook(
                                nullableMasterHook,
                                0,
                                System.currentTimeMillis(),
                                Executors.directExecutor())
                        .get();
        pending.acknowledgeMasterState(nullableMasterHook.getIdentifier(), masterStateNull);
        assertThat(pending.areMasterStatesFullyAcknowledged()).isTrue();
        assertThat(pending.areTasksFullyAcknowledged()).isFalse();

        pending.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics());
        assertThat(pending.areTasksFullyAcknowledged()).isTrue();

        final List<MasterState> resultMasterStates = pending.getMasterStates();
        assertThat(resultMasterStates).hasSize(1);
        final String deserializedState =
                masterHook
                        .createCheckpointDataSerializer()
                        .deserialize(StringSerializer.VERSION, resultMasterStates.get(0).bytes());
        assertThat("state").isEqualTo(deserializedState);
    }

    @Test
    void testInitiallyUnacknowledgedCoordinatorStates() throws Exception {
        final PendingCheckpoint checkpoint =
                createPendingCheckpointWithCoordinators(
                        new TestingOperatorInfo(), new TestingOperatorInfo());

        assertThat(checkpoint.getNumberOfNonAcknowledgedOperatorCoordinators()).isEqualTo(2);
        assertThat(checkpoint.isFullyAcknowledged()).isFalse();
    }

    @Test
    void testAcknowledgedCoordinatorStates() throws Exception {
        final OperatorInfo coord1 = new TestingOperatorInfo();
        final OperatorInfo coord2 = new TestingOperatorInfo();
        final PendingCheckpoint checkpoint =
                createPendingCheckpointWithCoordinators(coord1, coord2);

        final TaskAcknowledgeResult ack1 =
                checkpoint.acknowledgeCoordinatorState(coord1, new TestingStreamStateHandle());
        final TaskAcknowledgeResult ack2 = checkpoint.acknowledgeCoordinatorState(coord2, null);

        assertThat(TaskAcknowledgeResult.SUCCESS).isEqualTo(ack1);
        assertThat(TaskAcknowledgeResult.SUCCESS).isEqualTo(ack2);
        assertThat(0).isEqualTo(checkpoint.getNumberOfNonAcknowledgedOperatorCoordinators());
        assertThat(checkpoint.isFullyAcknowledged()).isTrue();
        assertThat(checkpoint.getOperatorStates().keySet())
                .contains(OPERATOR_ID, coord1.operatorId(), coord2.operatorId());
    }

    @Test
    void testDuplicateAcknowledgeCoordinator() throws Exception {
        final OperatorInfo coordinator = new TestingOperatorInfo();
        final PendingCheckpoint checkpoint = createPendingCheckpointWithCoordinators(coordinator);

        checkpoint.acknowledgeCoordinatorState(coordinator, new TestingStreamStateHandle());
        final TaskAcknowledgeResult secondAck =
                checkpoint.acknowledgeCoordinatorState(coordinator, null);

        assertThat(TaskAcknowledgeResult.DUPLICATE).isEqualTo(secondAck);
    }

    @Test
    void testAcknowledgeUnknownCoordinator() throws Exception {
        final PendingCheckpoint checkpoint =
                createPendingCheckpointWithCoordinators(new TestingOperatorInfo());

        final TaskAcknowledgeResult ack =
                checkpoint.acknowledgeCoordinatorState(new TestingOperatorInfo(), null);

        assertThat(TaskAcknowledgeResult.UNKNOWN).isEqualTo(ack);
    }

    @Test
    void testDisposeDisposesCoordinatorStates() throws Exception {
        final TestingStreamStateHandle handle1 = new TestingStreamStateHandle();
        final TestingStreamStateHandle handle2 = new TestingStreamStateHandle();
        final PendingCheckpoint checkpoint =
                createPendingCheckpointWithAcknowledgedCoordinators(handle1, handle2);

        abort(checkpoint, CheckpointFailureReason.CHECKPOINT_EXPIRED);

        assertThat(handle1.isDisposed()).isTrue();
        assertThat(handle2.isDisposed()).isTrue();
    }

    @Test
    void testReportTaskFinishedOnRestore() throws IOException {
        RecordCheckpointPlan recordCheckpointPlan =
                new RecordCheckpointPlan(new ArrayList<>(ACK_TASKS));
        PendingCheckpoint checkpoint = createPendingCheckpoint(recordCheckpointPlan);
        checkpoint.acknowledgeTask(
                ACK_TASKS.get(0).getAttemptId(),
                TaskStateSnapshot.FINISHED_ON_RESTORE,
                new CheckpointMetrics());
        assertThat(recordCheckpointPlan.getReportedFinishedOnRestoreTasks())
                .contains(ACK_TASKS.get(0).getVertex());
    }

    @Test
    void testReportTaskFinishedOperators() throws IOException {
        RecordCheckpointPlan recordCheckpointPlan =
                new RecordCheckpointPlan(new ArrayList<>(ACK_TASKS));
        PendingCheckpoint checkpoint = createPendingCheckpoint(recordCheckpointPlan);
        checkpoint.acknowledgeTask(
                ACK_TASKS.get(0).getAttemptId(),
                new TaskStateSnapshot(10, true),
                new CheckpointMetrics());
        assertThat(recordCheckpointPlan.getReportedOperatorsFinishedTasks())
                .contains(ACK_TASKS.get(0).getVertex());
    }

    // ------------------------------------------------------------------------

    private PendingCheckpoint createPendingCheckpoint(CheckpointProperties props)
            throws IOException {
        return createPendingCheckpoint(
                props,
                Collections.emptyList(),
                Collections.emptyList(),
                Executors.directExecutor());
    }

    private PendingCheckpoint createPendingCheckpoint(CheckpointProperties props, Executor executor)
            throws IOException {
        return createPendingCheckpoint(
                props, Collections.emptyList(), Collections.emptyList(), executor);
    }

    private PendingCheckpoint createPendingCheckpoint(
            CheckpointProperties props, Collection<String> masterStateIdentifiers)
            throws IOException {
        return createPendingCheckpoint(
                props, Collections.emptyList(), masterStateIdentifiers, Executors.directExecutor());
    }

    private PendingCheckpoint createPendingCheckpoint(CheckpointPlan checkpointPlan)
            throws IOException {
        return createPendingCheckpoint(
                CheckpointProperties.forCheckpoint(
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                Collections.emptyList(),
                Collections.emptyList(),
                checkpointPlan,
                Executors.directExecutor());
    }

    private PendingCheckpoint createPendingCheckpointWithCoordinators(OperatorInfo... coordinators)
            throws IOException {

        final PendingCheckpoint checkpoint =
                createPendingCheckpoint(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        OperatorInfo.getIds(Arrays.asList(coordinators)),
                        Collections.emptyList(),
                        Executors.directExecutor());

        checkpoint.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics());
        return checkpoint;
    }

    private PendingCheckpoint createPendingCheckpointWithAcknowledgedCoordinators(
            ByteStreamStateHandle... handles) throws IOException {
        final OperatorInfo[] coords = new OperatorInfo[handles.length];
        for (int i = 0; i < handles.length; i++) {
            coords[i] = new TestingOperatorInfo();
        }

        final PendingCheckpoint checkpoint = createPendingCheckpointWithCoordinators(coords);
        for (int i = 0; i < handles.length; i++) {
            checkpoint.acknowledgeCoordinatorState(coords[i], handles[i]);
        }

        return checkpoint;
    }

    private PendingCheckpoint createPendingCheckpoint(
            CheckpointProperties props,
            Collection<OperatorID> operatorCoordinators,
            Collection<String> masterStateIdentifiers,
            Executor executor)
            throws IOException {

        final List<Execution> ackTasks = new ArrayList<>(ACK_TASKS);
        final List<ExecutionVertex> tasksToCommit = new ArrayList<>(TASKS_TO_COMMIT);
        return createPendingCheckpoint(
                props,
                operatorCoordinators,
                masterStateIdentifiers,
                new DefaultCheckpointPlan(
                        Collections.emptyList(),
                        ackTasks,
                        tasksToCommit,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        true),
                executor);
    }

    private PendingCheckpoint createPendingCheckpoint(
            CheckpointProperties props,
            Collection<OperatorID> operatorCoordinators,
            Collection<String> masterStateIdentifiers,
            CheckpointPlan checkpointPlan,
            Executor executor)
            throws IOException {

        final Path checkpointDir = new Path(TempDirUtils.newFolder(tmpFolder).toURI());
        final FsCheckpointStorageLocation location =
                new FsCheckpointStorageLocation(
                        LocalFileSystem.getSharedInstance(),
                        checkpointDir,
                        checkpointDir,
                        checkpointDir,
                        CheckpointStorageLocationReference.getDefault(),
                        1024,
                        4096);

        PendingCheckpoint pendingCheckpoint =
                new PendingCheckpoint(
                        new JobID(),
                        0,
                        1,
                        checkpointPlan,
                        operatorCoordinators,
                        masterStateIdentifiers,
                        props,
                        new CompletableFuture<>(),
                        null,
                        new CompletableFuture<>());
        pendingCheckpoint.setCheckpointTargetLocation(location);
        return pendingCheckpoint;
    }

    @SuppressWarnings("unchecked")
    static void setTaskState(PendingCheckpoint pending, OperatorState state)
            throws NoSuchFieldException, IllegalAccessException {
        Field field = PendingCheckpoint.class.getDeclaredField("operatorStates");
        field.setAccessible(true);
        Map<OperatorID, OperatorState> taskStates =
                (Map<OperatorID, OperatorState>) field.get(pending);

        taskStates.put(new OperatorID(), state);
    }

    private void abort(PendingCheckpoint checkpoint, CheckpointFailureReason reason) {
        checkpoint.abort(
                reason, null, new CheckpointsCleaner(), () -> {}, Executors.directExecutor(), null);
    }

    private static final class QueueExecutor implements Executor {

        private final Queue<Runnable> queue = new ArrayDeque<>(4);

        @Override
        public void execute(Runnable command) {
            queue.add(command);
        }

        public void runQueuedCommands() {
            for (Runnable runnable : queue) {
                runnable.run();
            }
        }
    }

    private static final class TestingMasterTriggerRestoreHook
            implements MasterTriggerRestoreHook<String> {

        private final String identifier;
        private final ArrayDeque<String> stateContents;

        public TestingMasterTriggerRestoreHook(String identifier) {
            this.identifier = checkNotNull(identifier);
            stateContents = new ArrayDeque<>();
        }

        public void addStateContent(String stateContent) {
            stateContents.add(stateContent);
        }

        @Override
        public String getIdentifier() {
            return identifier;
        }

        @Nullable
        @Override
        public CompletableFuture<String> triggerCheckpoint(
                long checkpointId, long timestamp, Executor executor) {
            return CompletableFuture.completedFuture(stateContents.poll());
        }

        @Override
        public void restoreCheckpoint(long checkpointId, @Nullable String checkpointData) {}

        @Override
        public SimpleVersionedSerializer<String> createCheckpointDataSerializer() {
            return new StringSerializer();
        }
    }

    private static class RecordCheckpointPlan extends DefaultCheckpointPlan {

        private final List<ExecutionVertex> reportedFinishedOnRestoreTasks = new ArrayList<>();

        private final List<ExecutionVertex> reportedOperatorsFinishedTasks = new ArrayList<>();

        public RecordCheckpointPlan(List<Execution> tasksToWaitFor) {
            super(
                    Collections.emptyList(),
                    tasksToWaitFor,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    Collections.emptyList(),
                    true);
        }

        @Override
        public void reportTaskFinishedOnRestore(ExecutionVertex task) {
            super.reportTaskFinishedOnRestore(task);
            reportedFinishedOnRestoreTasks.add(task);
        }

        @Override
        public void reportTaskHasFinishedOperators(ExecutionVertex task) {
            super.reportTaskHasFinishedOperators(task);
            reportedOperatorsFinishedTasks.add(task);
        }

        public List<ExecutionVertex> getReportedFinishedOnRestoreTasks() {
            return reportedFinishedOnRestoreTasks;
        }

        public List<ExecutionVertex> getReportedOperatorsFinishedTasks() {
            return reportedOperatorsFinishedTasks;
        }
    }
}
