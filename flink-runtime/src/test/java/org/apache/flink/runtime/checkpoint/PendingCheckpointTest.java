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
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.local.LocalFileSystem;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorageLocation;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for the {@link PendingCheckpoint}.
 */
public class PendingCheckpointTest {

	private static final Map<ExecutionAttemptID, ExecutionVertex> ACK_TASKS = new HashMap<>();
	private static final ExecutionAttemptID ATTEMPT_ID = new ExecutionAttemptID();

	static {
		ExecutionJobVertex jobVertex = mock(ExecutionJobVertex.class);
		when(jobVertex.getOperatorIDs()).thenReturn(Collections.singletonList(new OperatorID()));

		ExecutionVertex vertex = mock(ExecutionVertex.class);
		when(vertex.getMaxParallelism()).thenReturn(128);
		when(vertex.getTotalNumberOfParallelSubtasks()).thenReturn(1);
		when(vertex.getJobVertex()).thenReturn(jobVertex);
		ACK_TASKS.put(ATTEMPT_ID, vertex);
	}

	@Rule
	public final TemporaryFolder tmpFolder = new TemporaryFolder();

	/**
	 * Tests that pending checkpoints can be subsumed iff they are forced.
	 */
	@Test
	public void testCanBeSubsumed() throws Exception {
		// Forced checkpoints cannot be subsumed
		CheckpointProperties forced = new CheckpointProperties(true, CheckpointType.SAVEPOINT, false, false, false, false, false);
		PendingCheckpoint pending = createPendingCheckpoint(forced);
		assertFalse(pending.canBeSubsumed());

		try {
			pending.abortSubsumed();
			fail("Did not throw expected Exception");
		} catch (IllegalStateException ignored) {
			// Expected
		}

		// Non-forced checkpoints can be subsumed
		CheckpointProperties subsumed = new CheckpointProperties(false, CheckpointType.SAVEPOINT, false, false, false, false, false);
		pending = createPendingCheckpoint(subsumed);
		assertTrue(pending.canBeSubsumed());
	}

	/**
	 * Tests that the completion future is succeeded on finalize and failed on
	 * abort and failures during finalize.
	 */
	@Test
	public void testCompletionFuture() throws Exception {
		CheckpointProperties props = new CheckpointProperties(false, CheckpointType.SAVEPOINT, false, false, false, false, false);

		// Abort declined
		PendingCheckpoint pending = createPendingCheckpoint(props);
		CompletableFuture<CompletedCheckpoint> future = pending.getCompletionFuture();

		assertFalse(future.isDone());
		pending.abortDeclined();
		assertTrue(future.isDone());

		// Abort expired
		pending = createPendingCheckpoint(props);
		future = pending.getCompletionFuture();

		assertFalse(future.isDone());
		pending.abortExpired();
		assertTrue(future.isDone());

		// Abort subsumed
		pending = createPendingCheckpoint(props);
		future = pending.getCompletionFuture();

		assertFalse(future.isDone());
		pending.abortSubsumed();
		assertTrue(future.isDone());

		// Finalize (all ACK'd)
		pending = createPendingCheckpoint(props);
		future = pending.getCompletionFuture();

		assertFalse(future.isDone());
		pending.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics());
		assertTrue(pending.isFullyAcknowledged());
		pending.finalizeCheckpoint();
		assertTrue(future.isDone());

		// Finalize (missing ACKs)
		pending = createPendingCheckpoint(props);
		future = pending.getCompletionFuture();

		assertFalse(future.isDone());
		try {
			pending.finalizeCheckpoint();
			fail("Did not throw expected Exception");
		} catch (IllegalStateException ignored) {
			// Expected
		}
	}

	/**
	 * Tests that abort discards state.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testAbortDiscardsState() throws Exception {
		CheckpointProperties props = new CheckpointProperties(false, CheckpointType.SAVEPOINT, false, false, false, false, false);
		QueueExecutor executor = new QueueExecutor();

		OperatorState state = mock(OperatorState.class);
		doNothing().when(state).registerSharedStates(any(SharedStateRegistry.class));

		// Abort declined
		PendingCheckpoint pending = createPendingCheckpoint(props, executor);
		setTaskState(pending, state);

		pending.abortDeclined();
		// execute asynchronous discard operation
		executor.runQueuedCommands();
		verify(state, times(1)).discardState();

		// Abort error
		Mockito.reset(state);

		pending = createPendingCheckpoint(props, executor);
		setTaskState(pending, state);

		pending.abortError(new Exception("Expected Test Exception"));
		// execute asynchronous discard operation
		executor.runQueuedCommands();
		verify(state, times(1)).discardState();

		// Abort expired
		Mockito.reset(state);

		pending = createPendingCheckpoint(props, executor);
		setTaskState(pending, state);

		pending.abortExpired();
		// execute asynchronous discard operation
		executor.runQueuedCommands();
		verify(state, times(1)).discardState();

		// Abort subsumed
		Mockito.reset(state);

		pending = createPendingCheckpoint(props, executor);
		setTaskState(pending, state);

		pending.abortSubsumed();
		// execute asynchronous discard operation
		executor.runQueuedCommands();
		verify(state, times(1)).discardState();
	}

	/**
	 * Tests that the stats callbacks happen if the callback is registered.
	 */
	@Test
	public void testPendingCheckpointStatsCallbacks() throws Exception {
		{
			// Complete successfully
			PendingCheckpointStats callback = mock(PendingCheckpointStats.class);
			PendingCheckpoint pending = createPendingCheckpoint(
					CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
			pending.setStatsCallback(callback);

			pending.acknowledgeTask(ATTEMPT_ID, null, new CheckpointMetrics());
			verify(callback, times(1)).reportSubtaskStats(nullable(JobVertexID.class), any(SubtaskStateStats.class));

			pending.finalizeCheckpoint();
			verify(callback, times(1)).reportCompletedCheckpoint(any(String.class));
		}

		{
			// Fail subsumed
			PendingCheckpointStats callback = mock(PendingCheckpointStats.class);
			PendingCheckpoint pending = createPendingCheckpoint(
					CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
			pending.setStatsCallback(callback);

			pending.abortSubsumed();
			verify(callback, times(1)).reportFailedCheckpoint(anyLong(), any(Exception.class));
		}

		{
			// Fail subsumed
			PendingCheckpointStats callback = mock(PendingCheckpointStats.class);
			PendingCheckpoint pending = createPendingCheckpoint(
					CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
			pending.setStatsCallback(callback);

			pending.abortDeclined();
			verify(callback, times(1)).reportFailedCheckpoint(anyLong(), any(Exception.class));
		}

		{
			// Fail subsumed
			PendingCheckpointStats callback = mock(PendingCheckpointStats.class);
			PendingCheckpoint pending = createPendingCheckpoint(
					CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
			pending.setStatsCallback(callback);

			pending.abortError(new Exception("Expected test error"));
			verify(callback, times(1)).reportFailedCheckpoint(anyLong(), any(Exception.class));
		}

		{
			// Fail subsumed
			PendingCheckpointStats callback = mock(PendingCheckpointStats.class);
			PendingCheckpoint pending = createPendingCheckpoint(
					CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
			pending.setStatsCallback(callback);

			pending.abortExpired();
			verify(callback, times(1)).reportFailedCheckpoint(anyLong(), any(Exception.class));
		}
	}

	/**
	 * FLINK-5985.
	 *
	 * <p>Ensures that subtasks that acknowledge their state as 'null' are considered stateless. This means that they
	 * should not appear in the task states map of the checkpoint.
	 */
	@Test
	public void testNullSubtaskStateLeadsToStatelessTask() throws Exception {
		PendingCheckpoint pending = createPendingCheckpoint(
				CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
		pending.acknowledgeTask(ATTEMPT_ID, null, mock(CheckpointMetrics.class));
		Assert.assertTrue(pending.getOperatorStates().isEmpty());
	}

	/**
	 * FLINK-5985.
	 *
	 * <p>This tests checks the inverse of {@link #testNullSubtaskStateLeadsToStatelessTask()}. We want to test that
	 * for subtasks that acknowledge some state are given an entry in the task states of the checkpoint.
	 */
	@Test
	public void testNonNullSubtaskStateLeadsToStatefulTask() throws Exception {
		PendingCheckpoint pending = createPendingCheckpoint(
				CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION));
		pending.acknowledgeTask(ATTEMPT_ID, mock(TaskStateSnapshot.class), mock(CheckpointMetrics.class));
		Assert.assertFalse(pending.getOperatorStates().isEmpty());
	}

	@Test
	public void testSetCanceller() throws Exception {
		final CheckpointProperties props = new CheckpointProperties(false, CheckpointType.CHECKPOINT, true, true, true, true, true);

		PendingCheckpoint aborted = createPendingCheckpoint(props);
		aborted.abortDeclined();
		assertTrue(aborted.isDiscarded());
		assertFalse(aborted.setCancellerHandle(mock(ScheduledFuture.class)));

		PendingCheckpoint pending = createPendingCheckpoint(props);
		ScheduledFuture<?> canceller = mock(ScheduledFuture.class);

		assertTrue(pending.setCancellerHandle(canceller));
		pending.abortDeclined();
		verify(canceller).cancel(false);
	}

	// ------------------------------------------------------------------------

	private PendingCheckpoint createPendingCheckpoint(CheckpointProperties props) throws IOException {
		return createPendingCheckpoint(props, Executors.directExecutor());
	}

	private PendingCheckpoint createPendingCheckpoint(CheckpointProperties props, Executor executor) throws IOException {

		final Path checkpointDir = new Path(tmpFolder.newFolder().toURI());
		final FsCheckpointStorageLocation location = new FsCheckpointStorageLocation(
				LocalFileSystem.getSharedInstance(),
				checkpointDir, checkpointDir, checkpointDir,
				CheckpointStorageLocationReference.getDefault(),
				1024);

		final Map<ExecutionAttemptID, ExecutionVertex> ackTasks = new HashMap<>(ACK_TASKS);

		return new PendingCheckpoint(
			new JobID(),
			0,
			1,
			ackTasks,
			props,
			location,
			executor);
	}

	@SuppressWarnings("unchecked")
	static void setTaskState(PendingCheckpoint pending, OperatorState state) throws NoSuchFieldException, IllegalAccessException {
		Field field = PendingCheckpoint.class.getDeclaredField("operatorStates");
		field.setAccessible(true);
		Map<OperatorID, OperatorState> taskStates = (Map<OperatorID, OperatorState>) field.get(pending);

		taskStates.put(new OperatorID(), state);
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
}
