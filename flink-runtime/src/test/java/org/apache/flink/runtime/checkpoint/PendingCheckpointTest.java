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
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PendingCheckpointTest {

	@Rule
	public TemporaryFolder tmpFolder = new TemporaryFolder();

	private static final Map<ExecutionAttemptID, ExecutionVertex> ACK_TASKS = new HashMap<>();
	private static final ExecutionAttemptID ATTEMPT_ID = new ExecutionAttemptID();

	static {
		ACK_TASKS.put(ATTEMPT_ID, mock(ExecutionVertex.class));
	}

	/**
	 * Tests that pending checkpoints can be subsumed iff they are forced.
	 */
	@Test
	public void testCanBeSubsumed() throws Exception {
		// Forced checkpoints cannot be subsumed
		CheckpointProperties forced = new CheckpointProperties(true, true, false, false, false, false, false);
		PendingCheckpoint pending = createPendingCheckpoint(forced, "ignored");
		assertFalse(pending.canBeSubsumed());

		try {
			pending.abortSubsumed();
			fail("Did not throw expected Exception");
		} catch (IllegalStateException ignored) {
			// Expected
		}

		// Non-forced checkpoints can be subsumed
		CheckpointProperties subsumed = new CheckpointProperties(false, true, false, false, false, false, false);
		pending = createPendingCheckpoint(subsumed, "ignored");
		assertTrue(pending.canBeSubsumed());
	}

	/**
	 * Tests that the persist checkpoint property is respected by the pending
	 * checkpoint when finalizing.
	 */
	@Test
	public void testPersistExternally() throws Exception {
		File tmp = tmpFolder.newFolder();

		// Persisted checkpoint
		CheckpointProperties persisted = new CheckpointProperties(false, true, false, false, false, false, false);

		PendingCheckpoint pending = createPendingCheckpoint(persisted, tmp.getAbsolutePath());
		pending.acknowledgeTask(ATTEMPT_ID, null);

		assertEquals(0, tmp.listFiles().length);
		pending.finalizeCheckpoint();
		assertEquals(1, tmp.listFiles().length);

		// Ephemeral checkpoint
		CheckpointProperties ephemeral = new CheckpointProperties(false, false, true, true, true, true, true);
		pending = createPendingCheckpoint(ephemeral, null);
		pending.acknowledgeTask(ATTEMPT_ID, null);

		assertEquals(1, tmp.listFiles().length);
		pending.finalizeCheckpoint();
		assertEquals(1, tmp.listFiles().length);
	}

	/**
	 * Tests that the completion future is succeeded on finalize and failed on
	 * abort and failures during finalize.
	 */
	@Test
	public void testCompletionFuture() throws Exception {
		CheckpointProperties props = new CheckpointProperties(false, true, false, false, false, false, false);

		// Abort declined
		PendingCheckpoint pending = createPendingCheckpoint(props, "ignored");
		Future<CompletedCheckpoint> future = pending.getCompletionFuture();

		assertFalse(future.isDone());
		pending.abortDeclined();
		assertTrue(future.isDone());

		// Abort expired
		pending = createPendingCheckpoint(props, "ignored");
		future = pending.getCompletionFuture();

		assertFalse(future.isDone());
		pending.abortExpired();
		assertTrue(future.isDone());

		// Abort subsumed
		pending = createPendingCheckpoint(props, "ignored");
		future = pending.getCompletionFuture();

		assertFalse(future.isDone());
		pending.abortSubsumed();
		assertTrue(future.isDone());

		// Finalize (all ACK'd)
		String target = tmpFolder.newFolder().getAbsolutePath();
		pending = createPendingCheckpoint(props, target);
		future = pending.getCompletionFuture();

		assertFalse(future.isDone());
		pending.acknowledgeTask(ATTEMPT_ID, null);
		pending.finalizeCheckpoint();
		assertTrue(future.isDone());

		// Finalize (missing ACKs)
		pending = createPendingCheckpoint(props, "ignored");
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
		CheckpointProperties props = new CheckpointProperties(false, true, false, false, false, false, false);
		TaskState state = mock(TaskState.class);

		String targetDir = tmpFolder.newFolder().getAbsolutePath();

		// Abort declined
		PendingCheckpoint pending = createPendingCheckpoint(props, targetDir);
		setTaskState(pending, state);

		pending.abortDeclined();
		verify(state, times(1)).discardState();

		// Abort error
		Mockito.reset(state);

		pending = createPendingCheckpoint(props, targetDir);
		setTaskState(pending, state);

		pending.abortError(new Exception("Expected Test Exception"));
		verify(state, times(1)).discardState();

		// Abort expired
		Mockito.reset(state);

		pending = createPendingCheckpoint(props, targetDir);
		setTaskState(pending, state);

		pending.abortExpired();
		verify(state, times(1)).discardState();

		// Abort subsumed
		Mockito.reset(state);

		pending = createPendingCheckpoint(props, targetDir);
		setTaskState(pending, state);

		pending.abortSubsumed();
		verify(state, times(1)).discardState();
	}

	// ------------------------------------------------------------------------

	private static PendingCheckpoint createPendingCheckpoint(CheckpointProperties props, String targetDirectory) {
		Map<ExecutionAttemptID, ExecutionVertex> ackTasks = new HashMap<>(ACK_TASKS);
		return new PendingCheckpoint(
			new JobID(),
			0,
			1,
			ackTasks,
			false,
			props,
			targetDirectory,
			Executors.directExecutor());
	}

	@SuppressWarnings("unchecked")
	static void setTaskState(PendingCheckpoint pending, TaskState state) throws NoSuchFieldException, IllegalAccessException {
		Field field = PendingCheckpoint.class.getDeclaredField("taskStates");
		field.setAccessible(true);
		Map<JobVertexID, TaskState> taskStates = (Map<JobVertexID, TaskState>) field.get(pending);

		taskStates.put(new JobVertexID(), state);
	}
}
