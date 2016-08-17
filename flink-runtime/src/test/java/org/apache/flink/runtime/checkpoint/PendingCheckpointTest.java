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
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PendingCheckpointTest {

	private static final Map<ExecutionAttemptID, ExecutionVertex> ACK_TASKS = new HashMap<>();
	private static final ExecutionAttemptID ATTEMPT_ID = new ExecutionAttemptID();

	static {
		ACK_TASKS.put(ATTEMPT_ID, mock(ExecutionVertex.class));
	}

	/**
	 * Tests that pending checkpoints can be subsumed.
	 */
	@Test
	public void testCanBeSubsumed() throws Exception {
		PendingCheckpoint pending = createPendingCheckpoint();
		assertTrue(pending.canBeSubsumed());
	}

	/**
	 * Tests that abort discards state.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testAbort() throws Exception {
		TaskState state = mock(TaskState.class);

		// Abort declined
		PendingCheckpoint pending = createPendingCheckpoint();
		setTaskState(pending, state);

		pending.abortDeclined();
		verify(state, times(1)).discard(Matchers.any(ClassLoader.class));

		// Abort error
		Mockito.reset(state);

		pending = createPendingCheckpoint();
		setTaskState(pending, state);

		pending.abortError(new Exception("Expected Test Exception"));
		verify(state, times(1)).discard(Matchers.any(ClassLoader.class));

		// Abort expired
		Mockito.reset(state);

		pending = createPendingCheckpoint();
		setTaskState(pending, state);

		pending.abortExpired();
		verify(state, times(1)).discard(Matchers.any(ClassLoader.class));

		// Abort subsumed
		Mockito.reset(state);

		pending = createPendingCheckpoint();
		setTaskState(pending, state);

		pending.abortSubsumed();
		verify(state, times(1)).discard(Matchers.any(ClassLoader.class));
	}

	/**
	 * Tests that the CompletedCheckpoint `deleteStateWhenDisposed` flag is
	 * correctly set to true.
	 */
	@Test
	public void testFinalizeCheckpoint() throws Exception {
		TaskState state = mock(TaskState.class);
		PendingCheckpoint pending = createPendingCheckpoint();
		PendingCheckpointTest.setTaskState(pending, state);

		pending.acknowledgeTask(ATTEMPT_ID, null, 0, null);

		CompletedCheckpoint checkpoint = pending.finalizeCheckpoint();

		// Does discard state
		checkpoint.discard(ClassLoader.getSystemClassLoader());
		verify(state, times(1)).discard(Matchers.any(ClassLoader.class));
	}

	// ------------------------------------------------------------------------

	private static PendingCheckpoint createPendingCheckpoint() {
		ClassLoader classLoader = ClassLoader.getSystemClassLoader();
		Map<ExecutionAttemptID, ExecutionVertex> ackTasks = new HashMap<>(ACK_TASKS);
		return new PendingCheckpoint(new JobID(), 0, 1, ackTasks, classLoader);
	}

	@SuppressWarnings("unchecked")
	static void setTaskState(PendingCheckpoint pending, TaskState state) throws NoSuchFieldException, IllegalAccessException {
		Field field = PendingCheckpoint.class.getDeclaredField("taskStates");
		field.setAccessible(true);
		Map<JobVertexID, TaskState> taskStates = (Map<JobVertexID, TaskState>) field.get(pending);

		taskStates.put(new JobVertexID(), state);
	}
}
