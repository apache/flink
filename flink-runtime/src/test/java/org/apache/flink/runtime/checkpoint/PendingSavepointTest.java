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
import org.apache.flink.runtime.checkpoint.savepoint.HeapSavepointStore;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PendingSavepointTest {

	private static final Map<ExecutionAttemptID, ExecutionVertex> ACK_TASKS = new HashMap<>();
	private static final ExecutionAttemptID ATTEMPT_ID = new ExecutionAttemptID();

	static {
		ACK_TASKS.put(ATTEMPT_ID, mock(ExecutionVertex.class));
	}

	/**
	 * Tests that pending savepoints cannot be subsumed.
	 */
	@Test
	public void testCanBeSubsumed() throws Exception {
		PendingSavepoint pending = createPendingSavepoint();
		assertFalse(pending.canBeSubsumed());
	}

	/**
	 * Tests that abort discards state fails the completeion future.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testAbort() throws Exception {
		TaskState state = mock(TaskState.class);

		// Abort declined
		PendingSavepoint pending = createPendingSavepoint();
		PendingCheckpointTest.setTaskState(pending, state);

		pending.abortDeclined();
		verify(state, times(1)).discard(Matchers.any(ClassLoader.class));

		// Abort error
		Mockito.reset(state);

		pending = createPendingSavepoint();
		PendingCheckpointTest.setTaskState(pending, state);
		Future<String> future = pending.getCompletionFuture();

		pending.abortError(new Exception("Expected Test Exception"));
		verify(state, times(1)).discard(Matchers.any(ClassLoader.class));
		assertTrue(future.failed().isCompleted());

		// Abort expired
		Mockito.reset(state);

		pending = createPendingSavepoint();
		PendingCheckpointTest.setTaskState(pending, state);
		future = pending.getCompletionFuture();

		pending.abortExpired();
		verify(state, times(1)).discard(Matchers.any(ClassLoader.class));
		assertTrue(future.failed().isCompleted());

		// Abort subsumed
		pending = createPendingSavepoint();

		try {
			pending.abortSubsumed();
			fail("Did not throw expected Exception");
		} catch (Throwable ignored) { // expected
		}
	}

	/**
	 * Tests that the CompletedCheckpoint `deleteStateWhenDisposed` flag is
	 * correctly set to false.
	 */
	@Test
	public void testFinalizeCheckpoint() throws Exception {
		TaskState state = mock(TaskState.class);
		PendingSavepoint pending = createPendingSavepoint();
		PendingCheckpointTest.setTaskState(pending, state);

		Future<String> future = pending.getCompletionFuture();

		pending.acknowledgeTask(ATTEMPT_ID, null, 0, null);

		CompletedCheckpoint checkpoint = pending.finalizeCheckpoint();

		// Does _NOT_ discard state
		checkpoint.discard(ClassLoader.getSystemClassLoader());
		verify(state, times(0)).discard(Matchers.any(ClassLoader.class));

		// Future is completed
		String path = Await.result(future, Duration.Zero());
		assertNotNull(path);
	}

	// ------------------------------------------------------------------------

	private static PendingSavepoint createPendingSavepoint() {
		ClassLoader classLoader = ClassLoader.getSystemClassLoader();
		Map<ExecutionAttemptID, ExecutionVertex> ackTasks = new HashMap<>(ACK_TASKS);
		return new PendingSavepoint(new JobID(), 0, 1, ackTasks, classLoader, new HeapSavepointStore());
	}

}
