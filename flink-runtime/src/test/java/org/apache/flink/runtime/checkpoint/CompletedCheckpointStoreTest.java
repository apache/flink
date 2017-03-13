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
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test for basic {@link CompletedCheckpointStore} contract.
 */
public abstract class CompletedCheckpointStoreTest extends TestLogger {

	/**
	 * Creates the {@link CompletedCheckpointStore} implementation to be tested.
	 */
	protected abstract CompletedCheckpointStore createCompletedCheckpoints(
			int maxNumberOfCheckpointsToRetain) throws Exception;

	// ---------------------------------------------------------------------------------------------

	/**
	 * Tests that at least one checkpoint needs to be retained.
	 */
	@Test(expected = Exception.class)
	public void testExceptionOnNoRetainedCheckpoints() throws Exception {
		createCompletedCheckpoints(0);
	}

	/**
	 * Tests adding and getting a checkpoint.
	 */
	@Test
	public void testAddAndGetLatestCheckpoint() throws Exception {
		CompletedCheckpointStore checkpoints = createCompletedCheckpoints(4);
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		
		// Empty state
		assertEquals(0, checkpoints.getNumberOfRetainedCheckpoints());
		assertEquals(0, checkpoints.getAllCheckpoints().size());

		TestCompletedCheckpoint[] expected = new TestCompletedCheckpoint[] {
				createCheckpoint(0), createCheckpoint(1) };

		// Add and get latest
		checkpoints.addCheckpoint(expected[0], sharedStateRegistry);
		assertEquals(1, checkpoints.getNumberOfRetainedCheckpoints());
		verifyCheckpoint(expected[0], checkpoints.getLatestCheckpoint());

		checkpoints.addCheckpoint(expected[1], sharedStateRegistry);
		assertEquals(2, checkpoints.getNumberOfRetainedCheckpoints());
		verifyCheckpoint(expected[1], checkpoints.getLatestCheckpoint());
	}

	/**
	 * Tests that adding more checkpoints than retained discards the correct checkpoints (using
	 * the correct class loader).
	 */
	@Test
	public void testAddCheckpointMoreThanMaxRetained() throws Exception {
		CompletedCheckpointStore checkpoints = createCompletedCheckpoints(1);   
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		TestCompletedCheckpoint[] expected = new TestCompletedCheckpoint[] {
				createCheckpoint(0), createCheckpoint(1),
				createCheckpoint(2), createCheckpoint(3)
		};

		// Add checkpoints
		checkpoints.addCheckpoint(expected[0], sharedStateRegistry);
		assertEquals(1, checkpoints.getNumberOfRetainedCheckpoints());

		for (int i = 1; i < expected.length; i++) {
			Collection<TaskState> taskStates = expected[i - 1].getTaskStates().values();

			checkpoints.addCheckpoint(expected[i], sharedStateRegistry);

			// The ZooKeeper implementation discards asynchronously
			expected[i - 1].awaitDiscard();
			assertTrue(expected[i - 1].isDiscarded());
			assertEquals(1, checkpoints.getNumberOfRetainedCheckpoints());

			for (TaskState taskState : taskStates) {
				for (SubtaskState subtaskState : taskState.getStates()) {
					verify(subtaskState, times(1)).unregisterSharedStates(sharedStateRegistry);
				}
			}
		}
	}

	/**
	 * Tests that
	 * <ul>
	 * <li>{@link CompletedCheckpointStore#getLatestCheckpoint()} returns <code>null</code>,</li>
	 * <li>{@link CompletedCheckpointStore#getAllCheckpoints()} returns an empty list,</li>
	 * <li>{@link CompletedCheckpointStore#getNumberOfRetainedCheckpoints()} returns 0.</li>
	 * </ul>
	 */
	@Test
	public void testEmptyState() throws Exception {
		CompletedCheckpointStore checkpoints = createCompletedCheckpoints(1);

		assertNull(checkpoints.getLatestCheckpoint());
		assertEquals(0, checkpoints.getAllCheckpoints().size());
		assertEquals(0, checkpoints.getNumberOfRetainedCheckpoints());
	}

	/**
	 * Tests that all added checkpoints are returned.
	 */
	@Test
	public void testGetAllCheckpoints() throws Exception {
		CompletedCheckpointStore checkpoints = createCompletedCheckpoints(4);
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		TestCompletedCheckpoint[] expected = new TestCompletedCheckpoint[] {
				createCheckpoint(0), createCheckpoint(1),
				createCheckpoint(2), createCheckpoint(3)
		};

		for (TestCompletedCheckpoint checkpoint : expected) {
			checkpoints.addCheckpoint(checkpoint, sharedStateRegistry);
		}

		List<CompletedCheckpoint> actual = checkpoints.getAllCheckpoints();

		assertEquals(expected.length, actual.size());

		for (int i = 0; i < expected.length; i++) {
			assertEquals(expected[i], actual.get(i));
		}
	}

	/**
	 * Tests that all checkpoints are discarded (using the correct class loader).
	 */
	@Test
	public void testDiscardAllCheckpoints() throws Exception {
		CompletedCheckpointStore checkpoints = createCompletedCheckpoints(4);
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		TestCompletedCheckpoint[] expected = new TestCompletedCheckpoint[] {
				createCheckpoint(0), createCheckpoint(1),
				createCheckpoint(2), createCheckpoint(3)
		};

		for (TestCompletedCheckpoint checkpoint : expected) {
			checkpoints.addCheckpoint(checkpoint, sharedStateRegistry);
		}

		checkpoints.shutdown(JobStatus.FINISHED, sharedStateRegistry);

		// Empty state
		assertNull(checkpoints.getLatestCheckpoint());
		assertEquals(0, checkpoints.getAllCheckpoints().size());
		assertEquals(0, checkpoints.getNumberOfRetainedCheckpoints());

		// All have been discarded
		for (TestCompletedCheckpoint checkpoint : expected) {
			// The ZooKeeper implementation discards asynchronously
			checkpoint.awaitDiscard();
			assertTrue(checkpoint.isDiscarded());
		}
	}

	// ---------------------------------------------------------------------------------------------

	protected TestCompletedCheckpoint createCheckpoint(int id) throws IOException {
		return createCheckpoint(id, 4);
	}

	protected TestCompletedCheckpoint createCheckpoint(int id, int numberOfStates)
			throws IOException {
		return createCheckpoint(id, numberOfStates, CheckpointProperties.forStandardCheckpoint());
	}

	protected TestCompletedCheckpoint createCheckpoint(int id, int numberOfStates, CheckpointProperties props)
			throws IOException {

		JobVertexID jvid = new JobVertexID();

		Map<JobVertexID, TaskState> taskGroupStates = new HashMap<>();
		TaskState taskState = new TaskState(jvid, numberOfStates, numberOfStates, 1);
		taskGroupStates.put(jvid, taskState);

		for (int i = 0; i < numberOfStates; i++) {
			SubtaskState subtaskState = CheckpointCoordinatorTest.mockSubtaskState(jvid, i, new KeyGroupRange(i, i));

			taskState.putState(i, subtaskState);
		}

		return new TestCompletedCheckpoint(new JobID(), id, 0, taskGroupStates, props);
	}

	protected void resetCheckpoint(Collection<TaskState> taskStates) {
		for (TaskState taskState : taskStates) {
			for (SubtaskState subtaskState : taskState.getStates()) {
				Mockito.reset(subtaskState);
			}
		}
	}

	protected void verifyCheckpointRegistered(Collection<TaskState> taskStates, SharedStateRegistry sharedStateRegistry) {
		for (TaskState taskState : taskStates) {
			for (SubtaskState subtaskState : taskState.getStates()) {
				verify(subtaskState, times(1)).registerSharedStates(eq(sharedStateRegistry));
			}
		}
	}

	protected void verifyCheckpointDiscarded(Collection<TaskState> taskStates) {
		for (TaskState taskState : taskStates) {
			for (SubtaskState subtaskState : taskState.getStates()) {
				verify(subtaskState, times(1)).discardSharedStatesOnFail();
				verify(subtaskState, times(1)).discardState();
			}
		}
	}

	private void verifyCheckpoint(CompletedCheckpoint expected, CompletedCheckpoint actual) {
		assertEquals(expected, actual);
	}

	/**
	 * A test {@link CompletedCheckpoint}. We want to verify that the correct class loader is
	 * used when discarding. Spying on a regular {@link CompletedCheckpoint} instance with
	 * Mockito doesn't work, because it it breaks serializability.
	 */
	protected static class TestCompletedCheckpoint extends CompletedCheckpoint {

		private static final long serialVersionUID = 4211419809665983026L;

		private boolean isDiscarded;

		// Latch for test variants which discard asynchronously
		private transient final CountDownLatch discardLatch = new CountDownLatch(1);

		public TestCompletedCheckpoint(
			JobID jobId,
			long checkpointId,
			long timestamp,
			Map<JobVertexID, TaskState> taskGroupStates,
			CheckpointProperties props) {

			super(jobId, checkpointId, timestamp, Long.MAX_VALUE, taskGroupStates, props);
		}

		@Override
		public boolean discardOnSubsume(SharedStateRegistry sharedStateRegistry) throws Exception {
			if (super.discardOnSubsume(sharedStateRegistry)) {
				discard();
				return true;
			} else {
				return false;
			}
		}

		@Override
		public boolean discardOnShutdown(JobStatus jobStatus, SharedStateRegistry sharedStateRegistry) throws Exception {
			if (super.discardOnShutdown(jobStatus, sharedStateRegistry)) {
				discard();
				return true;
			} else {
				return false;
			}
		}

		void discard() {
			if (!isDiscarded) {
				this.isDiscarded = true;

				if (discardLatch != null) {
					discardLatch.countDown();
				}
			}
		}

		public boolean isDiscarded() {
			return isDiscarded;
		}

		public void awaitDiscard() throws InterruptedException {
			if (discardLatch != null) {
				discardLatch.await();
			}
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			TestCompletedCheckpoint that = (TestCompletedCheckpoint) o;

			return getJobId().equals(that.getJobId())
					&& getCheckpointID() == that.getCheckpointID();
		}

		@Override
		public int hashCode() {
			return getJobId().hashCode() + (int) getCheckpointID();
		}
	}

}
