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

import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.powermock.api.mockito.PowerMockito.doReturn;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * Tests for basic {@link CompletedCheckpointStore} contract.
 */
public class StandaloneCompletedCheckpointStoreTest extends CompletedCheckpointStoreTest {

	@Override
	protected AbstractCompletedCheckpointStore createCompletedCheckpoints(
			int maxNumberOfCheckpointsToRetain) throws Exception {

		return new StandaloneCompletedCheckpointStore(maxNumberOfCheckpointsToRetain);
	}

	/**
	 * Tests that shutdown discards all checkpoints.
	 */
	@Test
	public void testShutdownDiscardsCheckpoints() throws Exception {
		AbstractCompletedCheckpointStore store = createCompletedCheckpoints(1);
		TestCompletedCheckpoint checkpoint = createCheckpoint(0);
		Collection<TaskState> taskStates = checkpoint.getTaskStates().values();

		store.addCheckpoint(checkpoint);
		assertEquals(1, store.getNumberOfRetainedCheckpoints());
		verifyCheckpointRegistered(taskStates, store.sharedStateRegistry);

		store.shutdown(JobStatus.FINISHED);
		assertEquals(0, store.getNumberOfRetainedCheckpoints());
		assertTrue(checkpoint.isDiscarded());
		verifyCheckpointDiscarded(taskStates);
	}

	/**
	 * Tests that suspends discards all checkpoints (as they cannot be
	 * recovered later in standalone recovery mode).
	 */
	@Test
	public void testSuspendDiscardsCheckpoints() throws Exception {
		AbstractCompletedCheckpointStore store = createCompletedCheckpoints(1);
		TestCompletedCheckpoint checkpoint = createCheckpoint(0);
		Collection<TaskState> taskStates = checkpoint.getTaskStates().values();

		store.addCheckpoint(checkpoint);
		assertEquals(1, store.getNumberOfRetainedCheckpoints());
		verifyCheckpointRegistered(taskStates, store.sharedStateRegistry);

		store.shutdown(JobStatus.SUSPENDED);
		assertEquals(0, store.getNumberOfRetainedCheckpoints());
		assertTrue(checkpoint.isDiscarded());
		verifyCheckpointDiscarded(taskStates);
	}
	
	/**
	 * Tests that the checkpoint does not exist in the store when we fail to add
	 * it into the store (i.e., there exists an exception thrown by the method).
	 */
	@Test
	public void testAddCheckpointWithFailedRemove() throws Exception {
		
		final int numCheckpointsToRetain = 1;
		CompletedCheckpointStore store = createCompletedCheckpoints(numCheckpointsToRetain);
		
		for (long i = 0; i <= numCheckpointsToRetain; ++i) {
			CompletedCheckpoint checkpointToAdd = mock(CompletedCheckpoint.class);
			doReturn(i).when(checkpointToAdd).getCheckpointID();
			doReturn(Collections.emptyMap()).when(checkpointToAdd).getTaskStates();
			doThrow(new IOException()).when(checkpointToAdd).discardOnSubsume(any(SharedStateRegistry.class));
			
			try {
				store.addCheckpoint(checkpointToAdd);
				
				// The checkpoint should be in the store if we successfully add it into the store.
				List<CompletedCheckpoint> addedCheckpoints = store.getAllCheckpoints();
				assertTrue(addedCheckpoints.contains(checkpointToAdd));
			} catch (Exception e) {
				// The checkpoint should not be in the store if any exception is thrown.
				List<CompletedCheckpoint> addedCheckpoints = store.getAllCheckpoints();
				assertFalse(addedCheckpoints.contains(checkpointToAdd));
			}
		}
	}
}
