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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for basic {@link CompletedCheckpointStore} contract.
 */
public class StandaloneCompletedCheckpointStoreTest extends CompletedCheckpointStoreTest {

	@Override
	protected CompletedCheckpointStore createCompletedCheckpoints(
			int maxNumberOfCheckpointsToRetain,
			ClassLoader userClassLoader) throws Exception {

		return new StandaloneCompletedCheckpointStore(maxNumberOfCheckpointsToRetain, userClassLoader);
	}

	/**
	 * Tests that shutdown discards all checkpoints.
	 */
	@Test
	public void testShutdownDiscardsCheckpoints() throws Exception {
		CompletedCheckpointStore store = createCompletedCheckpoints(1, ClassLoader.getSystemClassLoader());
		TestCheckpoint checkpoint = createCheckpoint(0);

		store.addCheckpoint(checkpoint);
		assertEquals(1, store.getNumberOfRetainedCheckpoints());

		store.shutdown();

		assertEquals(0, store.getNumberOfRetainedCheckpoints());
		assertTrue(checkpoint.isDiscarded());
	}

	/**
	 * Tests that suspends discards all checkpoints (as they cannot be
	 * recovered later in standalone recovery mode).
	 */
	@Test
	public void testSuspendDiscardsCheckpoints() throws Exception {
		CompletedCheckpointStore store = createCompletedCheckpoints(1, ClassLoader.getSystemClassLoader());
		TestCheckpoint checkpoint = createCheckpoint(0);

		store.addCheckpoint(checkpoint);
		assertEquals(1, store.getNumberOfRetainedCheckpoints());

		store.suspend();

		assertEquals(0, store.getNumberOfRetainedCheckpoints());
		assertTrue(checkpoint.isDiscarded());
	}
}
