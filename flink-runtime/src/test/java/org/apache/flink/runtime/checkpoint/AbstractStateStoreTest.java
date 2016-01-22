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
 * Basic {@link StateStore} behaviour test.
 */
public abstract class AbstractStateStoreTest {

	abstract StateStore<Integer> createStateStore() throws Exception;

	abstract boolean verifyDiscarded(StateStore<Integer> stateStore, String path);

	@Test
	public void testSimplePutGetDiscardSequence() throws Exception {
		StateStore<Integer> stateStore = createStateStore();

		// Put
		Integer expectedState = 211155;
		String path = stateStore.putState(expectedState);

		// Get
		Integer actualState = stateStore.getState(path);
		assertEquals(expectedState, actualState);

		// Dispose
		stateStore.disposeState(path);
		assertTrue(verifyDiscarded(stateStore, path));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGetStateInvalidPathThrowsException() throws Exception {
		StateStore<Integer> stateStore = createStateStore();
		stateStore.getState("testGetStateInvalidPathThrowsException");
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDisposeStateInvalidPathThrowsException() throws Exception {
		StateStore<Integer> stateStore = createStateStore();
		stateStore.getState("testDisposeStateInvalidPathThrowsException");
	}
}
