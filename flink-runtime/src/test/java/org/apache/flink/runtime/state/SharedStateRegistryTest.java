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


package org.apache.flink.runtime.state;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SharedStateRegistryTest {

	/**
	 * Validate that all states can be correctly registered at the registry.
	 */
	@Test
	public void testRegistryNormal() {
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		// register one state
		TestSharedState firstState = new TestSharedState("first");
		sharedStateRegistry.register(firstState, true);
		assertEquals(1, sharedStateRegistry.getReferenceCount(firstState));

		// register another state
		TestSharedState secondState = new TestSharedState("second");
		sharedStateRegistry.register(secondState, true);
		assertEquals(1, sharedStateRegistry.getReferenceCount(secondState));

		// register the first state again
		sharedStateRegistry.register(firstState, false);
		assertEquals(2, sharedStateRegistry.getReferenceCount(firstState));

		// unregister the second state
		sharedStateRegistry.unregister(secondState);
		assertEquals(0, sharedStateRegistry.getReferenceCount(secondState));

		// unregister the first state
		sharedStateRegistry.unregister(firstState);
		assertEquals(1, sharedStateRegistry.getReferenceCount(firstState));
	}

	/**
	 * Validate that registering a handle referencing uncreated state will throw exception
	 */
	@Test(expected = IllegalStateException.class)
	public void testRegisterWithUncreatedReference() {
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		// register one state
		TestSharedState state = new TestSharedState("state");
		sharedStateRegistry.register(state, false);
	}

	/**
	 * Validate that registering duplicate creation of the same state will throw exception
	 */
	@Test(expected = IllegalStateException.class)
	public void testRegisterWithDuplicateState() {
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		// register one state
		TestSharedState state = new TestSharedState("state");
		sharedStateRegistry.register(state, true);
		sharedStateRegistry.register(state, true);
	}

	/**
	 * Validate that unregister an unexisted key will throw exception
	 */
	@Test(expected = IllegalStateException.class)
	public void testUnregisterWithUnexistedKey() {
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		sharedStateRegistry.unregister(new TestSharedState("unexisted"));
	}

	private static class TestSharedState implements SharedStateHandle {
		private static final long serialVersionUID = 4468635881465159780L;

		private String key;

		TestSharedState(String key) {
			this.key = key;
		}

		@Override
		public String getKey() {
			return key;
		}

		@Override
		public void discardState() throws Exception {
			// nothing to do
		}

		@Override
		public long getStateSize() {
			return key.length();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			TestSharedState testState = (TestSharedState) o;

			return key.equals(testState.key);
		}

		@Override
		public int hashCode() {
			return key.hashCode();
		}
	}
}
