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
		TestState firstState = new TestState("first");
		sharedStateRegistry.register(firstState.getKey(), firstState);
		assertEquals(1, sharedStateRegistry.getReferenceCount(firstState.getKey()));

		// register another state
		TestState secondState = new TestState("second");
		sharedStateRegistry.register(secondState.getKey(), secondState);
		assertEquals(1, sharedStateRegistry.getReferenceCount(secondState.getKey()));

		// register the first state again
		sharedStateRegistry.register(firstState.getKey(), firstState);
		assertEquals(2, sharedStateRegistry.getReferenceCount(firstState.getKey()));

		// unregister the second state
		sharedStateRegistry.unregister(secondState.getKey());
		assertEquals(0, sharedStateRegistry.getReferenceCount(secondState.getKey()));

		// unregister the first state
		sharedStateRegistry.unregister(firstState.getKey());
		assertEquals(1, sharedStateRegistry.getReferenceCount(firstState.getKey()));
	}

	/**
	 * Validate that registering a key with different states will throw exception
	 */
	@Test(expected = IllegalStateException.class)
	public void testRegisterWithInconsistentState() {
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		// register one state
		TestState state = new TestState("state");
		sharedStateRegistry.register("key", state);
		assertEquals(1, sharedStateRegistry.getReferenceCount("key"));

		// register the state with another key
		TestState anotherState = new TestState("anotherState");
		sharedStateRegistry.register("key", anotherState);
	}

	/**
	 * Validate that unregister an unexisted key will throw exception
	 */
	@Test(expected = IllegalStateException.class)
	public void testUnregisterWithUnexistedKey() {
		SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();

		sharedStateRegistry.unregister("unexistedKey");
	}

	private static class TestState implements StateObject {
		private static final long serialVersionUID = 4468635881465159780L;

		private String key;

		TestState(String key) {
			this.key = key;
		}

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

			TestState testState = (TestState) o;

			return key.equals(testState.key);
		}

		@Override
		public int hashCode() {
			return key.hashCode();
		}
	}
}
