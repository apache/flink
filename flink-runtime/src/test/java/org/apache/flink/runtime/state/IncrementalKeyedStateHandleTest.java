/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.checkpoint.savepoint.CheckpointTestUtils;
import org.junit.Test;

import java.util.Map;
import java.util.Random;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.spy;

public class IncrementalKeyedStateHandleTest {

	/**
	 * This test checks, that for an unregistered {@link IncrementalKeyedStateHandle} all state
	 * (including shared) is discarded.
	 */
	@Test
	public void testUnregisteredDiscarding() throws Exception {
		IncrementalKeyedStateHandle stateHandle = create(new Random(42));

		stateHandle.discardState();

		for (StreamStateHandle handle : stateHandle.getPrivateState().values()) {
			verify(handle).discardState();
		}

		for (StreamStateHandle handle : stateHandle.getSharedState().values()) {
			verify(handle).discardState();
		}

		verify(stateHandle.getMetaStateHandle()).discardState();
	}

	/**
	 * This test checks, that for a registered {@link IncrementalKeyedStateHandle} discards respect
	 * all shared state and only discard it one all references are released.
	 */
	@Test
	public void testSharedStateDeRegistration() throws Exception {

		Random rnd = new Random(42);

		SharedStateRegistry registry = spy(new SharedStateRegistry());

		// Create two state handles with overlapping shared state
		IncrementalKeyedStateHandle stateHandle1 = create(new Random(42));
		IncrementalKeyedStateHandle stateHandle2 = create(new Random(42));

		// Both handles should not be registered and not discarded by now.
		for (Map.Entry<StateHandleID, StreamStateHandle> entry :
			stateHandle1.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(entry.getKey());

			verify(registry, times(0)).unregisterReference(registryKey);
			verify(entry.getValue(), times(0)).discardState();
		}

		for (Map.Entry<StateHandleID, StreamStateHandle> entry :
			stateHandle2.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(entry.getKey());

			verify(registry, times(0)).unregisterReference(registryKey);
			verify(entry.getValue(), times(0)).discardState();
		}

		// Now we register both ...
		stateHandle1.registerSharedStates(registry);
		stateHandle2.registerSharedStates(registry);

		for (Map.Entry<StateHandleID, StreamStateHandle> stateHandleEntry :
			stateHandle1.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(stateHandleEntry.getKey());

			verify(registry).registerReference(
				registryKey,
				stateHandleEntry.getValue());
		}

		for (Map.Entry<StateHandleID, StreamStateHandle> stateHandleEntry :
			stateHandle2.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(stateHandleEntry.getKey());

			verify(registry).registerReference(
				registryKey,
				stateHandleEntry.getValue());
		}

		// We discard the first
		stateHandle1.discardState();

		// Should be unregistered, non-shared discarded, shared not discarded
		for (Map.Entry<StateHandleID, StreamStateHandle> entry :
			stateHandle1.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(entry.getKey());

			verify(registry, times(1)).unregisterReference(registryKey);
			verify(entry.getValue(), times(0)).discardState();
		}

		for (StreamStateHandle handle :
			stateHandle2.getSharedState().values()) {

			verify(handle, times(0)).discardState();
		}

		for (Map.Entry<StateHandleID, StreamStateHandle> handleEntry :
			stateHandle1.getPrivateState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(handleEntry.getKey());

			verify(registry, times(0)).unregisterReference(registryKey);
			verify(handleEntry.getValue(), times(1)).discardState();
		}

		for (Map.Entry<StateHandleID, StreamStateHandle> handleEntry :
			stateHandle2.getPrivateState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(handleEntry.getKey());

			verify(registry, times(0)).unregisterReference(registryKey);
			verify(handleEntry.getValue(), times(0)).discardState();
		}

		verify(stateHandle1.getMetaStateHandle(), times(1)).discardState();
		verify(stateHandle2.getMetaStateHandle(), times(0)).discardState();

		// We discard the second
		stateHandle2.discardState();


		// Now everything should be unregistered and discarded
		for (Map.Entry<StateHandleID, StreamStateHandle> entry :
			stateHandle1.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(entry.getKey());

			verify(registry, times(2)).unregisterReference(registryKey);
			verify(entry.getValue()).discardState();
		}

		for (Map.Entry<StateHandleID, StreamStateHandle> entry :
			stateHandle2.getSharedState().entrySet()) {

			SharedStateRegistryKey registryKey =
				stateHandle1.createSharedStateRegistryKeyFromFileName(entry.getKey());

			verify(registry, times(2)).unregisterReference(registryKey);
			verify(entry.getValue()).discardState();
		}

		verify(stateHandle1.getMetaStateHandle(), times(1)).discardState();
		verify(stateHandle2.getMetaStateHandle(), times(1)).discardState();
	}

	private static IncrementalKeyedStateHandle create(Random rnd) {
		return new IncrementalKeyedStateHandle(
			"test",
			KeyGroupRange.of(0, 0),
			1L,
			placeSpies(CheckpointTestUtils.createRandomStateHandleMap(rnd)),
			placeSpies(CheckpointTestUtils.createRandomStateHandleMap(rnd)),
			spy(CheckpointTestUtils.createDummyStreamStateHandle(rnd)));
	}

	private static Map<StateHandleID, StreamStateHandle> placeSpies(
		Map<StateHandleID, StreamStateHandle> map) {

		for (Map.Entry<StateHandleID, StreamStateHandle> entry : map.entrySet()) {
			entry.setValue(spy(entry.getValue()));
		}
		return map;
	}
}
