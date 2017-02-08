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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.MergingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.internal.InternalMergingState;

import java.util.Collection;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for {@link MergingState} ({@link org.apache.flink.runtime.state.internal.InternalMergingState})
 * that is stored on the heap.
 * 
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <SV> The type of the values in the state.
 * @param <S> The type of State
 * @param <SD> The type of StateDescriptor for the State S
 */
public abstract class AbstractHeapMergingState<K, N, IN, OUT, SV, S extends State, SD extends StateDescriptor<S, ?>>
		extends AbstractHeapState<K, N, SV, S, SD>
		implements InternalMergingState<N, IN, OUT> {

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param backend The state backend backing that created this state.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param stateTable The state tab;e to use in this kev/value state. May contain initial state.
	 */
	protected AbstractHeapMergingState(
			KeyedStateBackend<K> backend,
			SD stateDesc,
			StateTable<K, N, SV> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) {

		super(backend, stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		if (sources == null || sources.isEmpty()) {
			return; // nothing to do
		}

		final K key = backend.getCurrentKey();
		checkState(key != null, "No key set.");

		final Map<N, Map<K, SV>> namespaceMap = stateTable.get(backend.getCurrentKeyGroupIndex());

		if (namespaceMap != null) {
			SV merged = null;

			// merge the sources
			for (N source : sources) {
				Map<K, SV> keysForNamespace = namespaceMap.get(source);
				if (keysForNamespace != null) {
					// get and remove the next source per namespace/key
					SV sourceState = keysForNamespace.remove(key);

					// if the namespace map became empty, remove 
					if (keysForNamespace.isEmpty()) {
						namespaceMap.remove(source);
					}

					if (merged != null && sourceState != null) {
						merged = mergeState(merged, sourceState);
					}
					else if (merged == null) {
						merged = sourceState;
					}
				}
			}

			// merge into the target, if needed
			if (merged != null) {
				Map<K, SV> keysForTarget = namespaceMap.get(target);
				if (keysForTarget == null) {
					keysForTarget = createNewMap();
					namespaceMap.put(target, keysForTarget);
				}
				SV targetState = keysForTarget.get(key);

				if (targetState != null) {
					targetState = mergeState(targetState, merged);
				}
				else {
					targetState = merged;
				}
				keysForTarget.put(key, targetState);
			}
		}

		// else no entries for that key at all, nothing to do skip
	}

	protected abstract SV mergeState(SV a, SV b) throws Exception;
}
