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
import org.apache.flink.runtime.state.internal.InternalMergingState;

import java.util.Collection;

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
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param stateTable The state tab;e to use in this kev/value state. May contain initial state.
	 */
	protected AbstractHeapMergingState(
			SD stateDesc,
			NestedMapsStateTable<K, N, SV> stateTable,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer) {

		super(stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
		if (sources == null || sources.isEmpty()) {
			return; // nothing to do
		}

		final NestedMapsStateTable<K, N, SV> map = stateTable;

		SV merged = null;

		// merge the sources
		for (N source : sources) {

			// get and remove the next source per namespace/key
			SV sourceState = map.removeAndGetOld(source);

			if (merged != null && sourceState != null) {
				merged = mergeState(merged, sourceState);
			} else if (merged == null) {
				merged = sourceState;
			}
		}

		// merge into the target, if needed
		if (merged != null) {

			SV targetState = map.get(target);

			if (targetState != null) {
				targetState = mergeState(targetState, merged);
			} else {
				targetState = merged;
			}
			map.put(target, targetState);
		}
	}

	protected abstract SV mergeState(SV a, SV b) throws Exception;
}