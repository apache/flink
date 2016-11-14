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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class StateTable<K, N, ST> {

	/** Serializer for the state value. The state value could be a List<V>, for example. */
	protected final TypeSerializer<ST> stateSerializer;

	/** The serializer for the namespace */
	protected final TypeSerializer<N> namespaceSerializer;

	/** Map for holding the actual state objects. */
	private final List<Map<N, Map<K, ST>>> state;

	protected final KeyGroupRange keyGroupRange;

	public StateTable(
			TypeSerializer<ST> stateSerializer,
			TypeSerializer<N> namespaceSerializer,
			KeyGroupRange keyGroupRange) {
		this.stateSerializer = stateSerializer;
		this.namespaceSerializer = namespaceSerializer;
		this.keyGroupRange = keyGroupRange;

		this.state = Arrays.asList((Map<N, Map<K, ST>>[]) new Map[keyGroupRange.getNumberOfKeyGroups()]);
	}

	private int indexToOffset(int index) {
		return index - keyGroupRange.getStartKeyGroup();
	}

	public Map<N, Map<K, ST>> get(int index) {
		return keyGroupRange.contains(index) ? state.get(indexToOffset(index)) : null;
	}

	public void set(int index, Map<N, Map<K, ST>> map) {
		if (!keyGroupRange.contains(index)) {
			throw new RuntimeException("Unexpected key group index. This indicates a bug.");
		}
		state.set(indexToOffset(index), map);
	}

	public TypeSerializer<ST> getStateSerializer() {
		return stateSerializer;
	}

	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	public List<Map<N, Map<K, ST>>> getState() {
		return state;
	}
}
