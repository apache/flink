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

package org.apache.flink.runtime.state.heap;

import java.util.Objects;

/**
 * One full entry in a state table. Consists of an immutable key (not null), an immutable namespace (not null), and
 * a state that can be mutable and null.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of value
 */
public class StateEntry<K, N, S> {

	/**
	 * The key. Assumed to be immutable and not null.
	 */
	protected final K key;

	/**
	 * The namespace. Assumed to be immutable and not null.
	 */
	protected final N namespace;

	/**
	 * The state. This is not final to allow exchanging the object for copy-on-write. Can be null.
	 */
	protected S state;

	public StateEntry(K key, N namespace, S state) {
		this.key = key;
		this.namespace = namespace;
		this.state = state;
	}

	/**
	 * Returns the key of this entry.
	 */
	public K getKey() {
		return key;
	}

	/**
	 * Returns the namespace of this entry.
	 */
	public N getNamespace() {
		return namespace;
	}

	/**
	 * Returns the state of this entry.
	 */
	public S getState() {
		return state;
	}

	@Override
	public final boolean equals(Object o) {
		if (!(o instanceof CopyOnWriteStateTable.StateTableEntry)) {
			return false;
		}

		StateEntry<?, ?, ?> e = (StateEntry<?, ?, ?>) o;
		return e.getKey().equals(key)
				&& e.getNamespace().equals(namespace)
				&& Objects.equals(e.getState(), state);
	}

	@Override
	public final int hashCode() {
		return (key.hashCode() ^ namespace.hashCode()) ^ Objects.hashCode(state);
	}

	@Override
	public final String toString() {
		return "(" + key + "|" + namespace + ")=" + state;
	}
}
