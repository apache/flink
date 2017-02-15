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

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;

/**
 * Heap-backed partitioned {@link org.apache.flink.api.common.state.ListState} that is snapshotted
 * into files and supports state queries.
 *
 * As opposed to {@link HeapListState}, this class avoids some race conditions that occur when
 * state queries access state concurrently to operators changing state.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
public class QueryableHeapListState<K, N, V> extends HeapListState<K, N, V> {

	/**
	 * Private class extending ArrayList which forbids {@link #remove(int)} that is used by {@link
	 * #iterator()}'s remove function.
	 * <p>
	 * This is useful for the {@link HeapListState#get()} function that returns an {@link Iterable}.
	 * By using {@link Iterable#iterator()}, the user may call {@link java.util.Iterator#remove}
	 * which modifies the list. {@link HeapListState#get()}, however, should only return a copy but
	 * actually returns on the real value which queryable state reads concurrently. In order not to
	 * create any races during structural changes, we thus forbid {@link #remove(int)}.
	 * <p>
	 * <em>Note:</em> we only make the {@link #remove(int)} function unsupported so this is not a
	 * real immutable arraylist. Also, future changes in {@link ArrayList} are not covered since we
	 * do not have control over its iterator class.
	 *
	 * @param <V>
	 * 		list element type
	 */
	private static class QueryableStateArrayList<V> extends ArrayList<V> {
		private static final long serialVersionUID = 1L;

		/**
		 * Unsupported operation.
		 *
		 * @throw UnsupportedOperationException always thrown
		 * @deprecated unsupported
		 */
		@Deprecated
		@Override
		public V remove(final int index) {
			throw new UnsupportedOperationException(
				"Structural changes to queryable list state are not allowed.");
		}
	}

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param backend The state backend backing that created this state.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                           and can create a default state value.
	 * @param stateTable The state tab;e to use in this kev/value state. May contain initial state.
	 */
	public QueryableHeapListState(
		KeyedStateBackend<K> backend,
		ListStateDescriptor<V> stateDesc,
		StateTable<K, N, ArrayList<V>> stateTable,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer) {
		super(backend, stateDesc, stateTable, keySerializer, namespaceSerializer);
	}

	@Override
	public void add(V value) {
		Preconditions.checkState(currentNamespace != null, "No namespace set.");
		Preconditions.checkState(backend.getCurrentKey() != null, "No key set.");

		if (value == null) {
			clear();
			return;
		}

		ArrayList<V> list = creatingGetListState();
		synchronized (list) {
			list.add(value);
		}
	}

	protected ArrayList<V> newList() {
		return new QueryableStateArrayList<>();
	}

	@Override
	public byte[] getSerializedValue(K key, N namespace) throws Exception {
		Preconditions.checkState(namespace != null, "No namespace given.");
		Preconditions.checkState(key != null, "No key given.");

		ArrayList<V> result = nonCreatingGetListState(key, namespace);
		if (result == null) {
			return null;
		}

		synchronized (result) {
			return serializeList(result);
		}
	}

}
