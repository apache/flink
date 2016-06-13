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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.StateIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

@Internal
public class HeapReducingStateIterator<K, V> implements StateIterator<K, ReducingState<V>> {
	private final Iterator<Map.Entry<K, V>> it;
	private K key = null;
	private ProxyReducingState<V> state = new ProxyReducingState<>();

	public HeapReducingStateIterator(Iterator<Map.Entry<K, V>> it) {
		this.it = it;
	}

	@Override
	public K key() {
		if (key == null) {
			throw new IllegalStateException("No key set.");
		}
		return key;
	}

	@Override
	public ReducingState<V> state() {
		return state;
	}

	@Override
	public boolean advance() {
		if (!it.hasNext()) {
			this.key = null;
			this.state = null;
			return false;
		}
		Map.Entry<K, V> nextState = it.next();
		this.key = nextState.getKey();
		this.state.state = nextState.getValue();
		return true;
	}

	@Override
	public void delete() throws Exception {
		it.remove();
	}

	@Internal
	private static class ProxyReducingState<V> implements ReducingState<V> {
		protected V state = null;

		@Override
		public V get() throws IOException {
			if (state == null) {
				throw new IllegalStateException("No state set.");
			}

			return state;
		}

		@Override
		public void add(V value) throws IOException {
			throw new UnsupportedOperationException("Cannot update state view.");
		}

		@Override
		public void clear() {
			throw new UnsupportedOperationException("Cannot clear state view.");
		}
	}
}
