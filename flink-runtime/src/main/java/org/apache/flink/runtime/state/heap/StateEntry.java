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

/**
 * One entry in a state table.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of value
 */
public class StateEntry<K, N, S> {

	protected K key;
	protected N namespace;
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

}
