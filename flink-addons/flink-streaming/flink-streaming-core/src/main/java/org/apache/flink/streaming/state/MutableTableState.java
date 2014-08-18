/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.state;

import java.io.Serializable;

/**
 * The most general internal state that stores data in a mutable map.
 */
public class MutableTableState<K, V> extends TableState<K, V> implements Serializable {
	private static final long serialVersionUID = 1L;


	@Override
	public void put(K key, V value) {
		state.put(key, value);
	}

	@Override
	public V get(K key) {
		return state.get(key);
	}

	@Override
	public void delete(K key) {
		state.remove(key);
	}

	@Override
	public boolean containsKey(K key) {
		return state.containsKey(key);
	}

	@Override
	public MutableTableStateIterator<K, V> getIterator() {
		return new MutableTableStateIterator<K, V>(state.entrySet().iterator());
	}

	@Override
	public String toString() {
		return state.toString();
	}

}
