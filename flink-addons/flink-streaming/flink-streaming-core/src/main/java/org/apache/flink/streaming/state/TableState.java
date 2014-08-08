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
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The most general internal state that stores data in a mutable map.
 */
@SuppressWarnings("serial")
public class TableState<K, V> implements Serializable {

	protected Map<K, V> state=new LinkedHashMap<K, V>();

	public void put(K key, V value) {
		state.put(key, value);
	}

	public V get(K key) {
		return state.get(key);
	}

	public void delete(K key) {
		state.remove(key);
	}

	public boolean containsKey(K key) {
		return state.containsKey(key);
	}

	public TableStateIterator<K, V> getIterator() {
		return new TableStateIterator<K, V>(state.entrySet().iterator());
	}
	
	public void clear(){
		state.clear();
	}
}
