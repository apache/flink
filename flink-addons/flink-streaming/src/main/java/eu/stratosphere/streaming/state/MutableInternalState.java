/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.state;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The most general internal state that stores data in a mutable map.
 */
public class MutableInternalState<K, V> implements InternalState<K, V> {

	private Map<K, V> state=new LinkedHashMap<K, V>();
	@Override
	public void put(K key, V value) {
		// TODO Auto-generated method stub
		state.put(key, value);
	}

	@Override
	public V get(K key) {
		// TODO Auto-generated method stub
		return state.get(key);
	}

	@Override
	public void delete(K key) {
		// TODO Auto-generated method stub
		state.remove(key);
	}

	@Override
	public boolean containsKey(K key) {
		// TODO Auto-generated method stub
		return state.containsKey(key);
	}

	@Override
	public StateIterator<K, V> getIterator() {
		// TODO Auto-generated method stub
		return new MutableStateIterator<K, V>(state.entrySet().iterator());
	}

}
