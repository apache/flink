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

/**
 * An internal state interface that supports stateful operator.
 */
public interface TableState<K, V> {
	public void put(K key, V value);
	public V get(K key);
	public void delete(K key);
	public boolean containsKey(K key);
	public String serialize();
	public void deserialize(String str);
	public TableStateIterator<K, V> getIterator();
}
