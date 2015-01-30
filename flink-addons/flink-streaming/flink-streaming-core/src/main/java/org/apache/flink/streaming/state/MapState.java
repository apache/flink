/*
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
 */

package org.apache.flink.streaming.state;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.flink.streaming.state.checkpoint.MapCheckpoint;
import org.apache.flink.streaming.state.checkpoint.StateCheckpoint;

/**
 * A Map that can be used as a partitionable operator state, for both fault
 * tolerance and rebalancing in Flink programs. The default repartitioner for
 * this operator repartitions by the hashcodes of the keys in the map. </br>
 * </br> The MapState also allows for incremental (data efficient) checkpointing
 * of the state. The entries in the map should only be modified by using the
 * dedicated methods: {@link #put(Object, Object)},{@link #remove(Object)},
 * {@link #putAll(Map)} and {@link #clear}. Directly modifying the entryset
 * will cause errors when checkpointing.
 *
 * @param <K>
 *            The type of the keys.
 * @param <V>
 *            The type of the values.
 */
public class MapState<K, V> extends PartitionableState<Map<K, V>> implements Map<K, V> {

	private static final long serialVersionUID = 1L;

	protected Set<K> removedItems;
	protected Set<K> updatedItems;
	protected boolean clear;

	public MapState() {
		this.state = new HashMap<K, V>();
		this.updatedItems = new HashSet<K>();
		this.removedItems = new HashSet<K>();
	}

	@Override
	public StateCheckpoint<Map<K, V>> checkpoint() {
		this.updatedItems.removeAll(removedItems);
		StateCheckpoint<Map<K, V>> checkpoint = new MapCheckpoint<K, V>(this);
		resetHistory();
		return checkpoint;
	}

	@Override
	public OperatorState<Map<K, V>> restore(StateCheckpoint<Map<K, V>> checkpoint) {
		this.state = checkpoint.getCheckpointedState();
		resetHistory();
		return this;
	}

	@Override
	public OperatorState<Map<K, V>>[] repartition(int numberOfPartitions) {
		@SuppressWarnings("unchecked")
		MapState<K, V>[] states = new MapState[numberOfPartitions];
		for (int i = 0; i < numberOfPartitions; i++) {
			states[i] = new MapState<K, V>();
		}
		for (Entry<K, V> entry : this.state.entrySet()) {
			int partition = Math.abs(entry.getKey().hashCode() % numberOfPartitions);
			states[partition].state.put(entry.getKey(), entry.getValue());
		}
		return states;
	}

	@Override
	public OperatorState<Map<K, V>> reBuild(OperatorState<Map<K, V>>... parts) {
		clear();
		for (OperatorState<Map<K, V>> operatorState : parts) {
			putAll(operatorState.state);
		}
		return this;
	}

	public void resetHistory() {
		clear = false;
		removedItems.clear();
		updatedItems.clear();
	}

	@Override
	public int size() {
		return state.size();
	}

	@Override
	public boolean isEmpty() {
		return state.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return state.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return state.containsValue(value);
	}

	@Override
	public V get(Object key) {
		return state.get(key);
	}

	@Override
	public V put(K key, V value) {
		updatedItems.add(key);
		removedItems.remove(key);
		return state.put(key, value);
	}

	@SuppressWarnings("unchecked")
	@Override
	public V remove(Object key) {
		V removed = state.remove(key);
		if (removed != null) {
			removedItems.add((K) key);
			updatedItems.remove((K) key);
		}

		return removed;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		for (K key : m.keySet()) {
			updatedItems.add(key);
			removedItems.remove(key);
		}
		state.putAll(m);
	}

	@Override
	public void clear() {
		clear = true;
		updatedItems.clear();
		removedItems.clear();
		state.clear();
	}

	@Override
	public Set<K> keySet() {
		return state.keySet();
	}

	@Override
	public Collection<V> values() {
		return state.values();
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return state.entrySet();
	}

	@Override
	public Map<K, V> getState() {
		throw new RuntimeException(
				"State object should be accessed using the Map interface provided by the MapState");
	}

	@Override
	public OperatorState<Map<K, V>> setState(Map<K, V> state) {
		throw new RuntimeException(
				"State object should be accessed using the Map interface provided by the MapState");
	}

	public Set<K> getRemovedItems() {
		return removedItems;
	}

	public Set<K> getUpdatedItems() {
		return updatedItems;
	}

	public boolean isCleared() {
		return clear;
	}
}
