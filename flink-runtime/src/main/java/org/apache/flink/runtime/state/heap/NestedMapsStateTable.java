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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredBackendStateMetaInfo;

import java.util.HashMap;
import java.util.Map;

public class NestedMapsStateTable<K, N, ST> extends AbstractStateTable<K, N, ST> {

	/** Map for holding the actual state objects. */
	private final Map<N, Map<K, ST>>[] state;

	/** The offset to the contiguous key groups */
	private final int keyGroupOffset;

	// ------------------------------------------------------------------------
	public NestedMapsStateTable(KeyContext<K> keyContext, RegisteredBackendStateMetaInfo<N, ST> metaInfo) {
		super(keyContext, metaInfo);
		this.keyGroupOffset = keyContext.getKeyGroupRange().getStartKeyGroup();

		@SuppressWarnings("unchecked")
		Map<N, Map<K, ST>>[] state = (Map<N, Map<K, ST>>[]) new Map[keyContext.getNumberOfKeyGroups()];
		this.state = state;
	}

	// ------------------------------------------------------------------------
	//  access to maps
	// ------------------------------------------------------------------------

	public Map<N, Map<K, ST>>[] getState() {
		return state;
	}

	@VisibleForTesting
	public Map<N, Map<K, ST>> getMapForKeyGroup(int keyGroupIndex) {
		final int pos = indexToOffset(keyGroupIndex);
		if (pos >= 0 && pos < state.length) {
			return state[pos];
		} else {
			return null;
		}
	}

	public void setMapForKeyGroup(int index, Map<N, Map<K, ST>> map) {
		try {
			state[indexToOffset(index)] = map;
		}
		catch (ArrayIndexOutOfBoundsException e) {
			throw new IllegalArgumentException("Key group index out of range of key group range [" +
					keyGroupOffset + ", " + (keyGroupOffset + state.length) + ").");
		}
	}

	private int indexToOffset(int index) {
		return index - keyGroupOffset;
	}

	// ------------------------------------------------------------------------

	@Override
	public int size() {
		int count = 0;
		for (Map<N, Map<K, ST>> namespaceMap : state) {
			if (null != namespaceMap) {
				for (Map<K, ST> keyMap : namespaceMap.values()) {
					if (null != keyMap) {
						count += keyMap.size();
					}
				}
			}
		}
		return count;
	}

	@Override
	public ST get(Object namespace) {
		return get(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	@Override
	public boolean containsKey(Object namespace) {
		return containsKey(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	@Override
	public void put(N namespace, ST value) {
		put(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace, value);
	}

	@Override
	public ST putAndGetOld(N namespace, ST value) {
		return putAndGetOld(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace, value);
	}

	@Override
	public void remove(Object namespace) {
		remove(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	@Override
	public ST removeAndGetOld(Object namespace) {
		return removeAndGetOld(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace);
	}

	@Override
	public ST get(Object key, Object namespace) {
		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, keyContext.getNumberOfKeyGroups());
		return get(key, keyGroup, namespace);
	}

	// ------------------------------------------------------------------------

	boolean containsKey(Object key, int keyGroupIndex, Object namespace) {

		Map<N, Map<K, ST>> namespaceMap = getMapForKeyGroup(keyGroupIndex);

		if (namespaceMap == null) {
			return false;
		}

		Map<K, ST> keyedMap = namespaceMap.get(namespace);

		return keyedMap != null && keyedMap.containsKey(key);
	}

	ST get(Object key, int keyGroupIndex, Object namespace) {

		Map<N, Map<K, ST>> namespaceMap = getMapForKeyGroup(keyGroupIndex);

		if (namespaceMap == null) {
			return null;
		}

		Map<K, ST> keyedMap = namespaceMap.get(namespace);

		if (keyedMap == null) {
			return null;
		}

		return keyedMap.get(key);
	}

	private void put(K key, int keyGroupIndex, N namespace, ST value) {
		putAndGetOld(key, keyGroupIndex, namespace, value);
	}

	private ST putAndGetOld(K key, int keyGroupIndex, N namespace, ST value) {

		Map<N, Map<K, ST>> namespaceMap = getMapForKeyGroup(keyGroupIndex);

		if (namespaceMap == null) {
			namespaceMap = new HashMap<>();
			setMapForKeyGroup(keyGroupIndex, namespaceMap);
		}

		Map<K, ST> keyedMap = namespaceMap.get(namespace);

		if (keyedMap == null) {
			keyedMap = new HashMap<>();
			namespaceMap.put(namespace, keyedMap);
		}

		return keyedMap.put(key, value);
	}

	private void remove(Object key, int keyGroupIndex, Object namespace) {
		removeAndGetOld(key, keyGroupIndex, namespace);
	}

	private ST removeAndGetOld(Object key, int keyGroupIndex, Object namespace) {

		Map<N, Map<K, ST>> namespaceMap = getMapForKeyGroup(keyGroupIndex);

		if (namespaceMap == null) {
			return null;
		}

		Map<K, ST> keyedMap = namespaceMap.get(namespace);

		if (keyedMap == null) {
			return null;
		}

		ST removed = keyedMap.remove(key);

		if (keyedMap.isEmpty()) {
			namespaceMap.remove(namespace);
		}

		return removed;
	}
}
