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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState.StateIncrementalVisitor;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * This implementation of {@link StateTable} uses {@link CopyOnWriteStateMap}. It is maintaining a partitioning
 * by key-group. This implementation supports asynchronous snapshots.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of state.
 */
public class CopyOnWriteStateTable<K, N, S> extends StateTable<K, N, S> implements Iterable<StateEntry<K, N, S>> {

	/**
	 * Map for holding the actual state objects. The outer array represents the key-groups.
	 */
	private final CopyOnWriteStateMap<K, N, S>[] state;

	/**
	 * The offset to the contiguous key groups.
	 */
	private final int keyGroupOffset;

	/**
	 * Constructs a new {@code StateTable}.
	 *
	 * @param keyContext    the key context.
	 * @param metaInfo      the meta information, including the type serializer for state copy-on-write.
	 * @param keySerializer the serializer of the key.
	 */
	CopyOnWriteStateTable(
		InternalKeyContext<K> keyContext,
		RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
		TypeSerializer<K> keySerializer) {
		super(keyContext, metaInfo, keySerializer);

		this.keyGroupOffset = keyContext.getKeyGroupRange().getStartKeyGroup();

		@SuppressWarnings("unchecked")
		CopyOnWriteStateMap<K, N, S>[] state = new CopyOnWriteStateMap[keyContext.getKeyGroupRange().getNumberOfKeyGroups()];
		this.state = state;
	}

	// Public API from AbstractStateTable ------------------------------------------------------------------------------

	/**
	 * Returns the total number of entries in this {@link CopyOnWriteStateTable}. This is the sum of all state maps.
	 *
	 * @return the number of entries in this {@link CopyOnWriteStateTable}.
	 */
	@Override
	public int size() {
		int count = 0;
		for (CopyOnWriteStateMap<K, N, S> stateMap : state) {
			if (stateMap != null) {
				count += stateMap.size();
			}
		}
		return count;
	}

	@Override
	public S get(N namespace) {
		return get(keyContext.getCurrentKey(), namespace);
	}

	@Override
	public S get(K key, N namespace) {
		checkKeyNamespacePreconditions(key, namespace);

		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, keyContext.getNumberOfKeyGroups());
		CopyOnWriteStateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroup);

		if (stateMap == null) {
			return null;
		}

		return stateMap.get(key, namespace);
	}

	@Override
	public boolean containsKey(N namespace) {
		K key = keyContext.getCurrentKey();
		checkKeyNamespacePreconditions(key, namespace);

		CopyOnWriteStateMap<K, N, S> stateMap = getMapForKeyGroup(keyContext.getCurrentKeyGroupIndex());

		if (stateMap == null) {
			return false;
		}
		return stateMap.containsKey(key, namespace);
	}

	@Override
	public Stream<K> getKeys(N namespace) {
		return Arrays.stream(state)
			.filter(Objects::nonNull)
			.map(CopyOnWriteStateMap::iterator)
			.flatMap(iter -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, 0), false))
			.filter(entry -> entry.getNamespace().equals(namespace))
			.map(StateEntry::getKey);
	}

	@Override
	public void put(N namespace, S state) {
		put(keyContext.getCurrentKey(), keyContext.getCurrentKeyGroupIndex(), namespace, state);
	}

	@Override
	public void put(K key, int keyGroup, N namespace, S state) {
		checkKeyNamespacePreconditions(key, namespace);

		CopyOnWriteStateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroup);

		if (stateMap == null) {
			stateMap = new CopyOnWriteStateMap<>(getStateSerializer());
			setMapForKeyGroup(keyGroup, stateMap);
		}

		stateMap.put(key, namespace, state);
	}

	@Override
	public S putAndGetOld(N namespace, S state) {
		K key = keyContext.getCurrentKey();
		checkKeyNamespacePreconditions(key, namespace);

		int keyGroup = keyContext.getCurrentKeyGroupIndex();
		CopyOnWriteStateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroup);

		if (stateMap == null) {
			stateMap = new CopyOnWriteStateMap<>(getStateSerializer());
			setMapForKeyGroup(keyGroup, stateMap);
		}

		return stateMap.putAndGetOld(key, namespace, state);
	}

	@Override
	public void remove(N namespace) {
		K key = keyContext.getCurrentKey();
		checkKeyNamespacePreconditions(key, namespace);

		CopyOnWriteStateMap<K, N, S> stateMap = getMapForKeyGroup(keyContext.getCurrentKeyGroupIndex());

		if (stateMap == null) {
			return;
		}

		stateMap.remove(key, namespace);
	}

	@Override
	public S removeAndGetOld(N namespace) {
		K key = keyContext.getCurrentKey();
		checkKeyNamespacePreconditions(key, namespace);

		CopyOnWriteStateMap<K, N, S> stateMap = getMapForKeyGroup(keyContext.getCurrentKeyGroupIndex());

		if (stateMap == null) {
			return null;
		}

		return stateMap.removeAndGetOld(key, namespace);
	}

	@Override
	public <T> void transform(N namespace, T value, StateTransformationFunction<S, T> transformation) throws Exception {
		K key = keyContext.getCurrentKey();
		checkKeyNamespacePreconditions(key, namespace);

		int keyGroup = keyContext.getCurrentKeyGroupIndex();
		CopyOnWriteStateMap<K, N, S> stateMap = getMapForKeyGroup(keyGroup);

		if (stateMap == null) {
			stateMap = new CopyOnWriteStateMap<>(getStateSerializer());
			setMapForKeyGroup(keyGroup, stateMap);
		}

		stateMap.transform(key, namespace, value, transformation);
	}

	// ------------------------------------------------------------------------

	private void checkKeyNamespacePreconditions(K key, N namespace) {
		Preconditions.checkNotNull(key, "No key set. This method should not be called outside of a keyed context.");
		Preconditions.checkNotNull(namespace, "Provided namespace is null.");
	}

	// ------------------------------------------------------------------------
	//  access to maps
	// ------------------------------------------------------------------------

	private CopyOnWriteStateMap<K, N, S> getMapForKeyGroup(int keyGroupIndex) {
		final int pos = indexToOffset(keyGroupIndex);
		if (pos >= 0 && pos < state.length) {
			return state[pos];
		} else {
			return null;
		}
	}

	/**
	 * Sets the given map for the given key-group.
	 */
	private void setMapForKeyGroup(int keyGroupId, CopyOnWriteStateMap<K, N, S> map) {
		try {
			state[indexToOffset(keyGroupId)] = map;
		} catch (ArrayIndexOutOfBoundsException e) {
			throw new IllegalArgumentException("Key group index " + keyGroupId + " is out of range of key group " +
				"range [" + keyGroupOffset + ", " + (keyGroupOffset + state.length) + ").");
		}
	}

	/**
	 * Translates a key-group id to the internal array offset.
	 */
	private int indexToOffset(int index) {
		return index - keyGroupOffset;
	}

	// Snapshotting ----------------------------------------------------------------------------------------------------

	/**
	 * Creates a snapshot of this {@link CopyOnWriteStateTable}, to be written in checkpointing.
	 *
	 * @return a snapshot from this {@link CopyOnWriteStateTable}, for checkpointing.
	 */
	@Nonnull
	@Override
	public CopyOnWriteStateTableSnapshot<K, N, S> stateSnapshot() {
		return new CopyOnWriteStateTableSnapshot<>(this);
	}

	CopyOnWriteStateMapSnapshot<K, N, S>[] getStateMapSnapshotArray() {
		CopyOnWriteStateMapSnapshot<K, N, S>[] snapshotArray =
			new CopyOnWriteStateMapSnapshot[state.length];
		for (int i = 0; i < state.length; i++) {
			CopyOnWriteStateMap<K, N, S> stateMap = state[i];
			if (state[i] != null) {
				snapshotArray[i] = stateMap.stateSnapshot();
			}
		}
		return snapshotArray;
	}

	int getKeyGroupOffset() {
		return keyGroupOffset;
	}

	// For testing  ----------------------------------------------------------------------------------------------------

	@Override
	public int sizeOfNamespace(Object namespace) {
		int count = 0;
		for (CopyOnWriteStateMap<K, N, S> stateMap: state) {
			if (stateMap != null) {
				count += stateMap.sizeOfNamespace(namespace);
			}
		}
		return count;
	}

	@Nonnull
	@Override
	public Iterator<StateEntry<K, N, S>> iterator() {
		return Arrays.stream(state)
			.filter(Objects::nonNull)
			.map(CopyOnWriteStateMap::iterator)
			.flatMap(iter -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(iter, 0), false))
			.iterator();
	}

	// StateEntryIterator  ---------------------------------------------------------------------------------------------

	@Override
	public StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
		return new StateEntryIterator(recommendedMaxNumberOfReturnedRecords);
	}

	class StateEntryIterator implements StateIncrementalVisitor<K, N, S> {

		final int recommendedMaxNumberOfReturnedRecords;

		int keyGroupIndex;

		StateIncrementalVisitor<K, N, S> stateIncrementalVisitor;

		StateEntryIterator(int recommendedMaxNumberOfReturnedRecords) {
			this.recommendedMaxNumberOfReturnedRecords = recommendedMaxNumberOfReturnedRecords;
			this.keyGroupIndex = 0;
			next();
		}

		private void next() {
			while (keyGroupIndex < state.length) {
				CopyOnWriteStateMap<K, N, S> stateMap = state[keyGroupIndex++];
				if (stateMap != null) {
					StateIncrementalVisitor<K, N, S> visitor =
						stateMap.getStateIncrementalVisitor(recommendedMaxNumberOfReturnedRecords);
					if (visitor.hasNext()) {
						stateIncrementalVisitor = visitor;
						return;
					}
				}
			}
		}

		@Override
		public boolean hasNext() {
			return stateIncrementalVisitor != null && stateIncrementalVisitor.hasNext();
		}

		@Override
		public Collection<StateEntry<K, N, S>> nextEntries() {
			if (!hasNext()) {
				return null;
			}

			Collection<StateEntry<K, N, S>> collection =
				stateIncrementalVisitor.nextEntries();

			if (!hasNext()) {
				next();
			}

			return collection;
		}

		@Override
		public void remove(StateEntry<K, N, S> stateEntry) {
			state[keyGroupIndex - 1].remove(stateEntry.getKey(), stateEntry.getNamespace());
		}

		@Override
		public void update(StateEntry<K, N, S> stateEntry, S newValue) {
			state[keyGroupIndex - 1].put(stateEntry.getKey(), stateEntry.getNamespace(), newValue);
		}
	}
}
