/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.heap.space.SpaceAllocator;

import javax.annotation.Nonnull;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

public class SpillableStateTable<K, N, S> extends StateTable<K, N, S> implements Closeable {

	private static final int DEFAULT_NUM_KEYS_TO_DELETED_ONE_TIME = 2;

	private static final float DEFAULT_LOGICAL_REMOVE_KEYS_RATIO = 0.2f;

	private SpaceAllocator spaceAllocator;

	/**
	 * Constructs a new {@code SpillableStateTable}.
	 *
	 * @param keyContext    the key context.
	 * @param metaInfo      the meta information, including the type serializer for state copy-on-write.
	 * @param keySerializer the serializer of the key.
	 */
	SpillableStateTable(
		InternalKeyContext<K> keyContext,
		RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
		TypeSerializer<K> keySerializer,
		SpaceAllocator spaceAllocator
		) {
		super(keyContext, metaInfo, keySerializer);
		this.spaceAllocator = spaceAllocator;
	}

	@Override
	protected CopyOnWriteSkipListStateMap<K, N, S> createStateMap() {
		return new CopyOnWriteSkipListStateMap<>(
			getKeySerializer(),
			getNamespaceSerializer(),
			getStateSerializer(),
			spaceAllocator,
			DEFAULT_NUM_KEYS_TO_DELETED_ONE_TIME,
			DEFAULT_LOGICAL_REMOVE_KEYS_RATIO);
	}

	// Snapshotting ----------------------------------------------------------------------------------------------------

	/**
	 * Creates a snapshot of this {@link SpillableStateTable}, to be written in checkpointing.
	 *
	 * @return a snapshot from this {@link SpillableStateTable}, for checkpointing.
	 */
	@Nonnull
	@Override
	public SpillableStateTableSnapshot<K, N, S> stateSnapshot() {
		return new SpillableStateTableSnapshot<>(
			this,
			getKeySerializer().duplicate(),
			getNamespaceSerializer().duplicate(),
			getStateSerializer().duplicate(),
			getMetaInfo().getStateSnapshotTransformFactory().createForDeserializedState().orElse(null));
	}

	@SuppressWarnings("unchecked")
	List<CopyOnWriteSkipListStateMapSnapshot<K, N, S>> getStateMapSnapshotList() {
		List<CopyOnWriteSkipListStateMapSnapshot<K, N, S>> snapshotList = new ArrayList<>(keyGroupedStateMaps.length);
		for (int i = 0; i < keyGroupedStateMaps.length; i++) {
			CopyOnWriteSkipListStateMap<K, N, S> stateMap = (CopyOnWriteSkipListStateMap<K, N, S>) keyGroupedStateMaps[i];
			snapshotList.add(stateMap.stateSnapshot());
		}
		return snapshotList;
	}

	@Override
	public void close() {
		// TODO release resource
	}
}
