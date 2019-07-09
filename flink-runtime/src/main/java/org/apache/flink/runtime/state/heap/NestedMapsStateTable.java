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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotTransformer;

import javax.annotation.Nonnull;

/**
 * This implementation of {@link StateTable} uses {@link NestedStateMap}.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of state.
 */
@Internal
public class NestedMapsStateTable<K, N, S> extends StateTable<K, N, S> {

	/**
	 * Creates a new {@link NestedMapsStateTable} for the given key context and meta info.
	 * @param keyContext the key context.
	 * @param metaInfo the meta information for this state table.
	 * @param keySerializer the serializer of the key.
	 */
	public NestedMapsStateTable(
		InternalKeyContext<K> keyContext,
		RegisteredKeyValueStateBackendMetaInfo<N, S> metaInfo,
		TypeSerializer<K> keySerializer) {
		super(keyContext, metaInfo, keySerializer);
	}

	@Override
	protected NestedStateMap<K, N, S> createStateMap() {
		return new NestedStateMap<>();
	}

	// Snapshotting ----------------------------------------------------------------------------------------------------

	@Nonnull
	@Override
	public NestedMapsStateTableSnapshot<K, N, S> stateSnapshot() {
		return new NestedMapsStateTableSnapshot<>(
			this,
			getKeySerializer(),
			getNamespaceSerializer(),
			getStateSerializer(),
			getMetaInfo().getStateSnapshotTransformFactory().createForDeserializedState().orElse(null));
	}

	/**
	 * This class encapsulates the snapshot logic.
	 *
	 * @param <K> type of key.
	 * @param <N> type of namespace.
	 * @param <S> type of state.
	 */
	static class NestedMapsStateTableSnapshot<K, N, S>
			extends AbstractStateTableSnapshot<K, N, S> {

		NestedMapsStateTableSnapshot(
			NestedMapsStateTable<K, N, S> owningTable,
			TypeSerializer<K> localKeySerializer,
			TypeSerializer<N> localNamespaceSerializer,
			TypeSerializer<S> localStateSerializer,
			StateSnapshotTransformer<S> stateSnapshotTransformer) {
			super(owningTable,
				localKeySerializer,
				localNamespaceSerializer,
				localStateSerializer,
				stateSnapshotTransformer);
		}

		@Override
		protected StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> getStateMapSnapshotForKeyGroup(int keyGroup) {
			NestedStateMap<K, N, S> stateMap = (NestedStateMap<K, N, S>) owningStateTable.getMapForKeyGroup(keyGroup);

			return stateMap.stateSnapshot();
		}
	}
}
