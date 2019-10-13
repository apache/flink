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
import org.apache.flink.runtime.state.StateSnapshotTransformer;

import javax.annotation.Nonnull;

import java.util.List;

public class SpillableStateTableSnapshot<K, N, S> extends AbstractStateTableSnapshot<K, N, S> {

	/**
	 * The offset to the contiguous key groups.
	 */
	private final int keyGroupOffset;

	/**
	 * Snapshots of state partitioned by key-group.
	 */
	@Nonnull
	private final List<CopyOnWriteSkipListStateMapSnapshot<K, N, S>> stateMapSnapshots;

	/**
	 * Creates a new {@link CopyOnWriteSkipListStateMapSnapshot}.
	 *
	 * @param owningStateTable the {@link SpillableStateTable} for which this object represents a snapshot.
	 */
	SpillableStateTableSnapshot(
		SpillableStateTable<K, N, S> owningStateTable,
		TypeSerializer<K> localKeySerializer,
		TypeSerializer<N> localNamespaceSerializer,
		TypeSerializer<S> localStateSerializer,
		StateSnapshotTransformer<S> stateSnapshotTransformer) {
		super(owningStateTable,
			localKeySerializer,
			localNamespaceSerializer,
			localStateSerializer,
			stateSnapshotTransformer);

		this.keyGroupOffset = owningStateTable.getKeyGroupOffset();
		this.stateMapSnapshots = owningStateTable.getStateMapSnapshotList();
	}

	@Override
	protected StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> getStateMapSnapshotForKeyGroup(int keyGroup) {
		int indexOffset = keyGroup - keyGroupOffset;
		CopyOnWriteSkipListStateMapSnapshot<K, N, S> stateMapSnapshot = null;
		if (indexOffset >= 0 && indexOffset < stateMapSnapshots.size()) {
			stateMapSnapshot = stateMapSnapshots.get(indexOffset);
		}

		return stateMapSnapshot;
	}
}
