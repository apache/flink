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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;

/**
 * This class represents the snapshot of a {@link CopyOnWriteStateTable} and has a role in operator state checkpointing.
 * This class is also responsible for writing the state in the process of checkpointing.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
@Internal
public class CopyOnWriteStateTableSnapshot<K, N, S>
		extends AbstractStateTableSnapshot<K, N, S, CopyOnWriteStateTable<K, N, S>>
		implements StateSnapshot.StateKeyGroupWriter {

	/**
	 * The offset to the contiguous key groups.
	 */
	private final int keyGroupOffset;

	/**
	 * Snapshots of state partitioned by key-group.
	 */
	@Nonnull
	private final CopyOnWriteStateMapSnapshot<K, N, S>[] snapshotStateMap;

	/**
	 * A local duplicate of the table's key serializer.
	 */
	@Nonnull
	private final TypeSerializer<K> localKeySerializer;

	/**
	 * A local duplicate of the table's namespace serializer.
	 */
	@Nonnull
	private final TypeSerializer<N> localNamespaceSerializer;

	/**
	 * A local duplicate of the table's state serializer.
	 */
	@Nonnull
	private final TypeSerializer<S> localStateSerializer;

	@Nullable
	private final StateSnapshotTransformer<S> stateSnapshotTransformer;

	/**
	 * Creates a new {@link CopyOnWriteStateTableSnapshot}.
	 *
	 * @param owningStateTable the {@link CopyOnWriteStateTable} for which this object represents a snapshot.
	 */
	CopyOnWriteStateTableSnapshot(CopyOnWriteStateTable<K, N, S> owningStateTable) {

		super(owningStateTable);
		this.keyGroupOffset = owningStateTable.getKeyGroupOffset();
		this.snapshotStateMap = owningStateTable.getStateMapSnapshotArray();

		// We create duplicates of the serializers for the async snapshot, because TypeSerializer
		// might be stateful and shared with the event processing thread.
		this.localKeySerializer = owningStateTable.keySerializer.duplicate();
		this.localNamespaceSerializer = owningStateTable.metaInfo.getNamespaceSerializer().duplicate();
		this.localStateSerializer = owningStateTable.metaInfo.getStateSerializer().duplicate();

		this.stateSnapshotTransformer = owningStateTable.metaInfo.
			getStateSnapshotTransformFactory().createForDeserializedState().orElse(null);
	}

	@Override
	public StateKeyGroupWriter getKeyGroupWriter() {
		return this;
	}

	@Override
	public void writeStateInKeyGroup(@Nonnull DataOutputView dov, int keyGroupId) throws IOException {
		int indexOffset = keyGroupId - keyGroupOffset;
		CopyOnWriteStateMapSnapshot<K, N, S> stateMapSnapshot = null;
		if (indexOffset >= 0 && indexOffset < snapshotStateMap.length) {
			stateMapSnapshot = snapshotStateMap[indexOffset];
		}

		if (stateMapSnapshot == null) {
			dov.writeInt(0);
			return;
		}

		CopyOnWriteStateMapSnapshot.SnapshotIterator<K, N, S> snapshotIterator =
			stateMapSnapshot.getSnapshotIterator(stateSnapshotTransformer);
		dov.writeInt(snapshotIterator.size());

		while (snapshotIterator.hasNext()) {
			CopyOnWriteStateMap.StateMapEntry<K, N, S> entry = snapshotIterator.next();
			localNamespaceSerializer.serialize(entry.namespace, dov);
			localKeySerializer.serialize(entry.key, dov);
			localStateSerializer.serialize(entry.state, dov);
		}
		stateMapSnapshot.release();
	}

	@Nonnull
	@Override
	public StateMetaInfoSnapshot getMetaInfoSnapshot() {
		return owningStateTable.metaInfo.snapshot();
	}
}
