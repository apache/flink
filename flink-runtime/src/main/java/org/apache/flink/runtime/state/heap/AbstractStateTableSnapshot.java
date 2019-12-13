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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Abstract base class for snapshots of a {@link StateTable}. Offers a way to serialize the snapshot (by key-group).
 * All snapshots should be released after usage.
 */
@Internal
abstract class AbstractStateTableSnapshot<K, N, S>
	implements StateSnapshot, StateSnapshot.StateKeyGroupWriter {

	/**
	 * The {@link StateTable} from which this snapshot was created.
	 */
	protected final StateTable<K, N, S> owningStateTable;

	/**
	 * A local duplicate of the table's key serializer.
	 */
	@Nonnull
	protected final TypeSerializer<K> localKeySerializer;

	/**
	 * A local duplicate of the table's namespace serializer.
	 */
	@Nonnull
	protected final TypeSerializer<N> localNamespaceSerializer;

	/**
	 * A local duplicate of the table's state serializer.
	 */
	@Nonnull
	protected final TypeSerializer<S> localStateSerializer;

	@Nullable
	protected final StateSnapshotTransformer<S> stateSnapshotTransformer;

	/**
	 * Creates a new {@link AbstractStateTableSnapshot} for and owned by the given table.
	 *
	 * @param owningStateTable the {@link StateTable} for which this object represents a snapshot.
	 */
	AbstractStateTableSnapshot(
		StateTable<K, N, S> owningStateTable,
		TypeSerializer<K> localKeySerializer,
		TypeSerializer<N> localNamespaceSerializer,
		TypeSerializer<S> localStateSerializer,
		@Nullable StateSnapshotTransformer<S> stateSnapshotTransformer) {
		this.owningStateTable = Preconditions.checkNotNull(owningStateTable);
		this.localKeySerializer = Preconditions.checkNotNull(localKeySerializer);
		this.localNamespaceSerializer = Preconditions.checkNotNull(localNamespaceSerializer);
		this.localStateSerializer = Preconditions.checkNotNull(localStateSerializer);
		this.stateSnapshotTransformer = stateSnapshotTransformer;
	}

	/**
	 * Return the state map snapshot for the key group. If the snapshot does not exist, return null.
	 */
	protected abstract StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> getStateMapSnapshotForKeyGroup(int keyGroup);

	@Nonnull
	@Override
	public StateMetaInfoSnapshot getMetaInfoSnapshot() {
		return owningStateTable.getMetaInfo().snapshot();
	}

	@Override
	public StateKeyGroupWriter getKeyGroupWriter() {
		return this;
	}

	/**
	 * Implementation note: we currently chose the same format between {@link NestedMapsStateTable} and
	 * {@link CopyOnWriteStateTable}.
	 *
	 * <p>{@link NestedMapsStateTable} could naturally support a kind of
	 * prefix-compressed format (grouping by namespace, writing the namespace only once per group instead for each
	 * mapping). We might implement support for different formats later (tailored towards different state table
	 * implementations).
	 */
	@Override
	public void writeStateInKeyGroup(@Nonnull DataOutputView dov, int keyGroupId) throws IOException {
		StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> stateMapSnapshot = getStateMapSnapshotForKeyGroup(keyGroupId);
		stateMapSnapshot.writeState(localKeySerializer, localNamespaceSerializer, localStateSerializer, dov, stateSnapshotTransformer);
		stateMapSnapshot.release();
	}
}
