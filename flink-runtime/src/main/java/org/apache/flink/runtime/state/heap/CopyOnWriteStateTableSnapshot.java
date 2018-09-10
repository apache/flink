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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupPartitioner;
import org.apache.flink.runtime.state.KeyGroupPartitioner.ElementWriterFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * This class represents the snapshot of a {@link CopyOnWriteStateTable} and has a role in operator state checkpointing. Besides
 * holding the {@link CopyOnWriteStateTable}s internal entries at the time of the snapshot, this class is also responsible for
 * preparing and writing the state in the process of checkpointing.
 *
 * <p>IMPORTANT: Please notice that snapshot integrity of entries in this class rely on proper copy-on-write semantics
 * through the {@link CopyOnWriteStateTable} that created the snapshot object, but all objects in this snapshot must be considered
 * as READ-ONLY!. The reason is that the objects held by this class may or may not be deep copies of original objects
 * that may still used in the {@link CopyOnWriteStateTable}. This depends for each entry on whether or not it was subject to
 * copy-on-write operations by the {@link CopyOnWriteStateTable}. Phrased differently: the {@link CopyOnWriteStateTable} provides
 * copy-on-write isolation for this snapshot, but this snapshot does not isolate modifications from the
 * {@link CopyOnWriteStateTable}!
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
@Internal
public class CopyOnWriteStateTableSnapshot<K, N, S>
		extends AbstractStateTableSnapshot<K, N, S, CopyOnWriteStateTable<K, N, S>> {

	/**
	 * Version of the {@link CopyOnWriteStateTable} when this snapshot was created. This can be used to release the snapshot.
	 */
	private final int snapshotVersion;

	/**
	 * The state table entries, as by the time this snapshot was created. Objects in this array may or may not be deep
	 * copies of the current entries in the {@link CopyOnWriteStateTable} that created this snapshot. This depends for each entry
	 * on whether or not it was subject to copy-on-write operations by the {@link CopyOnWriteStateTable}.
	 */
	@Nonnull
	private final CopyOnWriteStateTable.StateTableEntry<K, N, S>[] snapshotData;

	/** The number of (non-null) entries in snapshotData. */
	@Nonnegative
	private final int numberOfEntriesInSnapshotData;

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

	/**
	 * Result of partitioning the snapshot by key-group. This is lazily created in the process of writing this snapshot
	 * to an output as part of checkpointing.
	 */
	@Nullable
	private StateKeyGroupWriter partitionedStateTableSnapshot;

	/**
	 * Creates a new {@link CopyOnWriteStateTableSnapshot}.
	 *
	 * @param owningStateTable the {@link CopyOnWriteStateTable} for which this object represents a snapshot.
	 */
	CopyOnWriteStateTableSnapshot(CopyOnWriteStateTable<K, N, S> owningStateTable) {

		super(owningStateTable);
		this.snapshotData = owningStateTable.snapshotTableArrays();
		this.snapshotVersion = owningStateTable.getStateTableVersion();
		this.numberOfEntriesInSnapshotData = owningStateTable.size();

		// We create duplicates of the serializers for the async snapshot, because TypeSerializer
		// might be stateful and shared with the event processing thread.
		this.localKeySerializer = owningStateTable.keyContext.getKeySerializer().duplicate();
		this.localNamespaceSerializer = owningStateTable.metaInfo.getNamespaceSerializer().duplicate();
		this.localStateSerializer = owningStateTable.metaInfo.getStateSerializer().duplicate();

		this.partitionedStateTableSnapshot = null;
	}

	/**
	 * Returns the internal version of the {@link CopyOnWriteStateTable} when this snapshot was created. This value must be used to
	 * tell the {@link CopyOnWriteStateTable} when to release this snapshot.
	 */
	int getSnapshotVersion() {
		return snapshotVersion;
	}

	/**
	 * Partitions the snapshot data by key-group. The algorithm first builds a histogram for the distribution of keys
	 * into key-groups. Then, the histogram is accumulated to obtain the boundaries of each key-group in an array.
	 * Last, we use the accumulated counts as write position pointers for the key-group's bins when reordering the
	 * entries by key-group. This operation is lazily performed before the first writing of a key-group.
	 *
	 * <p>As a possible future optimization, we could perform the repartitioning in-place, using a scheme similar to the
	 * cuckoo cycles in cuckoo hashing. This can trade some performance for a smaller memory footprint.
	 */
	@Nonnull
	@SuppressWarnings("unchecked")
	@Override
	public StateKeyGroupWriter getKeyGroupWriter() {
		if (partitionedStateTableSnapshot == null) {
			final InternalKeyContext<K> keyContext = owningStateTable.keyContext;
			final int numberOfKeyGroups = keyContext.getNumberOfKeyGroups();
			final KeyGroupRange keyGroupRange = keyContext.getKeyGroupRange();
			ElementWriterFunction<CopyOnWriteStateTable.StateTableEntry<K, N, S>> elementWriterFunction =
				(element, dov) -> {
					localNamespaceSerializer.serialize(element.namespace, dov);
					localKeySerializer.serialize(element.key, dov);
					localStateSerializer.serialize(element.state, dov);
				};
			StateSnapshotTransformer<S> stateSnapshotTransformer = owningStateTable.metaInfo.getSnapshotTransformer();
			StateTableKeyGroupPartitioner<K, N, S> stateTableKeyGroupPartitioner = stateSnapshotTransformer != null ?
				new TransformingStateTableKeyGroupPartitioner<>(
					snapshotData,
					numberOfEntriesInSnapshotData,
					keyGroupRange,
					numberOfKeyGroups,
					elementWriterFunction,
					stateSnapshotTransformer) :
				new StateTableKeyGroupPartitioner<>(
					snapshotData,
					numberOfEntriesInSnapshotData,
					keyGroupRange,
					numberOfKeyGroups,
					elementWriterFunction);
			partitionedStateTableSnapshot = stateTableKeyGroupPartitioner.partitionByKeyGroup();
		}
		return partitionedStateTableSnapshot;
	}

	@Nonnull
	@Override
	public StateMetaInfoSnapshot getMetaInfoSnapshot() {
		return owningStateTable.metaInfo.snapshot();
	}

	@Override
	public void release() {
		owningStateTable.releaseSnapshot(this);
	}

	/**
	 * Returns true iff the given state table is the owner of this snapshot object.
	 */
	boolean isOwner(CopyOnWriteStateTable<K, N, S> stateTable) {
		return stateTable == owningStateTable;
	}

	/**
	 * This class is the implementation of {@link KeyGroupPartitioner} for {@link CopyOnWriteStateTable}. This class
	 * swaps input and output in {@link #reportAllElementKeyGroups()} for performance reasons, so that we can reuse
	 * the non-flattened original snapshot array as partitioning output.
	 *
	 * @param <K> type of key.
	 * @param <N> type of namespace.
	 * @param <S> type of state value.
	 */
	@VisibleForTesting
	protected static class StateTableKeyGroupPartitioner<K, N, S>
		extends KeyGroupPartitioner<CopyOnWriteStateTable.StateTableEntry<K, N, S>> {

		@SuppressWarnings("unchecked")
		StateTableKeyGroupPartitioner(
			@Nonnull CopyOnWriteStateTable.StateTableEntry<K, N, S>[] snapshotData,
			@Nonnegative int stateTableSize,
			@Nonnull KeyGroupRange keyGroupRange,
			@Nonnegative int totalKeyGroups,
			@Nonnull ElementWriterFunction<CopyOnWriteStateTable.StateTableEntry<K, N, S>> elementWriterFunction) {

			super(
				new CopyOnWriteStateTable.StateTableEntry[stateTableSize],
				stateTableSize,
				// We have made sure that the snapshotData is big enough to hold the flattened entries in
				// CopyOnWriteStateTable#snapshotTableArrays(), we can safely reuse it as the destination array here.
				snapshotData,
				keyGroupRange,
				totalKeyGroups,
				CopyOnWriteStateTable.StateTableEntry::getKey,
				elementWriterFunction);
		}

		@Override
		protected void reportAllElementKeyGroups() {
			// In this step we i) 'flatten' the linked list of entries to a second array and ii) report key-groups.
			int flattenIndex = 0;
			for (CopyOnWriteStateTable.StateTableEntry<K, N, S> entry : partitioningDestination) {
				while (null != entry) {
					flattenIndex = tryAddToSource(flattenIndex, entry);
					entry = entry.next;
				}
			}
		}

		/** Tries to append next entry to {@code partitioningSource} array snapshot and returns next index.*/
		int tryAddToSource(int currentIndex, CopyOnWriteStateTable.StateTableEntry<K, N, S> entry) {
			final int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(entry.key, totalKeyGroups);
			reportKeyGroupOfElementAtIndex(currentIndex, keyGroup);
			partitioningSource[currentIndex] = entry;
			return currentIndex + 1;
		}
	}

	/**
	 * Extended state snapshot transforming {@link StateTableKeyGroupPartitioner}.
	 *
	 * <p>This partitioner can additionally transform state before including or not into the snapshot.
	 */
	protected static final class TransformingStateTableKeyGroupPartitioner<K, N, S>
		extends StateTableKeyGroupPartitioner<K, N, S> {
		private final StateSnapshotTransformer<S> stateSnapshotTransformer;

		TransformingStateTableKeyGroupPartitioner(
			@Nonnull CopyOnWriteStateTable.StateTableEntry<K, N, S>[] snapshotData,
			int stateTableSize,
			@Nonnull KeyGroupRange keyGroupRange,
			int totalKeyGroups,
			@Nonnull ElementWriterFunction<CopyOnWriteStateTable.StateTableEntry<K, N, S>> elementWriterFunction,
			@Nonnull StateSnapshotTransformer<S> stateSnapshotTransformer) {
			super(
				snapshotData,
				stateTableSize,
				keyGroupRange,
				totalKeyGroups,
				elementWriterFunction);
			this.stateSnapshotTransformer = stateSnapshotTransformer;
		}

		@Override
		int tryAddToSource(int currentIndex, CopyOnWriteStateTable.StateTableEntry<K, N, S> entry) {
			CopyOnWriteStateTable.StateTableEntry<K, N, S> filteredEntry = filterEntry(entry);
			if (filteredEntry != null) {
				return super.tryAddToSource(currentIndex, filteredEntry);
			}
			return currentIndex;
		}

		private CopyOnWriteStateTable.StateTableEntry<K, N, S> filterEntry(
			CopyOnWriteStateTable.StateTableEntry<K, N, S> entry) {
			S transformedValue = stateSnapshotTransformer.filterOrTransform(entry.state);
			if (transformedValue != null) {
				CopyOnWriteStateTable.StateTableEntry<K, N, S> filteredEntry = entry;
				if (transformedValue != entry.state) {
					filteredEntry = new CopyOnWriteStateTable.StateTableEntry<>(entry, entry.entryVersion);
					filteredEntry.state = transformedValue;
				}
				return filteredEntry;
			}
			return null;
		}
	}
}
