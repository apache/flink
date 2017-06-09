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
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import java.io.IOException;

/**
 * This class represents the snapshot of a {@link CopyOnWriteStateTable} and has a role in operator state checkpointing. Besides
 * holding the {@link CopyOnWriteStateTable}s internal entries at the time of the snapshot, this class is also responsible for
 * preparing and writing the state in the process of checkpointing.
 * <p>
 * IMPORTANT: Please notice that snapshot integrity of entries in this class rely on proper copy-on-write semantics
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
	 * The number of entries in the {@link CopyOnWriteStateTable} at the time of creating this snapshot.
	 */
	private final int stateTableSize;

	/**
	 * The state table entries, as by the time this snapshot was created. Objects in this array may or may not be deep
	 * copies of the current entries in the {@link CopyOnWriteStateTable} that created this snapshot. This depends for each entry
	 * on whether or not it was subject to copy-on-write operations by the {@link CopyOnWriteStateTable}.
	 */
	private final CopyOnWriteStateTable.StateTableEntry<K, N, S>[] snapshotData;

	/**
	 * Offsets for the individual key-groups. This is lazily created when the snapshot is grouped by key-group during
	 * the process of writing this snapshot to an output as part of checkpointing.
	 */
	private int[] keyGroupOffsets;

	/**
	 * A local duplicate of the table's key serializer.
	 */
	private final TypeSerializer<K> localKeySerializer;

	/**
	 * A local duplicate of the table's namespace serializer.
	 */
	private final TypeSerializer<N> localNamespaceSerializer;

	/**
	 * A local duplicate of the table's state serializer.
	 */
	private final TypeSerializer<S> localStateSerializer;

	/**
	 * Creates a new {@link CopyOnWriteStateTableSnapshot}.
	 *
	 * @param owningStateTable the {@link CopyOnWriteStateTable} for which this object represents a snapshot.
	 */
	CopyOnWriteStateTableSnapshot(CopyOnWriteStateTable<K, N, S> owningStateTable) {

		super(owningStateTable);
		this.snapshotData = owningStateTable.snapshotTableArrays();
		this.snapshotVersion = owningStateTable.getStateTableVersion();
		this.stateTableSize = owningStateTable.size();

		// We create duplicates of the serializers for the async snapshot, because TypeSerializer
		// might be stateful and shared with the event processing thread.
		this.localKeySerializer = owningStateTable.keyContext.getKeySerializer().duplicate();
		this.localNamespaceSerializer = owningStateTable.metaInfo.getNamespaceSerializer().duplicate();
		this.localStateSerializer = owningStateTable.metaInfo.getStateSerializer().duplicate();

		this.keyGroupOffsets = null;
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
	 * <p>
	 * As a possible future optimization, we could perform the repartitioning in-place, using a scheme similar to the
	 * cuckoo cycles in cuckoo hashing. This can trade some performance for a smaller memory footprint.
	 */
	@SuppressWarnings("unchecked")
	private void partitionEntriesByKeyGroup() {

		// We only have to perform this step once before the first key-group is written
		if (null != keyGroupOffsets) {
			return;
		}

		final KeyGroupRange keyGroupRange = owningStateTable.keyContext.getKeyGroupRange();
		final int totalKeyGroups = owningStateTable.keyContext.getNumberOfKeyGroups();
		final int baseKgIdx = keyGroupRange.getStartKeyGroup();
		final int[] histogram = new int[keyGroupRange.getNumberOfKeyGroups() + 1];

		CopyOnWriteStateTable.StateTableEntry<K, N, S>[] unfold = new CopyOnWriteStateTable.StateTableEntry[stateTableSize];

		// 1) In this step we i) 'unfold' the linked list of entries to a flat array and ii) build a histogram for key-groups
		int unfoldIndex = 0;
		for (CopyOnWriteStateTable.StateTableEntry<K, N, S> entry : snapshotData) {
			while (null != entry) {
				int effectiveKgIdx =
						KeyGroupRangeAssignment.computeKeyGroupForKeyHash(entry.key.hashCode(), totalKeyGroups) - baseKgIdx + 1;
				++histogram[effectiveKgIdx];
				unfold[unfoldIndex++] = entry;
				entry = entry.next;
			}
		}

		// 2) We accumulate the histogram bins to obtain key-group ranges in the final array
		for (int i = 1; i < histogram.length; ++i) {
			histogram[i] += histogram[i - 1];
		}

		// 3) We repartition the entries by key-group, using the histogram values as write indexes
		for (CopyOnWriteStateTable.StateTableEntry<K, N, S> t : unfold) {
			int effectiveKgIdx =
					KeyGroupRangeAssignment.computeKeyGroupForKeyHash(t.key.hashCode(), totalKeyGroups) - baseKgIdx;
			snapshotData[histogram[effectiveKgIdx]++] = t;
		}

		// 4) As byproduct, we also created the key-group offsets
		this.keyGroupOffsets = histogram;
	}

	@Override
	public void release() {
		owningStateTable.releaseSnapshot(this);
	}

	@Override
	public void writeMappingsInKeyGroup(DataOutputView dov, int keyGroupId) throws IOException {

		if (null == keyGroupOffsets) {
			partitionEntriesByKeyGroup();
		}

		final CopyOnWriteStateTable.StateTableEntry<K, N, S>[] groupedOut = snapshotData;
		KeyGroupRange keyGroupRange = owningStateTable.keyContext.getKeyGroupRange();
		int keyGroupOffsetIdx = keyGroupId - keyGroupRange.getStartKeyGroup() - 1;
		int startOffset = keyGroupOffsetIdx < 0 ? 0 : keyGroupOffsets[keyGroupOffsetIdx];
		int endOffset = keyGroupOffsets[keyGroupOffsetIdx + 1];

		// write number of mappings in key-group
		dov.writeInt(endOffset - startOffset);

		// write mappings
		for (int i = startOffset; i < endOffset; ++i) {
			CopyOnWriteStateTable.StateTableEntry<K, N, S> toWrite = groupedOut[i];
			groupedOut[i] = null; // free asap for GC
			localNamespaceSerializer.serialize(toWrite.namespace, dov);
			localKeySerializer.serialize(toWrite.key, dov);
			localStateSerializer.serialize(toWrite.state, dov);
		}
	}

	/**
	 * Returns true iff the given state table is the owner of this snapshot object.
	 */
	boolean isOwner(CopyOnWriteStateTable<K, N, S> stateTable) {
		return stateTable == owningStateTable;
	}
}
