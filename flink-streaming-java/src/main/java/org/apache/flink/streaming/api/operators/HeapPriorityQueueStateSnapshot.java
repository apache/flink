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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupPartitioner;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.lang.reflect.Array;

/**
 * This class represents the snapshot of an {@link HeapPriorityQueueSet}.
 *
 * @param <T> type of the state elements.
 */
public class HeapPriorityQueueStateSnapshot<T> implements StateSnapshot {

	/** Function that extracts keys from elements. */
	@Nonnull
	private final KeyExtractorFunction<T> keyExtractor;

	/** Copy of the heap array containing all the (immutable or deeply copied) elements. */
	@Nonnull
	private final T[] heapArrayCopy;

	/** The element serializer. */
	@Nonnull
	private final TypeSerializer<T> elementSerializer;

	/** The key-group range covered by this snapshot. */
	@Nonnull
	private final KeyGroupRange keyGroupRange;

	/** The total number of key-groups in the job. */
	@Nonnegative
	private final int totalKeyGroups;

	/** Result of partitioning the snapshot by key-group. */
	@Nullable
	private KeyGroupPartitionedSnapshot partitionedSnapshot;

	HeapPriorityQueueStateSnapshot(
		@Nonnull T[] heapArrayCopy,
		@Nonnull KeyExtractorFunction<T> keyExtractor,
		@Nonnull TypeSerializer<T> elementSerializer,
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnegative int totalKeyGroups) {

		// TODO ensure that the array contains a deep copy of elements if we are *not* dealing with immutable types.
		assert elementSerializer.isImmutableType();

		this.keyExtractor = keyExtractor;
		this.heapArrayCopy = heapArrayCopy;
		this.elementSerializer = elementSerializer;
		this.keyGroupRange = keyGroupRange;
		this.totalKeyGroups = totalKeyGroups;
	}

	@SuppressWarnings("unchecked")
	@Nonnull
	@Override
	public KeyGroupPartitionedSnapshot partitionByKeyGroup() {

		if (partitionedSnapshot == null) {

			T[] partitioningOutput = (T[]) Array.newInstance(
				heapArrayCopy.getClass().getComponentType(),
				heapArrayCopy.length);

			KeyGroupPartitioner<T> keyGroupPartitioner =
				new KeyGroupPartitioner<>(
					heapArrayCopy,
					heapArrayCopy.length,
					partitioningOutput,
					keyGroupRange,
					totalKeyGroups,
					keyExtractor,
					elementSerializer::serialize);

			partitionedSnapshot = keyGroupPartitioner.partitionByKeyGroup();
		}

		return partitionedSnapshot;
	}

	@Override
	public void release() {
	}
}
