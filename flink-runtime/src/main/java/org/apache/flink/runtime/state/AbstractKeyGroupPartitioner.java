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

package org.apache.flink.runtime.state;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Abstract class that contains the base algorithm for partitioning data into key-groups. This algorithm currently works
 * with two array (input, output) for optimal algorithmic complexity. Notice that this could also be implemented over a
 * single array, using some cuckoo-hashing-style element replacement. This would have worse algorithmic complexity but
 * better space efficiency. We currently prefer the trade-off in favor of better algorithmic complexity.
 */
public abstract class AbstractKeyGroupPartitioner<T> {

	/** Total number of input elements. */
	@Nonnegative
	protected final int numberOfElements;

	/** The total number of key-groups in the job. */
	@Nonnegative
	protected final int totalKeyGroups;

	/** The key-group range for the input data, covered in this partitioning. */
	@Nonnull
	protected final KeyGroupRange keyGroupRange;

	/**
	 * This bookkeeping array is used to count the elements in each key-group. In a second step, it is transformed into
	 * a histogram by accumulation.
	 */
	@Nonnull
	protected final int[] counterHistogram;

	/**
	 * This is a helper array that caches the key-group for each element, so we do not have to compute them twice.
	 */
	@Nonnull
	protected final int[] elementKeyGroups;

	/** Cached value of keyGroupRange#firstKeyGroup. */
	@Nonnegative
	protected final int firstKeyGroup;

	/** Cached result. */
	@Nullable
	protected PartitioningResult<T> computedResult;

	/**
	 * @param keyGroupRange the key-group range of the data that will be partitioned by this instance.
	 * @param totalKeyGroups the total number of key groups in the job.
	 */
	public AbstractKeyGroupPartitioner(
		@Nonnegative int numberOfElements,
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnegative int totalKeyGroups) {

		this.numberOfElements = numberOfElements;
		this.keyGroupRange = keyGroupRange;
		this.totalKeyGroups = totalKeyGroups;
		this.firstKeyGroup = keyGroupRange.getStartKeyGroup();
		this.elementKeyGroups = new int[numberOfElements];
		this.counterHistogram = new int[keyGroupRange.getNumberOfKeyGroups()];
		this.computedResult = null;
	}

	/**
	 * Partitions the data into key-groups and returns the result via {@link PartitioningResult}.
	 */
	public PartitioningResult<T> partitionByKeyGroup() {
		if (computedResult == null) {
			reportAllElementKeyGroups();
			buildHistogramFromCounts();
			executePartitioning();
		}
		return computedResult;
	}

	/**
	 * This method iterates over the input data and reports the key-group for each element.
	 */
	protected void reportAllElementKeyGroups() {
		final T[] input = getPartitioningInput();

		Preconditions.checkState(input.length >= numberOfElements);

		for (int i = 0; i < numberOfElements; ++i) {
			int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(extractKeyFromElement(input[i]), totalKeyGroups);
			reportKeyGroupOfElementAtIndex(i, keyGroup);
		}
	}

	/**
	 * Returns the key for the given element by which the key-group can be computed.
	 */
	@Nonnull
	protected abstract Object extractKeyFromElement(T element);

	/**
	 * Returns the input data for the partitioning. All elements to consider must be densely in the index interval
	 * [0, {@link #numberOfElements}[, without null values.
	 */
	@Nonnull
	protected abstract T[] getPartitioningInput();

	/**
	 * Returns the output array for the partitioning. The size must be {@link #numberOfElements} (or bigger).
	 */
	@Nonnull
	protected abstract T[] getPartitioningOutput();

	/**
	 * This method reports in the bookkeeping data that the element at the given index belongs to the given key-group.
	 */
	protected void reportKeyGroupOfElementAtIndex(int index, int keyGroup) {
		final int keyGroupIndex = keyGroup - firstKeyGroup;
		elementKeyGroups[index] = keyGroupIndex;
		++counterHistogram[keyGroupIndex];
	}

	/**
	 * This method creates a histogram from the counts per key-group in {@link #counterHistogram}.
	 */
	private void buildHistogramFromCounts() {
		int sum = 0;
		for (int i = 0; i < counterHistogram.length; ++i) {
			int currentSlotValue = counterHistogram[i];
			counterHistogram[i] = sum;
			sum += currentSlotValue;
		}
	}

	private void executePartitioning() {

		final T[] in = getPartitioningInput();
		final T[] out = getPartitioningOutput();

		Preconditions.checkState(in != out);
		Preconditions.checkState(in.length >= numberOfElements);
		Preconditions.checkState(out.length >= numberOfElements);

		// We repartition the entries by their pre-computed key-groups, using the histogram values as write indexes
		for (int inIdx = 0; inIdx < numberOfElements; ++inIdx) {
			int effectiveKgIdx = elementKeyGroups[inIdx];
			int outIdx = counterHistogram[effectiveKgIdx]++;
			out[outIdx] = in[inIdx];
		}

		this.computedResult = new PartitioningResult<>(firstKeyGroup, counterHistogram, out);
	}

	/**
	 * This represents the result of key-group partitioning. The data in {@link #partitionedElements} is partitioned
	 * w.r.t. {@link AbstractKeyGroupPartitioner#keyGroupRange}.
	 */
	public static class PartitioningResult<T> {

		/**
		 * The exclusive-end-offsets for all key-groups of the covered range for the partitioning. Exclusive-end-offset
		 * for key-group n is under keyGroupOffsets[n - firstKeyGroup].
		 */
		@Nonnull
		private final int[] keyGroupOffsets;

		/**
		 * Array with elements that are partitioned w.r.t. the covered key-group range. The start offset for each
		 * key-group is in {@link #keyGroupOffsets}.
		 */
		@Nonnull
		private final T[] partitionedElements;

		private final int firstKeyGroup;

		PartitioningResult(
			@Nonnegative int firstKeyGroup,
			@Nonnull int[] keyGroupEndOffsets,
			@Nonnull T[] partitionedElements) {

			this.firstKeyGroup = firstKeyGroup;
			this.keyGroupOffsets = keyGroupEndOffsets;
			this.partitionedElements = partitionedElements;
		}

		@Nonnull
		public T[] getPartitionedElements() {
			return partitionedElements;
		}

		@Nonnegative
		public int getKeyGroupStartOffsetInclusive(int keyGroup) {
			int idx = keyGroup - firstKeyGroup - 1;
			return idx < 0 ? 0 : keyGroupOffsets[idx];
		}

		@Nonnegative
		public int getKeyGroupEndOffsetExclusive(int keyGroup) {
			return keyGroupOffsets[keyGroup - firstKeyGroup];
		}

		@Nonnegative
		public int getFirstKeyGroup() {
			return firstKeyGroup;
		}

		@Nonnegative
		public int getNumberOfKeyGroups() {
			return keyGroupOffsets.length;
		}
	}
}
