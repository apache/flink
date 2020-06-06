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

import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * This implementation of {@link InternalPriorityQueue} is internally partitioned into sub-queues per key-group and
 * essentially works as a heap-of-heaps. Instances will have set semantics for elements if the sub-queues have set
 * semantics.
 *
 * @param <T> the type of elements in the queue.
 * @param <PQ> type type of sub-queue used for each key-group partition.
 */
public class KeyGroupPartitionedPriorityQueue<T, PQ extends InternalPriorityQueue<T> & HeapPriorityQueueElement>
	implements InternalPriorityQueue<T>, KeyGroupedInternalPriorityQueue<T> {

	/** A heap of heap sets. Each sub-heap represents the partition for a key-group.*/
	@Nonnull
	private final HeapPriorityQueue<PQ> heapOfKeyGroupedHeaps;

	/** All elements from keyGroupHeap, indexed by their key-group id, relative to firstKeyGroup. */
	@Nonnull
	private final PQ[] keyGroupedHeaps;

	/** Function to extract the key from contained elements. */
	@Nonnull
	private final KeyExtractorFunction<T> keyExtractor;

	/** The total number of key-groups (in the job). */
	@Nonnegative
	private final int totalKeyGroups;

	/** The smallest key-group id with a subpartition managed by this ordered set. */
	@Nonnegative
	private final int firstKeyGroup;

	@SuppressWarnings("unchecked")
	public KeyGroupPartitionedPriorityQueue(
		@Nonnull KeyExtractorFunction<T> keyExtractor,
		@Nonnull PriorityComparator<T> elementPriorityComparator,
		@Nonnull PartitionQueueSetFactory<T, PQ> orderedCacheFactory,
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnegative int totalKeyGroups) {

		this.keyExtractor = keyExtractor;
		this.totalKeyGroups = totalKeyGroups;
		this.firstKeyGroup = keyGroupRange.getStartKeyGroup();
		this.keyGroupedHeaps = (PQ[]) new InternalPriorityQueue[keyGroupRange.getNumberOfKeyGroups()];
		this.heapOfKeyGroupedHeaps = new HeapPriorityQueue<>(
			new InternalPriorityQueueComparator<>(elementPriorityComparator),
			keyGroupRange.getNumberOfKeyGroups());
		for (int i = 0; i < keyGroupedHeaps.length; i++) {
			final PQ keyGroupSubHeap =
				orderedCacheFactory.create(firstKeyGroup + i, totalKeyGroups, keyExtractor, elementPriorityComparator);
			keyGroupedHeaps[i] = keyGroupSubHeap;
			heapOfKeyGroupedHeaps.add(keyGroupSubHeap);
		}
	}

	@Nullable
	@Override
	public T poll() {
		final PQ headList = heapOfKeyGroupedHeaps.peek();
		final T head = headList.poll();
		heapOfKeyGroupedHeaps.adjustModifiedElement(headList);
		return head;
	}

	@Nullable
	@Override
	public T peek() {
		return heapOfKeyGroupedHeaps.peek().peek();
	}

	@Override
	public boolean add(@Nonnull T toAdd) {
		final PQ list = getKeyGroupSubHeapForElement(toAdd);

		// the branch checks if the head element has (potentially) changed.
		if (list.add(toAdd)) {
			heapOfKeyGroupedHeaps.adjustModifiedElement(list);
			// could we have a new head?
			return toAdd.equals(peek());
		} else {
			// head unchanged
			return false;
		}
	}

	@Override
	public boolean remove(@Nonnull T toRemove) {
		final PQ list = getKeyGroupSubHeapForElement(toRemove);

		final T oldHead = peek();

		// the branch checks if the head element has (potentially) changed.
		if (list.remove(toRemove)) {
			heapOfKeyGroupedHeaps.adjustModifiedElement(list);
			// could we have a new head?
			return toRemove.equals(oldHead);
		} else {
			// head unchanged
			return false;
		}
	}

	@Override
	public boolean isEmpty() {
		return peek() == null;
	}

	@Override
	public int size() {
		int sizeSum = 0;
		for (PQ list : keyGroupedHeaps) {
			sizeSum += list.size();
		}
		return sizeSum;
	}

	@Override
	public void addAll(@Nullable Collection<? extends T> toAdd) {

		if (toAdd == null) {
			return;
		}

		// TODO consider bulk loading the partitions and "heapify" keyGroupHeap once after all elements are inserted.
		for (T element : toAdd) {
			add(element);
		}
	}

	@Nonnull
	@Override
	public CloseableIterator<T> iterator() {
		return new KeyGroupConcatenationIterator<>(keyGroupedHeaps);
	}

	private PQ getKeyGroupSubHeapForElement(T element) {
		return keyGroupedHeaps[computeKeyGroupIndex(element)];
	}

	private int computeKeyGroupIndex(T element) {
		final Object extractKeyFromElement = keyExtractor.extractKeyFromElement(element);
		final int keyGroupId = KeyGroupRangeAssignment.assignToKeyGroup(extractKeyFromElement, totalKeyGroups);
		return globalKeyGroupToLocalIndex(keyGroupId);
	}

	private int globalKeyGroupToLocalIndex(int keyGroupId) {
		int keyGroupIndex = keyGroupId - firstKeyGroup;
		checkArgument((keyGroupIndex >= 0 && keyGroupIndex < keyGroupedHeaps.length),
			"key group from %s to %s does not contain %s", firstKeyGroup,
			(firstKeyGroup + keyGroupedHeaps.length), keyGroupId);
		return keyGroupIndex;
	}

	@Nonnull
	@Override
	public Set<T> getSubsetForKeyGroup(int keyGroupId) {
		HashSet<T> result = new HashSet<>();
		PQ partitionQueue = keyGroupedHeaps[globalKeyGroupToLocalIndex(keyGroupId)];
		try (CloseableIterator<T> iterator = partitionQueue.iterator()) {
			while (iterator.hasNext()) {
				result.add(iterator.next());
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException("Exception while iterating key group.", e);
		}
		return result;
	}

	/**
	 * Iterator for {@link KeyGroupPartitionedPriorityQueue}. This iterator is not guaranteeing any order of elements.
	 * Using code must {@link #close()} after usage.
	 *
	 * @param <T> the type of iterated elements.
	 */
	private static final class KeyGroupConcatenationIterator<
		T, PQS extends InternalPriorityQueue<T> & HeapPriorityQueueElement>
		implements CloseableIterator<T> {

		/** Array with the subpartitions that we iterate. No null values in the array. */
		@Nonnull
		private final PQS[] keyGroupLists;

		/** The subpartition the is currently iterated. */
		@Nonnegative
		private int index;

		/** The iterator of the current subpartition. */
		@Nonnull
		private CloseableIterator<T> current;

		private KeyGroupConcatenationIterator(@Nonnull PQS[] keyGroupLists) {
			this.keyGroupLists = keyGroupLists;
			this.index = 0;
			this.current = CloseableIterator.empty();
		}

		@Override
		public boolean hasNext() {
			boolean currentHasNext = current.hasNext();

			// find the iterator of the next partition that has elements.
			while (!currentHasNext && index < keyGroupLists.length) {
				IOUtils.closeQuietly(current);
				current = keyGroupLists[index++].iterator();
				currentHasNext = current.hasNext();
			}
			return currentHasNext;
		}

		@Override
		public T next() {
			return current.next();
		}

		@Override
		public void close() throws Exception {
			current.close();
		}
	}

	/**
	 * Comparator that compares {@link InternalPriorityQueue} objects by their head element. Must handle null results
	 * from {@link #peek()}.
	 *
	 * @param <T> type of the elements in the compared queues.
	 * @param <Q> type of queue.
	 */
	private static final class InternalPriorityQueueComparator<T, Q extends InternalPriorityQueue<T>>
		implements PriorityComparator<Q> {

		/** Comparator for the queue elements, so we can compare their heads. */
		@Nonnull
		private final PriorityComparator<T> elementPriorityComparator;

		InternalPriorityQueueComparator(@Nonnull PriorityComparator<T> elementPriorityComparator) {
			this.elementPriorityComparator = elementPriorityComparator;
		}

		@Override
		public int comparePriority(Q o1, Q o2) {
			final T left = o1.peek();
			final T right = o2.peek();
			if (left == null) {
				return (right == null ? 0 : 1);
			} else {
				return (right == null ? -1 : elementPriorityComparator.comparePriority(left, right));
			}
		}
	}

	/**
	 * Factory that produces the sub-queues that represent the partitions of a {@link KeyGroupPartitionedPriorityQueue}.
	 *
	 * @param <T> type of the elements in the queue set.
	 * @param <PQS> type of the priority queue. Must have set semantics and {@link HeapPriorityQueueElement}.
	 */
	public interface PartitionQueueSetFactory<T, PQS extends InternalPriorityQueue<T> & HeapPriorityQueueElement> {

		/**
		 * Creates a new queue for a given key-group partition.
		 *
		 * @param keyGroupId the key-group of the elements managed by the produced queue.
		 * @param numKeyGroups the total number of key-groups in the job.
		 * @param elementPriorityComparator the comparator that determines the order of managed elements by priority.
		 * @return a new queue for the given key-group.
		 */
		@Nonnull
		PQS create(
			@Nonnegative int keyGroupId,
			@Nonnegative int numKeyGroups,
			@Nonnull KeyExtractorFunction<T> keyExtractorFunction,
			@Nonnull PriorityComparator<T> elementPriorityComparator);
	}
}
