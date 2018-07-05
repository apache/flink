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
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Comparator;

/**
 * This implementation of {@link InternalPriorityQueue} is internally partitioned into sub-queues per key-group and
 * essentially works as a heap-of-heaps. Instances will have set semantics for elements if the sub-queues have set
 * semantics.
 *
 * @param <T> the type of elements in the queue.
 * @param <PQ> type type of sub-queue used for each key-group partition.
 */
public class KeyGroupPartitionedPriorityQueue<T, PQ extends InternalPriorityQueue<T> & HeapPriorityQueueElement>
	implements InternalPriorityQueue<T> {

	/** A heap of heap sets. Each sub-heap represents the partition for a key-group.*/
	@Nonnull
	private final HeapPriorityQueue<PQ> heapOfkeyGroupedHeaps;

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
		@Nonnull Comparator<T> elementComparator,
		@Nonnull PartitionQueueSetFactory<T, PQ> orderedCacheFactory,
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnegative int totalKeyGroups) {

		this.keyExtractor = keyExtractor;
		this.totalKeyGroups = totalKeyGroups;
		this.firstKeyGroup = keyGroupRange.getStartKeyGroup();
		this.keyGroupedHeaps = (PQ[]) new InternalPriorityQueue[keyGroupRange.getNumberOfKeyGroups()];
		this.heapOfkeyGroupedHeaps = new HeapPriorityQueue<>(
			new InternalPriorityQueueComparator<>(elementComparator),
			keyGroupRange.getNumberOfKeyGroups());
		for (int i = 0; i < keyGroupedHeaps.length; i++) {
			final PQ keyGroupSubHeap =
				orderedCacheFactory.create(firstKeyGroup + i, totalKeyGroups, elementComparator);
			keyGroupedHeaps[i] = keyGroupSubHeap;
			heapOfkeyGroupedHeaps.add(keyGroupSubHeap);
		}
	}

	@Nullable
	@Override
	public T poll() {
		final PQ headList = heapOfkeyGroupedHeaps.peek();
		final T head = headList.poll();
		heapOfkeyGroupedHeaps.adjustModifiedElement(headList);
		return head;
	}

	@Nullable
	@Override
	public T peek() {
		return heapOfkeyGroupedHeaps.peek().peek();
	}

	@Override
	public boolean add(@Nonnull T toAdd) {
		final PQ list = getKeyGroupSubHeapForElement(toAdd);

		// the branch checks if the head element has (potentially) changed.
		if (list.add(toAdd)) {
			heapOfkeyGroupedHeaps.adjustModifiedElement(list);
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
			heapOfkeyGroupedHeaps.adjustModifiedElement(list);
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
		return keyGroupId - firstKeyGroup;
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
		implements Comparator<Q> {

		/** Comparator for the queue elements, so we can compare their heads. */
		@Nonnull
		private final Comparator<T> elementComparator;

		InternalPriorityQueueComparator(@Nonnull Comparator<T> elementComparator) {
			this.elementComparator = elementComparator;
		}

		@Override
		public int compare(Q o1, Q o2) {
			final T left = o1.peek();
			final T right = o2.peek();
			if (left == null) {
				return (right == null ? 0 : 1);
			} else {
				return (right == null ? -1 : elementComparator.compare(left, right));
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
		 * @param elementComparator the comparator that determines the order of the managed elements.
		 * @return a new queue for the given key-group.
		 */
		@Nonnull
		PQS create(@Nonnegative int keyGroupId, @Nonnegative int numKeyGroups, @Nonnull Comparator<T> elementComparator);
	}
}
