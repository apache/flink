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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateSnapshot;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.apache.flink.util.CollectionUtil.MAX_ARRAY_SIZE;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A heap-based priority queue for internal timers. This heap is supported by hash sets for fast contains
 * (de-duplication) and deletes. The heap implementation is a simple binary tree stored inside an array. Element indexes
 * in the heap array start at 1 instead of 0 to make array index computations a bit simpler in the hot methods.
 *
 * <p>Possible future improvements:
 * <ul>
 *  <li>We could also implement shrinking for the heap and the deduplication maps.</li>
 *  <li>We could replace the deduplication maps with more efficient custom implementations. In particular, a hash set
 * would be enough if it could return existing elements on unsuccessful adding, etc..</li>
 * </ul>
 *
 * @param <K> type of the key of the internal timers managed by this priority queue.
 * @param <N> type of the namespace of the internal timers managed by this priority queue.
 */
public class InternalTimerHeap<K, N> implements Iterable<InternalTimer<K, N>> {

	/**
	 * Comparator for {@link TimerHeapInternalTimer}, based on the timestamp in ascending order.
	 */
	private static final Comparator<TimerHeapInternalTimer<?, ?>> COMPARATOR =
		(o1, o2) -> Long.compare(o1.getTimestamp(), o2.getTimestamp());

	/**
	 * This array contains one hash set per key-group. The sets are used for fast de-duplication and deletes of timers.
	 */
	private final HashMap<TimerHeapInternalTimer<K, N>, TimerHeapInternalTimer<K, N>>[] deduplicationMapsByKeyGroup;

	/**
	 * The array that represents the heap-organized priority queue.
	 */
	private TimerHeapInternalTimer<K, N>[] queue;

	/**
	 * The current size of the priority queue.
	 */
	private int size;

	/**
	 * The key-group range of timers that are managed by this queue.
	 */
	private final KeyGroupRange keyGroupRange;

	/**
	 * The total number of key-groups of the job.
	 */
	private final int totalNumberOfKeyGroups;


	/**
	 * Creates an empty {@link InternalTimerHeap} with the requested initial capacity.
	 *
	 * @param minimumCapacity the minimum and initial capacity of this priority queue.
	 */
	@SuppressWarnings("unchecked")
	InternalTimerHeap(
		@Nonnegative int minimumCapacity,
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnegative int totalNumberOfKeyGroups) {

		this.totalNumberOfKeyGroups = totalNumberOfKeyGroups;
		this.keyGroupRange = keyGroupRange;

		final int keyGroupsInLocalRange = keyGroupRange.getNumberOfKeyGroups();
		final int deduplicationSetSize = 1 + minimumCapacity / keyGroupsInLocalRange;
		this.deduplicationMapsByKeyGroup = new HashMap[keyGroupsInLocalRange];
		for (int i = 0; i < keyGroupsInLocalRange; ++i) {
			deduplicationMapsByKeyGroup[i] = new HashMap<>(deduplicationSetSize);
		}

		this.queue = new TimerHeapInternalTimer[1 + minimumCapacity];
	}

	@Nullable
	public InternalTimer<K, N> poll() {
		return size() > 0 ? removeElementAtIndex(1) : null;
	}

	@Nullable
	public InternalTimer<K, N> peek() {
		return size() > 0 ? queue[1] : null;
	}

	/**
	 * Adds a new timer with the given timestamp, key, and namespace to the heap, if an identical timer was not yet
	 * registered.
	 *
	 * @param timestamp the timer timestamp.
	 * @param key the timer key.
	 * @param namespace the timer namespace.
	 * @return true iff a new timer with given timestamp, key, and namespace was added to the heap.
	 */
	public boolean scheduleTimer(long timestamp, @Nonnull K key, @Nonnull N namespace) {
		return addInternal(new TimerHeapInternalTimer<>(timestamp, key, namespace));
	}

	/**
	 * Stops timer with the given timestamp, key, and namespace by removing it from the heap, if it exists on the heap.
	 *
	 * @param timestamp the timer timestamp.
	 * @param key the timer key.
	 * @param namespace the timer namespace.
	 * @return true iff a timer with given timestamp, key, and namespace was found and removed from the heap.
	 */
	public boolean stopTimer(long timestamp, @Nonnull K key, @Nonnull N namespace) {
		return removeInternal(new TimerHeapInternalTimer<>(timestamp, key, namespace));
	}

	public boolean isEmpty() {
		return size() == 0;
	}

	@Nonnegative
	public int size() {
		return size;
	}

	public void clear() {
		Arrays.fill(queue, null);
		for (HashMap<TimerHeapInternalTimer<K, N>, TimerHeapInternalTimer<K, N>> timerHashMap :
			deduplicationMapsByKeyGroup) {
			timerHashMap.clear();
		}
		size = 0;
	}

	@SuppressWarnings({"unchecked"})
	@Nonnull
	public InternalTimer<K, N>[] toArray() {
		return (InternalTimer<K, N>[]) Arrays.copyOfRange(queue, 1, size + 1, queue.getClass());
	}

	/**
	 * Returns an iterator over the elements in this queue. The iterator
	 * does not return the elements in any particular order.
	 *
	 * @return an iterator over the elements in this queue.
	 */
	@Nonnull
	public Iterator<InternalTimer<K, N>> iterator() {
		return new InternalTimerPriorityQueueIterator();
	}

	/**
	 * This method adds all the given timers to the heap.
	 */
	void bulkAddRestoredTimers(Collection<? extends InternalTimer<K, N>> restoredTimers) {

		if (restoredTimers == null) {
			return;
		}

		resizeForBulkLoad(restoredTimers.size());

		for (InternalTimer<K, N> timer : restoredTimers) {
			if (timer instanceof TimerHeapInternalTimer) {
				addInternal((TimerHeapInternalTimer<K, N>) timer);
			} else {
				scheduleTimer(timer.getTimestamp(), timer.getKey(), timer.getNamespace());
			}
		}
	}

	/**
	 * Returns an unmodifiable set of all timers in the given key-group.
	 */
	@Nonnull
	Set<InternalTimer<K, N>> getTimersForKeyGroup(@Nonnegative int keyGroupIdx) {
		return Collections.unmodifiableSet(getDedupMapForKeyGroup(keyGroupIdx).keySet());
	}

	@VisibleForTesting
	@SuppressWarnings("unchecked")
	@Nonnull
	List<Set<InternalTimer<K, N>>> getTimersByKeyGroup() {
		List<Set<InternalTimer<K, N>>> result = new ArrayList<>(deduplicationMapsByKeyGroup.length);
		for (int i = 0; i < deduplicationMapsByKeyGroup.length; ++i) {
			result.add(i, Collections.unmodifiableSet(deduplicationMapsByKeyGroup[i].keySet()));
		}
		return result;
	}

	@Nonnull
	StateSnapshot snapshot(TimerHeapInternalTimer.TimerSerializer<K, N> serializer) {
		return new InternalTimerHeapSnapshot<>(
			Arrays.copyOfRange(queue, 1, size + 1),
			serializer,
			keyGroupRange,
			totalNumberOfKeyGroups);
	}

	private boolean addInternal(TimerHeapInternalTimer<K, N> timer) {

		if (getDedupMapForTimer(timer).putIfAbsent(timer, timer) == null) {
			final int newSize = increaseSizeByOne();
			moveElementToIdx(timer, newSize);
			siftUp(newSize);
			return true;
		} else {
			return false;
		}
	}

	private boolean removeInternal(TimerHeapInternalTimer<K, N> timerToRemove) {

		TimerHeapInternalTimer<K, N> storedTimer = getDedupMapForTimer(timerToRemove).remove(timerToRemove);

		if (storedTimer != null) {
			removeElementAtIndex(storedTimer.getTimerHeapIndex());
			return true;
		}

		return false;
	}

	private TimerHeapInternalTimer<K, N> removeElementAtIndex(int removeIdx) {
		TimerHeapInternalTimer<K, N>[] heap = this.queue;
		TimerHeapInternalTimer<K, N> removedValue = heap[removeIdx];

		assert removedValue.getTimerHeapIndex() == removeIdx;

		final int oldSize = size;

		if (removeIdx != oldSize) {
			TimerHeapInternalTimer<K, N> timer = heap[oldSize];
			moveElementToIdx(timer, removeIdx);
			siftDown(removeIdx);
			if (heap[removeIdx] == timer) {
				siftUp(removeIdx);
			}
		}

		heap[oldSize] = null;
		getDedupMapForTimer(removedValue).remove(removedValue);

		--size;
		return removedValue;
	}

	private void siftUp(int idx) {
		final TimerHeapInternalTimer<K, N>[] heap = this.queue;
		final TimerHeapInternalTimer<K, N> currentTimer = heap[idx];
		int parentIdx = idx >>> 1;

		while (parentIdx > 0 && isTimerLessThen(currentTimer, heap[parentIdx])) {
			moveElementToIdx(heap[parentIdx], idx);
			idx = parentIdx;
			parentIdx >>>= 1;
		}

		moveElementToIdx(currentTimer, idx);
	}

	private void siftDown(int idx) {
		final TimerHeapInternalTimer<K, N>[] heap = this.queue;
		final int heapSize = this.size;

		final TimerHeapInternalTimer<K, N> currentTimer = heap[idx];
		int firstChildIdx = idx << 1;
		int secondChildIdx = firstChildIdx + 1;

		if (isTimerIndexValid(secondChildIdx, heapSize) &&
			isTimerLessThen(heap[secondChildIdx], heap[firstChildIdx])) {
			firstChildIdx = secondChildIdx;
		}

		while (isTimerIndexValid(firstChildIdx, heapSize) &&
			isTimerLessThen(heap[firstChildIdx], currentTimer)) {
			moveElementToIdx(heap[firstChildIdx], idx);
			idx = firstChildIdx;
			firstChildIdx = idx << 1;
			secondChildIdx = firstChildIdx + 1;

			if (isTimerIndexValid(secondChildIdx, heapSize) &&
				isTimerLessThen(heap[secondChildIdx], heap[firstChildIdx])) {
				firstChildIdx = secondChildIdx;
			}
		}

		moveElementToIdx(currentTimer, idx);
	}

	private HashMap<TimerHeapInternalTimer<K, N>, TimerHeapInternalTimer<K, N>> getDedupMapForKeyGroup(
		@Nonnegative int keyGroupIdx) {
		return deduplicationMapsByKeyGroup[globalKeyGroupToLocalIndex(keyGroupIdx)];
	}

	private boolean isTimerIndexValid(int timerIndex, int heapSize) {
		return timerIndex <= heapSize;
	}

	private boolean isTimerLessThen(TimerHeapInternalTimer<K, N> a, TimerHeapInternalTimer<K, N> b) {
		return COMPARATOR.compare(a, b) < 0;
	}

	private void moveElementToIdx(TimerHeapInternalTimer<K, N> element, int idx) {
		queue[idx] = element;
		element.setTimerHeapIndex(idx);
	}

	private HashMap<TimerHeapInternalTimer<K, N>, TimerHeapInternalTimer<K, N>> getDedupMapForTimer(
		InternalTimer<K, N> timer) {
		int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(timer.getKey(), totalNumberOfKeyGroups);
		return getDedupMapForKeyGroup(keyGroup);
	}

	private int globalKeyGroupToLocalIndex(int keyGroup) {
		checkArgument(keyGroupRange.contains(keyGroup));
		return keyGroup - keyGroupRange.getStartKeyGroup();
	}

	private int increaseSizeByOne() {
		final int oldArraySize = queue.length;
		final int minRequiredNewSize = ++size;
		if (minRequiredNewSize >= oldArraySize) {
			final int grow = (oldArraySize < 64) ? oldArraySize + 2 : oldArraySize >> 1;
			resizeQueueArray(oldArraySize + grow, minRequiredNewSize);
		}
		// TODO implement shrinking as well?
		return minRequiredNewSize;
	}

	private void resizeForBulkLoad(int totalSize) {
		if (totalSize > queue.length) {
			int desiredSize = totalSize + (totalSize >>> 3);
			resizeQueueArray(desiredSize, totalSize);
		}
	}

	private void resizeQueueArray(int desiredSize, int minRequiredSize) {
		if (isValidArraySize(desiredSize)) {
			queue = Arrays.copyOf(queue, desiredSize);
		} else if (isValidArraySize(minRequiredSize)) {
			queue = Arrays.copyOf(queue, MAX_ARRAY_SIZE);
		} else {
			throw new OutOfMemoryError("Required minimum timer heap size " + minRequiredSize +
				" exceeds maximum size of " + MAX_ARRAY_SIZE + ".");
		}
	}

	private static boolean isValidArraySize(int size) {
		return size >= 0 && size <= MAX_ARRAY_SIZE;
	}

	/**
	 * {@link Iterator} implementation for {@link InternalTimerPriorityQueueIterator}.
	 * {@link Iterator#remove()} is not supported.
	 */
	private class InternalTimerPriorityQueueIterator implements Iterator<InternalTimer<K, N>> {

		private int iterationIdx;

		InternalTimerPriorityQueueIterator() {
			this.iterationIdx = 0;
		}

		@Override
		public boolean hasNext() {
			return iterationIdx < size;
		}

		@Override
		public InternalTimer<K, N> next() {
			if (iterationIdx >= size) {
				throw new NoSuchElementException("Iterator has no next element.");
			}
			return queue[++iterationIdx];
		}
	}
}
