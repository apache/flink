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
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;

/**
 * This class is an implementation of a {@link InternalPriorityQueue} with set semantics that internally consists of
 * two different storage types. The first storage is a (potentially slow) ordered set store manages the ground truth
 * about the elements in this queue. The second storage is a (fast) ordered set cache, typically with some limited
 * capacity. The cache is used to improve performance of accesses to the underlying store and contains contains an
 * ordered (partial) view on the top elements in the ordered store. We are currently applying simple write-trough
 * to keep cache and store in sync on updates and refill the cache from the store when it is empty and we expect more
 * elements contained in the store.
 *
 * @param <E> the type if the managed elements.
 */
public class CachingInternalPriorityQueueSet<E> implements InternalPriorityQueue<E>, HeapPriorityQueueElement {

	/** A ordered set cache that contains a (partial) view on the top elements in the ordered store. */
	@Nonnull
	private final OrderedSetCache<E> orderedCache;

	/** A store with ordered set semantics that contains the ground truth of all inserted elements. */
	@Nonnull
	private final OrderedSetStore<E> orderedStore;

	/** This flag is true if there could be elements in the backend that are not in the cache (false positives ok). */
	private boolean storeOnlyElements;

	/** Management data for the {@link HeapPriorityQueueElement} trait. */
	private int pqManagedIndex;

	@SuppressWarnings("unchecked")
	public CachingInternalPriorityQueueSet(
		@Nonnull OrderedSetCache<E> orderedCache,
		@Nonnull OrderedSetStore<E> orderedStore) {
		this.orderedCache = orderedCache;
		this.orderedStore = orderedStore;
		// We are careful and set this to true. Could be set to false if we know for sure that the store is empty, but
		// checking this could be an expensive operation.
		this.storeOnlyElements = true;
		this.pqManagedIndex = HeapPriorityQueueElement.NOT_CONTAINED;
	}

	@Nullable
	@Override
	public E peek() {

		checkRefillCacheFromStore();

		return orderedCache.peekFirst();
	}

	@Nullable
	@Override
	public E poll() {

		checkRefillCacheFromStore();

		final E first = orderedCache.removeFirst();

		if (first != null) {
			// write-through sync
			orderedStore.remove(first);
		}

		return first;
	}

	@Override
	public boolean add(@Nonnull E toAdd) {

		checkRefillCacheFromStore();

		// write-through sync
		orderedStore.add(toAdd);

		final boolean cacheFull = orderedCache.isFull();

		if ((!cacheFull && !storeOnlyElements) || orderedCache.isInLowerBound(toAdd)) {

			if (cacheFull) {
				// we drop the element with lowest priority from the cache
				orderedCache.removeLast();
				// the dropped element is now only in the store
				storeOnlyElements = true;
			}

			orderedCache.add(toAdd);
			return toAdd.equals(orderedCache.peekFirst());
		} else {
			// we only added to the store
			storeOnlyElements = true;
			return false;
		}
	}

	@Override
	public boolean remove(@Nonnull E toRemove) {

		checkRefillCacheFromStore();

		boolean newHead = toRemove.equals(orderedCache.peekFirst());
		// write-through sync
		orderedStore.remove(toRemove);
		orderedCache.remove(toRemove);
		return newHead;
	}

	@Override
	public void addAll(@Nullable Collection<? extends E> toAdd) {

		if (toAdd == null) {
			return;
		}

		for (E element : toAdd) {
			add(element);
		}
	}

	@Override
	public int size() {
		return orderedStore.size();
	}

	@Override
	public boolean isEmpty() {
		checkRefillCacheFromStore();
		return orderedCache.isEmpty();
	}

	@Nonnull
	@Override
	public CloseableIterator<E> iterator() {
		return orderedStore.orderedIterator();
	}

	@Override
	public int getInternalIndex() {
		return pqManagedIndex;
	}

	@Override
	public void setInternalIndex(int updateIndex) {
		this.pqManagedIndex = updateIndex;
	}

	/**
	 * Refills the cache from the store when the cache is empty and we expect more elements in the store.
	 *
	 * TODO: We can think about exploiting the property that the store is already sorted when bulk-filling the cache.
	 */
	private void checkRefillCacheFromStore() {
		if (storeOnlyElements && orderedCache.isEmpty()) {
			try (final CloseableIterator<E> iterator = orderedStore.orderedIterator()) {
				while (iterator.hasNext() && !orderedCache.isFull()) {
					orderedCache.add(iterator.next());
				}
				storeOnlyElements = iterator.hasNext();
			} catch (Exception e) {
				throw new FlinkRuntimeException("Exception while closing RocksDB iterator.", e);
			}
		}
	}

	/**
	 * Interface for an ordered cache with set semantics of elements to be used in
	 * {@link CachingInternalPriorityQueueSet}. Cache implementations typically have a limited capacity as indicated
	 * via the {@link #isFull()} method.
	 *
	 * @param <E> the type of contained elements.
	 */
	public interface OrderedSetCache<E> {

		/**
		 * Adds the given element to the cache (if not yet contained). This method should only be called if the cache
		 * is not full.
		 * @param element element to add to the cache.
		 */
		void add(@Nonnull E element);

		/**
		 * Removes the given element from the cache (if contained).
		 * @param element element to remove from the cache.
		 */
		void remove(@Nonnull E element);

		/**
		 * Returns <code>true</> if the cache is full and no more elements can be added.
		 */
		boolean isFull();

		/**
		 * Returns <code>true</> if the cache is empty, i.e. contains ne elements.
		 */
		boolean isEmpty();

		/**
		 * Returns true, if the element is compares smaller than the currently largest element in the cache.
		 */
		boolean isInLowerBound(@Nonnull E toCheck);

		/**
		 * Removes and returns the first (smallest) element from the cache.
		 */
		@Nullable
		E removeFirst();

		/**
		 * Removes and returns the last (larges) element from the cache.
		 */
		@Nullable
		E removeLast();

		/**
		 * Returns the first (smallest) element from the cache (without removing it).
		 */
		@Nullable
		E peekFirst();

		/**
		 * Returns the last (larges) element from the cache (without removing it).
		 */
		@Nullable
		E peekLast();
	}

	/**
	 * Interface for an ordered store with set semantics of elements to be used in
	 * {@link CachingInternalPriorityQueueSet}. Stores are assumed to have (practically) unlimited capacity, but their
	 * operations could all be expensive.
	 *
	 * @param <E> the type of contained elements.
	 */
	public interface OrderedSetStore<E> {

		/**
		 * Adds the given element to the store (if not yet contained).
		 * @param element element to add to the store.
		 */
		void add(@Nonnull E element);

		/**
		 * Removed the given element from the cache (if contained).
		 * @param element element to remove from the cache.
		 */
		void remove(@Nonnull E element);

		/**
		 * Returns the number of elements in the store.
		 */
		@Nonnegative
		int size();

		/**
		 * Returns an iterator over the store that returns element in order. The iterator must be closed by the client
		 * after usage.
		 */
		@Nonnull
		CloseableIterator<E> orderedIterator();
	}
}
