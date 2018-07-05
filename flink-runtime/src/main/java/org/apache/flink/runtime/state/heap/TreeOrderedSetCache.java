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

import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import it.unimi.dsi.fastutil.objects.ObjectAVLTreeSet;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Comparator;

/**
 * Implementation of {@link org.apache.flink.runtime.state.heap.CachingInternalPriorityQueueSet.OrderedSetCache} based
 * on an AVL-Tree. We chose the implementation from fastutil over JDK for performance reasons.
 *
 * <p>Maintainer notes: We can consider the following potential performance improvements. First, we could introduce a
 * bulk-load method to OrderedSetCache to exploit the fact that adding from an OrderedSetStore is already happening in
 * sorted order, e.g. there are more efficient ways to construct search trees from sorted elements. Second, we could
 * replace the internal AVL-Tree with an extended variant of {@link HeapPriorityQueueSet} that is organized as a
 * Min-Max-Heap.
 *
 * @param <E> type of the contained elements.
 */
public class TreeOrderedSetCache<E> implements CachingInternalPriorityQueueSet.OrderedSetCache<E> {

	/** The tree is used to store cached elements. */
	@Nonnull
	private final ObjectAVLTreeSet<E> avlTree;

	/** The element comparator. */
	@Nonnull
	private final Comparator<E> elementComparator;

	/** The maximum capacity of the cache. */
	@Nonnegative
	private final int capacity;

	/**
	 * Creates a new {@link TreeOrderedSetCache} with the given capacity and element comparator. Capacity must be > 0.
	 * @param elementComparator comparator for the cached elements.
	 * @param capacity the capacity of the cache. Must be > 0.
	 */
	public TreeOrderedSetCache(@Nonnull Comparator<E> elementComparator, @Nonnegative int capacity) {
		Preconditions.checkArgument(capacity > 0, "Cache capacity must be greater than 0.");
		this.avlTree = new ObjectAVLTreeSet<>(elementComparator);
		this.elementComparator = elementComparator;
		this.capacity = capacity;
	}

	@Override
	public void add(@Nonnull E element) {
		assert !isFull();
		avlTree.add(element);
	}

	@Override
	public void remove(@Nonnull E element) {
		avlTree.remove(element);
	}

	@Override
	public boolean isFull() {
		return avlTree.size() == capacity;
	}

	@Override
	public boolean isEmpty() {
		return avlTree.isEmpty();
	}

	@Override
	public boolean isInLowerBound(@Nonnull E toCheck) {
		return avlTree.isEmpty() || elementComparator.compare(peekLast(), toCheck) > 0;
	}

	@Nullable
	@Override
	public E removeFirst() {
		if (avlTree.isEmpty()) {
			return null;
		}
		final E first = avlTree.first();
		avlTree.remove(first);
		return first;
	}

	@Nullable
	@Override
	public E removeLast() {
		if (avlTree.isEmpty()) {
			return null;
		}
		final E last = avlTree.last();
		avlTree.remove(last);
		return last;
	}

	@Nullable
	@Override
	public E peekFirst() {
		return !avlTree.isEmpty() ? avlTree.first() : null;
	}

	@Nullable
	@Override
	public E peekLast() {
		return !avlTree.isEmpty() ? avlTree.last() : null;
	}

	@Nonnull
	@Override
	public CloseableIterator<E> orderedIterator() {
		return CloseableIterator.adapterForIterator(avlTree.iterator());
	}
}
