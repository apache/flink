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

package org.apache.flink.contrib.streaming.state;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Iterator;
import java.util.TreeSet;

/**
 * Implementation of a lexicographically ordered set of byte-arrays, based on a {@link TreeSet}.
 */
public class TreeOrderedSetCache implements RocksDBCachingPriorityQueueSet.OrderedByteArraySetCache {

	/** Maximum capacity. */
	private final int maxSize;

	@Nonnull
	private final TreeSet<byte[]> treeSet;

	TreeOrderedSetCache(int maxSize) {
		this.maxSize = maxSize;
		this.treeSet = new TreeSet<>(LEXICOGRAPHIC_BYTE_COMPARATOR);
	}

	@Override
	public int size() {
		return treeSet.size();
	}

	@Override
	public int maxSize() {
		return maxSize;
	}

	@Override
	public boolean isEmpty() {
		return treeSet.isEmpty();
	}

	@Override
	public boolean isFull() {
		return treeSet.size() >= maxSize;
	}

	@Override
	public boolean add(@Nonnull byte[] toAdd) {
		return treeSet.add(toAdd);
	}

	@Override
	public boolean remove(@Nonnull byte[] toRemove) {
		return treeSet.remove(toRemove);
	}

	@Nullable
	@Override
	public byte[] peekFirst() {
		return !isEmpty() ? treeSet.first() : null;
	}

	@Nullable
	@Override
	public byte[] peekLast() {
		return !isEmpty() ? treeSet.last() : null;
	}

	@Nullable
	@Override
	public byte[] pollFirst() {
		return !isEmpty() ? treeSet.pollFirst() : null;
	}

	@Nullable
	@Override
	public byte[] pollLast() {
		return !isEmpty() ? treeSet.pollLast() : null;
	}

	@Override
	public void bulkLoadFromOrderedIterator(@Nonnull Iterator<byte[]> orderedIterator) {
		treeSet.clear();
		for (int i = maxSize; --i >= 0 && orderedIterator.hasNext(); ) {
			treeSet.add(orderedIterator.next());
		}
	}
}
