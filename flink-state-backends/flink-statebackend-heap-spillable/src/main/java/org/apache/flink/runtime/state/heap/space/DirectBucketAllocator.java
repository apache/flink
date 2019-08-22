/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap.space;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.apache.flink.runtime.state.heap.space.SpaceConstants.NO_SPACE;

/**
 * This allocator is aimed to allocate large space, and reduce fragments.
 */
public class DirectBucketAllocator implements BucketAllocator {

	/**
	 * Capacity of this allocator.
	 */
	private final int capacity;

	/**
	 * Map for used spaces. It maps space offset to space size.
	 */
	private final SortedMap<Integer, Integer> usedSpaces = new TreeMap<>();

	/**
	 * Map for free spaces. It maps space offset to space size.
	 */
	private final SortedMap<Integer, Integer> freeSpaces = new TreeMap<>();

	/**
	 * Offset of space that has never been allocated. Size of this
	 * space is (capacity - offset).
	 */
	private int offset;

	DirectBucketAllocator(int capacity) {
		this.capacity = capacity;
		this.offset = 0;
	}

	@Override
	public synchronized int allocate(int len) {
		// 1. try to find a completely matched space
		int curOffset = findMatchSpace(len);
		if (curOffset != NO_SPACE) {
			return curOffset;
		}

		// 2. if there is no space that has never been allocated,
		// try to compact and find a free space
		if (this.offset + len > capacity) {
			return compactionAndFindFreeSpace(len);
		}

		// 3. allocate space directly
		curOffset = this.offset;
		this.offset += len;
		this.usedSpaces.put(curOffset, len);
		return curOffset;
	}

	@Override
	public synchronized void free(int offset) {
		Integer len = usedSpaces.remove(offset);
		Preconditions.checkNotNull(len, "Try to free an unused space");
		freeSpaces.put(offset, len);
	}

	private int findMatchSpace(int len) {
		Optional<Map.Entry<Integer, Integer>> result =
			this.freeSpaces.entrySet().stream().filter(i -> i.getValue() == len).findFirst();
		if (result.isPresent()) {
			int interChunkOffset = result.get().getKey();
			this.usedSpaces.put(interChunkOffset, this.freeSpaces.remove(interChunkOffset));
			return interChunkOffset;
		}
		return NO_SPACE;
	}

	private int compactionAndFindFreeSpace(int len) {
		// 1. compact space fragments
		Integer lastOffset = -1;
		Integer lastLen = -1;
		Map<Integer, Integer> needMerge = new HashMap<>();
		Iterator<Map.Entry<Integer, Integer>> iterator = freeSpaces.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry<Integer, Integer> freeEntry = iterator.next();
			if (lastOffset != -1 && lastOffset + lastLen == freeEntry.getKey()) {
				// find offset that can be merged
				lastLen = lastLen + freeEntry.getValue();
				needMerge.put(lastOffset, lastLen);
				iterator.remove();
				continue;
			}

			lastOffset = freeEntry.getKey();
			lastLen = freeEntry.getValue();
		}
		needMerge.forEach(this.freeSpaces::put);

		// 2. find max space to allocate
		Optional<Map.Entry<Integer, Integer>> result =
			freeSpaces.entrySet().stream().reduce((x, y) -> x.getValue() > y.getValue() ? x : y);
		if (result.isPresent()) {
			int interChunkOffset = result.get().getKey();
			int interChunkLen = result.get().getValue();
			if (interChunkLen > len) {
				freeSpaces.remove(interChunkOffset);
				usedSpaces.put(interChunkOffset, len);
				freeSpaces.put(interChunkOffset + len, interChunkLen - len);
				return interChunkOffset;
			}
		}

		return NO_SPACE;
	}

	@VisibleForTesting
	int getCapacity() {
		return capacity;
	}

	@VisibleForTesting
	int getOffset() {
		return offset;
	}

	@VisibleForTesting
	SortedMap<Integer, Integer> getUsedSpace() {
		return usedSpaces;
	}

	@VisibleForTesting
	SortedMap<Integer, Integer> getFreeSpaces() {
		return freeSpaces;
	}
}
