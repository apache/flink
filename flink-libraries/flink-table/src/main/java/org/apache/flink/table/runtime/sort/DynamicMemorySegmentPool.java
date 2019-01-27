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

package org.apache.flink.table.runtime.sort;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.table.runtime.util.MemorySegmentPool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * MemorySegment pool that can allocate segments from the floating memory pool.
 */
public class DynamicMemorySegmentPool implements MemorySegmentPool {

	private static final Logger LOG = LoggerFactory.getLogger(DynamicMemorySegmentPool.class);

	private final List<MemorySegment> segments;
	private final Object owner;
	private final int pageSize;
	private final MemoryManager memoryManager;
	private final int perRequestNumBuffers;
	private final int additionalLimitNumPages;
	private int allocateFloatingNum;

	public DynamicMemorySegmentPool(
			final MemoryManager memoryManager, final List<MemorySegment>
			memorySegments, int perRequestNumBuffers, int additionalLimitNumPages) {
		this.memoryManager = memoryManager;
		this.perRequestNumBuffers = perRequestNumBuffers;
		this.additionalLimitNumPages = additionalLimitNumPages;
		this.segments = memorySegments;
		this.pageSize = segments.get(0).size();
		this.owner = segments.get(0).getOwner();
	}

	@Override
	public MemorySegment nextSegment() {
		if (this.segments.size() > 0) {
			return this.segments.remove(this.segments.size() - 1);
		} else if (allocateFloatingNum < additionalLimitNumPages) {
			int requestNum = Math.min(perRequestNumBuffers, additionalLimitNumPages - allocateFloatingNum);
			try {
				List<MemorySegment> allocates = memoryManager.allocatePages(owner, requestNum, false);
				this.segments.addAll(allocates);
				allocateFloatingNum += allocates.size();
				allocates.clear();
				LOG.info("{} allocate {} floating segments successfully!", owner, requestNum);
			} catch (MemoryAllocationException e) {
				LOG.warn("DynamicMemorySegmentPool can't allocate {} floating pages, use {} floating pages now",
						requestNum, perRequestNumBuffers + allocateFloatingNum, e);
				return null;
			}
			if (this.segments.size() > 0) {
				return this.segments.remove(this.segments.size() - 1);
			} else {
				return null;
			}
		} else {
			return null;
		}
	}

	public int resetAndReturnFloatingNum() {
		int old = allocateFloatingNum;
		allocateFloatingNum = 0;
		return old;
	}

	@Override
	public int pageSize() {
		return pageSize;
	}

	@Override
	public void returnAll(List<MemorySegment> memory) {
		segments.addAll(memory);
	}

	@Override
	public void clear() {
		segments.clear();
	}
}
