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
package org.apache.flink.runtime.iterative.task;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryAllocationException;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.sort.UnilateralSortMerger;

import java.util.ArrayList;
import java.util.List;

/**
 * A memory allocator that is used by the sorters so that they can reuse the memory
 * allocated in case of iterative tasks
 */
public class SorterMemoryAllocator {

	private MemoryManager memoryManager;

	// The memory segments that does the sort read
	private List<MemorySegment> sortReadBufferPages;

	// The memory segments that does the write
	private List<MemorySegment> writeBufferPages;

	// The memory segments that processes large records
	private List<MemorySegment> largeBufferPages;

	private int numWriteBuffers;

	private int numLargeBufferHolders;

	// The parent task creating this memoryallocator
	private final AbstractInvokable parentTask;

	// Indicates whether this allocator was created as part of iterative tasks.
	private boolean keepOpenForIterativeTasks;

	public SorterMemoryAllocator(MemoryManager memoryManager, AbstractInvokable parentTask, double relativeMemory, int maxNumFileHandles, boolean handleLargeRecords) throws MemoryAllocationException {
		this(memoryManager, parentTask, relativeMemory, maxNumFileHandles, handleLargeRecords, false);
	}

	public SorterMemoryAllocator(MemoryManager memoryManager, AbstractInvokable parentTask, double relativeMemory, int maxNumFileHandles, boolean handleLargeRecords, boolean keepOpenForIterativeTasks)
		throws MemoryAllocationException {
		this.memoryManager = memoryManager;
		this.parentTask = parentTask;
		sortReadBufferPages = this.memoryManager.allocatePages(parentTask, this.memoryManager
			.computeNumberOfPages(relativeMemory));
		init(maxNumFileHandles, handleLargeRecords, keepOpenForIterativeTasks);
	}

	private void init(int maxNumFileHandles, boolean handleLargeRecords, boolean keepOpenedForIterativeTasks) {
		numWriteBuffers = UnilateralSortMerger.calculateNumOfWriteBuffers(false, handleLargeRecords, maxNumFileHandles, sortReadBufferPages.size());
		if (numWriteBuffers > 0) {
			this.writeBufferPages = new ArrayList<MemorySegment>(numWriteBuffers);
			for (int j = 0; j < numWriteBuffers; j++) {
				this.writeBufferPages.add(this.sortReadBufferPages.remove(this.sortReadBufferPages.size() - 1));
			}
		}
		numLargeBufferHolders = UnilateralSortMerger.calculateNumOfLargeRecords(false, handleLargeRecords, maxNumFileHandles, sortReadBufferPages.size());
		if (numLargeBufferHolders > 0) {
			this.largeBufferPages = new ArrayList<MemorySegment>(numLargeBufferHolders);
			for (int j = 0; j < numLargeBufferHolders; j++) {
				largeBufferPages.add(this.sortReadBufferPages.remove(this.sortReadBufferPages.size() - 1));
			}
		}
		this.keepOpenForIterativeTasks = keepOpenedForIterativeTasks;
	}

	public SorterMemoryAllocator(MemoryManager memoryManager, List<MemorySegment> memorySegments, AbstractInvokable parentTask, int maxNumFileHandles, boolean handleLargeRecords, boolean keepOpenForIterativeTasks) {
		this.memoryManager = memoryManager;
		this.parentTask = parentTask;
		sortReadBufferPages = memorySegments;
		init(maxNumFileHandles, handleLargeRecords, keepOpenForIterativeTasks);
	}

	/**
	 * Allows to put back the sort read buffers back to the original allocated sort read buffers
	 * This ideally happens when some of the sort read buffers were removed from the original list.
	 *
	 * @param memory list of memory segments to be returned back
	 */
	private void putBackSortReadBuffers(List<MemorySegment> memory) {
		if (getSortBufferMemory() != null) {
			this.sortReadBufferPages.addAll(memory);
		}
	}

	/**
	 * Returns list of writeBuffer
	 *
	 * @return list of write buffer pages
	 */
	public List<MemorySegment> getWriteBufferMemory() {
		return writeBufferPages;
	}

	/**
	 * Returns list of sortBuffer
	 *
	 * @return list of sort read buffer pages
	 */
	public List<MemorySegment> getSortBufferMemory() {
		return sortReadBufferPages;
	}

	/**
	 * Returns list of large buffers
	 *
	 * @return list of large buffer pages
	 */
	public List<MemorySegment> getLargeBufferMemory() {
		return largeBufferPages;
	}

	/**
	 * Number of write buffers
	 *
	 * @return number of write buffers
	 */
	public int getNumWriteBuffers() {
		return this.numWriteBuffers;
	}

	/**
	 * Number of large buffers
	 *
	 * @return number of large buffers
	 */
	public int getNumLargeBufferHolder() {
		return this.numLargeBufferHolders;
	}

	/**
	 * The owner who created this allocator
	 *
	 * @return the owner created this allocator
	 */
	public AbstractInvokable getParentTask() {
		return this.parentTask;
	}

	/**
	 * If this allocator has to be retained for iterative tasks
	 *
	 * @return true if the allocator has to be retained for iterative tasks, else false.
	 */
	public boolean isKeepOpenForIterativeTasks() {
		return this.keepOpenForIterativeTasks;
	}

	/**
	 * Close all the created buffers
	 */
	public void close() {
		if (!keepOpenForIterativeTasks) {
			releaseWriteBufferMemory();

			releaseSortBufferMemory();

			releaseLargeBufferMemory();
		}
	}

	public void releaseLargeBufferMemory() {
		if (!keepOpenForIterativeTasks && getLargeBufferMemory() != null) {
			this.memoryManager.release(getLargeBufferMemory());
			getLargeBufferMemory().clear();
		}
	}

	public void releaseSortBufferMemory() {
		if (!keepOpenForIterativeTasks && getSortBufferMemory() != null) {
			this.memoryManager.release(getSortBufferMemory());
			getSortBufferMemory().clear();
		}
	}

	public void releaseWriteBufferMemory() {
		if (!keepOpenForIterativeTasks && getWriteBufferMemory() != null) {
			this.memoryManager.release(getWriteBufferMemory());
			getWriteBufferMemory().clear();
		}
	}

	/**
	 * Releases the given set of memory segments to the sort buffer memory.
	 * If the allocator is for iterative tasks then the segments are put back to
	 * the sort buffer memory, if not it is released back to the memory manager
	 *
	 * @param segments the list of segments to be released
	 */
	public void releaseToSortBufferMemory(List<MemorySegment> segments) {
		if (!keepOpenForIterativeTasks) {
			if (segments != null) {
				this.memoryManager.release(segments);
				segments.clear();
			}
		} else {
			putBackSortReadBuffers(segments);
		}
	}
}
