/**
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


package org.apache.flink.runtime.memorymanager;

import java.util.Collection;
import java.util.List;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

/**
 * Interface for a memory manager that assigns portions of memory to different tasks. Each allocated segment of
 * memory is specific to a task. Memory segments can be freed individually, or all memory allocated by a task can 
 * be freed as a whole.
 * <p>
 * Internally, memory is represented as byte arrays. The memory manager acts like a distributer for memory, which
 * means it assigns portions of the arrays to tasks. If memory is released, it means that this part of the memory can
 * be assigned to other tasks.
 */
public interface MemoryManager {
	
	List<MemorySegment> allocatePages(AbstractInvokable owner, int numPages) throws MemoryAllocationException;
	
	void allocatePages(AbstractInvokable owner, List<MemorySegment> target, int numPages) throws MemoryAllocationException;
	
	/**
	 * Tries to release the memory for the specified segment. If the <code>segment</code> has already been released or
	 * is <code>null</code>, the request is simply ignored. If the segment is not from the expected
	 * MemorySegment implementation type, an <code>IllegalArgumentException</code> is thrown.
	 * 
	 * @param segment The segment to be released.
	 * @throws IllegalArgumentException Thrown, if the given segment is of an incompatible type.
	 */
	void release(MemorySegment segment);

	/**
	 * Tries to release the memory for the specified collection of segments.
	 * 
	 * @param <T> The type of memory segment.
	 * @param segments The segments to be released.
	 * @throws NullPointerException Thrown, if the given collection is null.
	 * @throws IllegalArgumentException Thrown, id the segments are of an incompatible type.
	 */
	<T extends MemorySegment> void release(Collection<T> segments);
	
	/**
	 * Releases all memory segments for the given task. 
	 *
	 * @param task The task whose memory segments are to be released.
	 */
	void releaseAll(AbstractInvokable task);
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the size of the pages handled by the memory manager.
	 * 
	 * @return The size of the pages handled by the memory manager.
	 */
	int getPageSize();

	/**
	 * Returns the total size of memory.
	 * 
	 * @return The total size of memory.
	 */
	long getMemorySize();
	
	/**
	 * Computes to how many pages the given number of bytes corresponds. If the given number of bytes is not an
	 * exact multiple of a page size, the result is rounded down, such that a portion of the memory (smaller
	 * than the page size) is not included.
	 * 
	 * @param fraction the fraction of the total memory per slot
	 * @return The number of pages to which 
	 */
	int computeNumberOfPages(double fraction);

	/**
	 * Computes the memory size of the fraction per slot.
	 * 
	 * @param fraction The fraction of the memory of the task slot.
	 * @return The number of pages corresponding to the memory fraction.
	 */
	long computeMemorySize(double fraction);
	
	/**
	 * Rounds the given value down to a multiple of the memory manager's page size.
	 * 
	 * @return The given value, rounded down to a multiple of the page size.
	 */
	long roundDownToPageSizeMultiple(long numBytes);

	// --------------------------------------------------------------------------------------------
	
	/**
	 * Shuts the memory manager down, trying to release all the memory it managed. Depending
	 * on implementation details, the memory does not necessarily become reclaimable by the
	 * garbage collector, because there might still be references to allocated segments in the
	 * code that allocated them from the memory manager.
	 */
	void shutdown();
	
	/**
	 * Checks if the memory manager all memory available and the descriptors of the free segments
	 * describe a contiguous memory layout.
	 * 
	 * @return True, if the memory manager is empty and valid, false if it is not empty or corrupted.
	 */
	boolean verifyEmpty();
}
