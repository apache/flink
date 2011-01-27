/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.services.memorymanager;


import java.util.Collection;


/**
 *
 *
 * @author Alexander Alexandrov
 */
public interface MemoryManager {
	/**
	 * Tries to allocate a memory segment of the specified size.
	 * 
	 * @param segmentSize
	 *        The size of the memory to be allocated.
	 * @return The allocated memory segment.
	 * 
	 * @throws MemoryAllocationException Thrown, if not enough memory is available.
	 * @throws IllegalArgumentException Thrown, if the size parameter is not a positive integer.
	 */
	MemorySegment allocate(int segmentSize) throws MemoryAllocationException;

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
	 * Tries to allocate a collection of <code>numberOfBuffers</code> memory
	 * segments of the specified <code>segmentSize</code> and <code>segmentType</code>.
	 * 
	 * @param numberOfSegments The number of segments to allocate.
	 * @param segmentSize The size of the memory segments to be allocated.
	 * @return A collection of allocated memory segments.
	 * 
	 * @throws MemoryAllocationException Thrown, if the memory segments could not be allocated.
	 */
	Collection<MemorySegment> allocate(int numberOfSegments, int segmentSize) throws MemoryAllocationException;

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
	 * Shuts the memory manager down, trying to release all the memory it managed. Depending
	 * on implementation details, the memory does not necessarily become reclaimable by the
	 * garbage collector, because there might still be references to allocated segments in the
	 * code that allocated them from the memory manager. 
	 */
	void shutdown();
}
