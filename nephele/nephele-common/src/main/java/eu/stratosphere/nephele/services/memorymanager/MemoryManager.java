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

import java.io.IOException;
import java.util.Collection;

public interface MemoryManager {
	/**
	 * Tries to allocate a memory segment of the specified size.
	 * 
	 * @param <T>
	 * @param segmentSize
	 *        the size of the memory to be allocated
	 * @return the allocated memory segment
	 * @throws MemoryAllocationException
	 *         if not enough memory is available
	 * @throws IllegalArgumentException
	 *         if the size parameter is not a positive
	 *         integer.
	 */
	MemorySegment allocate(int segmentSize) throws MemoryAllocationException;

	/**
	 * Tries to release the memory for the specified segment. If the <code>segment</code> has already been released or
	 * is <code>null</code>,
	 * the request is simply ignored. If the segment is not from the expected
	 * MemorySegment implementation type, an <code>IllegalArgumentException</code> is thrown.
	 * 
	 * @param segment
	 * @throws IllegalArgumentException
	 */
	void release(MemorySegment segment);

	/**
	 * Tries to allocate a collection of <code>numberOfBuffers</code> memory
	 * segments of the specified <code>segmentSize</code> and <code>segmentType</code>.
	 * 
	 * @param <T>
	 * @param numberOfSegments
	 * @param segmentSize
	 *        the size of the memory to be allocated
	 * @param segmentType
	 *        the segment type wrapping the underlying memory
	 * @return a collection of allocated memory segments
	 * @throws IOException
	 */
	Collection<MemorySegment> allocate(int numberOfSegments, int segmentSize) throws MemoryAllocationException;

	/**
	 * Tries to release the memory for the specified collection of segments.
	 * 
	 * @param <T>
	 * @param segments
	 * @throws NullPointerException
	 * @throws IllegalArgumentException
	 */
	<T extends MemorySegment> void release(Collection<T> segments);

	/**
	 * Shutdown method.
	 */
	void shutdown();
}
