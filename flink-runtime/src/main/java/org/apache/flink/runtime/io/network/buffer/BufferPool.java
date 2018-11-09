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

package org.apache.flink.runtime.io.network.buffer;

import java.io.IOException;

/**
 * A dynamically sized buffer pool.
 */
public interface BufferPool extends BufferProvider, BufferRecycler {

	/**
	 * Destroys this buffer pool.
	 *
	 * <p>If not all buffers are available, they are recycled lazily as soon as they are recycled.
	 */
	void lazyDestroy();

	/**
	 * Checks whether this buffer pool has been destroyed.
	 */
	@Override
	boolean isDestroyed();

	/**
	 * Returns the number of guaranteed (minimum number of) memory segments of this buffer pool.
	 */
	int getNumberOfRequiredMemorySegments();

	/**
	 * Returns the maximum number of memory segments this buffer pool should use.
	 *
	 * @return maximum number of memory segments to use or <tt>-1</tt> if unlimited
	 */
	int getMaxNumberOfMemorySegments();

	/**
	 * Returns the current size of this buffer pool.
	 *
	 * <p>The size of the buffer pool can change dynamically at runtime.
	 */
	int getNumBuffers();

	/**
	 * Sets the current size of this buffer pool.
	 *
	 * <p>The size needs to be greater or equal to the guaranteed number of memory segments.
	 */
	void setNumBuffers(int numBuffers) throws IOException;

	/**
	 * Returns the number memory segments, which are currently held by this buffer pool.
	 */
	int getNumberOfAvailableMemorySegments();

	/**
	 * Returns the number of used buffers of this buffer pool.
	 */
	int bestEffortGetNumOfUsedBuffers();
}
