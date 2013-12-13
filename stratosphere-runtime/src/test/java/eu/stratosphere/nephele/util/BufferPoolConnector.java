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

package eu.stratosphere.nephele.util;

import java.util.Queue;

import eu.stratosphere.nephele.io.channels.MemoryBufferPoolConnector;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;

/**
 * This is a simple implementation of a {@link MemoryBufferPoolConnector} used for the server unit tests.
 * <p>
 * This class is thread-safe.
 * 
 */
public final class BufferPoolConnector implements MemoryBufferPoolConnector {

	/**
	 * Reference to the memory pool the byte buffer was originally taken from.
	 */
	private final Queue<MemorySegment> memoryPool;

	/**
	 * Constructs a new buffer pool connector
	 * 
	 * @param bufferPool
	 *        a reference to the memory pool the byte buffer was originally taken from
	 */
	public BufferPoolConnector(final Queue<MemorySegment> bufferPool) {
		this.memoryPool = bufferPool;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void recycle(final MemorySegment memSeg) {

		synchronized (this.memoryPool) {
			this.memoryPool.add(memSeg);
			this.memoryPool.notify();
		}
	}
}
