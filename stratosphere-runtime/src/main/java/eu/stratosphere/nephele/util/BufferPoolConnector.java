/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.util;

import eu.stratosphere.core.memory.MemorySegment;
import eu.stratosphere.runtime.io.BufferRecycler;

import java.util.Queue;

public class BufferPoolConnector implements BufferRecycler {

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

	@Override
	public void recycle(final MemorySegment buffer) {
		synchronized (this.memoryPool) {
			this.memoryPool.add(buffer);
			this.memoryPool.notify();
		}
	}
}
