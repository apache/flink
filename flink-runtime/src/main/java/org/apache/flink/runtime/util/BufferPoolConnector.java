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


package org.apache.flink.runtime.util;

import java.util.Queue;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.BufferRecycler;

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
