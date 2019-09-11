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

package org.apache.flink.runtime.memory;

import org.apache.flink.core.memory.MemoryType;

import static org.apache.flink.runtime.memory.MemoryManager.DEFAULT_PAGE_SIZE;

/** Builder class for {@link MemoryManager}. */
public class MemoryManagerBuilder {
	private static final int DEFAULT_MEMORY_SIZE = 32 * DEFAULT_PAGE_SIZE;

	private long memorySize = DEFAULT_MEMORY_SIZE;
	private int numberOfSlots = 1;
	private int pageSize = DEFAULT_PAGE_SIZE;
	private boolean preAllocateMemory = true;

	private MemoryManagerBuilder() {

	}

	public MemoryManagerBuilder setMemorySize(long memorySize) {
		this.memorySize = memorySize;
		return this;
	}

	public MemoryManagerBuilder setNumberOfSlots(int numberOfSlots) {
		this.numberOfSlots = numberOfSlots;
		return this;
	}

	public MemoryManagerBuilder setPageSize(int pageSize) {
		this.pageSize = pageSize;
		return this;
	}

	public MemoryManagerBuilder setPreAllocateMemory(boolean preAllocateMemory) {
		this.preAllocateMemory = preAllocateMemory;
		return this;
	}

	public MemoryManager build() {
		return new MemoryManager(memorySize, numberOfSlots, pageSize, MemoryType.HEAP, preAllocateMemory);
	}

	public static MemoryManagerBuilder newBuilder() {
		return new MemoryManagerBuilder();
	}
}
