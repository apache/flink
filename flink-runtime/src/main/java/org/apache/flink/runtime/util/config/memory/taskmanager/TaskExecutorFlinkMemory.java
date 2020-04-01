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

package org.apache.flink.runtime.util.config.memory.taskmanager;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.config.memory.FlinkMemory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Flink internal memory components of Task Executor.
 *
 * <p>A TaskExecutor's internal Flink memory consists of the following components.
 * <ul>
 *     <li>Framework Heap Memory</li>
 *     <li>Framework Off-Heap Memory</li>
 *     <li>Task Heap Memory</li>
 *     <li>Task Off-Heap Memory</li>
 *     <li>Network Memory</li>
 *     <li>Managed Memory</li>
 * </ul>
 *
 * <p>The relationships of TaskExecutor Flink memory components are shown below.
 * <pre>
 *               ┌ ─ ─  Total Flink Memory - ─ ─ ┐
 *               |┌ ─ ─ - - - On-Heap - - - ─ ─ ┐|
 *                 ┌───────────────────────────┐
 *               |││   Framework Heap Memory   ││|
 *                 └───────────────────────────┘
 *               │ ┌───────────────────────────┐ │
 *                ||      Task Heap Memory     ││
 *               │ └───────────────────────────┘ │
 *                └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 *               |┌ ─ ─ - - - Off-Heap  - - ─ ─ ┐|
 *                │┌───────────────────────────┐│
 *               │ │ Framework Off-Heap Memory │ │ ─┐
 *                │└───────────────────────────┘│   │
 *               │ ┌───────────────────────────┐ │  │
 *                ││   Task Off-Heap Memory    ││   ┼─ JVM Direct Memory
 *               │ └───────────────────────────┘ │  │
 *                │┌───────────────────────────┐│   │
 *               │ │      Network Memory       │ │ ─┘
 *                │└───────────────────────────┘│
 *               │ ┌───────────────────────────┐ │
 *                |│      Managed Memory       │|
 *               │ └───────────────────────────┘ │
 *                └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 *               └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 * </pre>
 */
public class TaskExecutorFlinkMemory implements FlinkMemory {
	private static final long serialVersionUID = 1L;

	private final MemorySize frameworkHeap;
	private final MemorySize frameworkOffHeap;
	private final MemorySize taskHeap;
	private final MemorySize taskOffHeap;
	private final MemorySize network;
	private final MemorySize managed;

	public TaskExecutorFlinkMemory(
		final MemorySize frameworkHeap,
		final MemorySize frameworkOffHeap,
		final MemorySize taskHeap,
		final MemorySize taskOffHeap,
		final MemorySize network,
		final MemorySize managed) {

		this.frameworkHeap = checkNotNull(frameworkHeap);
		this.frameworkOffHeap = checkNotNull(frameworkOffHeap);
		this.taskHeap = checkNotNull(taskHeap);
		this.taskOffHeap = checkNotNull(taskOffHeap);
		this.network = checkNotNull(network);
		this.managed = checkNotNull(managed);
	}

	public MemorySize getFrameworkHeap() {
		return frameworkHeap;
	}

	public MemorySize getFrameworkOffHeap() {
		return frameworkOffHeap;
	}

	public MemorySize getTaskHeap() {
		return taskHeap;
	}

	public MemorySize getTaskOffHeap() {
		return taskOffHeap;
	}

	public MemorySize getNetwork() {
		return network;
	}

	public MemorySize getManaged() {
		return managed;
	}

	@Override
	public MemorySize getJvmHeapMemorySize() {
		return frameworkHeap.add(taskHeap);
	}

	@Override
	public MemorySize getJvmDirectMemorySize() {
		return frameworkOffHeap.add(taskOffHeap).add(network);
	}

	@Override
	public MemorySize getTotalFlinkMemorySize() {
		return frameworkHeap.add(frameworkOffHeap).add(taskHeap).add(taskOffHeap).add(network).add(managed);
	}
}
