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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.configuration.MemorySize;

import java.io.Serializable;

/**
 * Describe the specifics of different resource dimensions of the TaskExecutor.
 *
 * <p>A TaskExecutor's memory consists of the following components.
 * <ul>
 *     <li>Framework Heap Memory</li>
 *     <li>Task Heap Memory</li>
 *     <li>Task Off-Heap Memory</li>
 *     <li>Shuffle Memory</li>
 *     <li>Managed Memory</li>
 *     <ul>
 *         <li>On-Heap Managed Memory</li>
 *         <li>Off-Heap Managed Memory</li>
 *     </ul>
 *     <li>JVM Metaspace</li>
 *     <li>JVM Overhead</li>
 * </ul>
 * Among all the components, Framework Heap Memory, Task Heap Memory and On-Heap Managed Memory use on heap memory,
 * while the rest use off heap memory. We use Total Process Memory to refer to all the memory components, while Total
 * Flink Memory refering to all the components except JVM Metaspace and JVM Overhead.
 *
 * <p>The relationships of TaskExecutor memory components are shown below.
 * <pre>
 *               ┌ ─ ─ Total Process Memory  ─ ─ ┐
 *                ┌ ─ ─ Total Flink Memory  ─ ─ ┐
 *               │ ┌───────────────────────────┐ │
 *                ││   Framework Heap Memory   ││  ─┐
 *               │ └───────────────────────────┘ │  │
 *                │┌───────────────────────────┐│   │
 *               │ │     Task Heap Memory      │ │ ─┤
 *                │└───────────────────────────┘│   │
 *               │ ┌───────────────────────────┐ │  │
 *            ┌─  ││   Task Off-Heap Memory    ││   │
 *            │  │ └───────────────────────────┘ │  ├─ On-Heap
 *            │   │┌───────────────────────────┐│   │
 *            ├─ │ │      Shuffle Memory       │ │  │
 *            │   │└───────────────────────────┘│   │
 *            │  │ ┌───── Managed Memory ──────┐ │  │
 *            │   ││┌─────────────────────────┐││   │
 *            │  │ ││ On-Heap Managed Memory  ││ │ ─┘
 *            │   ││├─────────────────────────┤││
 *  Off-Heap ─┼─ │ ││ Off-Heap Managed Memory ││ │
 *            │   ││└─────────────────────────┘││
 *            │  │ └───────────────────────────┘ │
 *            │   └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 *            │  │┌─────────────────────────────┐│
 *            ├─  │        JVM Metaspace        │
 *            │  │└─────────────────────────────┘│
 *            │   ┌─────────────────────────────┐
 *            └─ ││        JVM Overhead         ││
 *                └─────────────────────────────┘
 *               └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 * </pre>
 */
public class TaskExecutorResourceSpec implements Serializable {

	private final MemorySize frameworkHeapSize;

	private final MemorySize frameworkOffHeapMemorySize;

	private final MemorySize taskHeapSize;

	private final MemorySize taskOffHeapSize;

	private final MemorySize shuffleMemSize;

	private final MemorySize onHeapManagedMemorySize;

	private final MemorySize offHeapManagedMemorySize;

	private final MemorySize jvmMetaspaceSize;

	private final MemorySize jvmOverheadSize;

	public TaskExecutorResourceSpec(
		MemorySize frameworkHeapSize,
		MemorySize frameworkOffHeapSize,
		MemorySize taskHeapSize,
		MemorySize taskOffHeapSize,
		MemorySize shuffleMemSize,
		MemorySize onHeapManagedMemorySize,
		MemorySize offHeapManagedMemorySize,
		MemorySize jvmMetaspaceSize,
		MemorySize jvmOverheadSize) {

		this.frameworkHeapSize = frameworkHeapSize;
		this.frameworkOffHeapMemorySize = frameworkOffHeapSize;
		this.taskHeapSize = taskHeapSize;
		this.taskOffHeapSize = taskOffHeapSize;
		this.shuffleMemSize = shuffleMemSize;
		this.onHeapManagedMemorySize = onHeapManagedMemorySize;
		this.offHeapManagedMemorySize = offHeapManagedMemorySize;
		this.jvmMetaspaceSize = jvmMetaspaceSize;
		this.jvmOverheadSize = jvmOverheadSize;
	}

	public MemorySize getFrameworkHeapSize() {
		return frameworkHeapSize;
	}

	public MemorySize getFrameworkOffHeapMemorySize() {
		return frameworkOffHeapMemorySize;
	}

	public MemorySize getTaskHeapSize() {
		return taskHeapSize;
	}

	public MemorySize getTaskOffHeapSize() {
		return taskOffHeapSize;
	}

	public MemorySize getShuffleMemSize() {
		return shuffleMemSize;
	}

	public MemorySize getOnHeapManagedMemorySize() {
		return onHeapManagedMemorySize;
	}

	public MemorySize getOffHeapManagedMemorySize() {
		return offHeapManagedMemorySize;
	}

	public MemorySize getManagedMemorySize() {
		return onHeapManagedMemorySize.add(offHeapManagedMemorySize);
	}

	public MemorySize getJvmMetaspaceSize() {
		return jvmMetaspaceSize;
	}

	public MemorySize getJvmOverheadSize() {
		return jvmOverheadSize;
	}

	public MemorySize getTotalFlinkMemorySize() {
		return frameworkHeapSize.add(frameworkOffHeapMemorySize).add(taskHeapSize).add(taskOffHeapSize).add(shuffleMemSize).add(getManagedMemorySize());
	}

	public MemorySize getTotalProcessMemorySize() {
		return getTotalFlinkMemorySize().add(jvmMetaspaceSize).add(jvmOverheadSize);
	}

	public MemorySize getJvmHeapMemorySize() {
		return frameworkHeapSize.add(taskHeapSize).add(onHeapManagedMemorySize);
	}

	public MemorySize getJvmDirectMemorySize() {
		return frameworkOffHeapMemorySize.add(taskOffHeapSize).add(shuffleMemSize);
	}

	@Override
	public String toString() {
		return "TaskExecutorResourceSpec {"
			+ "frameworkHeapSize=" + frameworkHeapSize.toString()
			+ ", frameworkOffHeapSize=" + frameworkOffHeapMemorySize.toString()
			+ ", taskHeapSize=" + taskHeapSize.toString()
			+ ", taskOffHeapSize=" + taskOffHeapSize.toString()
			+ ", shuffleMemSize=" + shuffleMemSize.toString()
			+ ", onHeapManagedMemorySize=" + onHeapManagedMemorySize.toString()
			+ ", offHeapManagedMemorySize=" + offHeapManagedMemorySize.toString()
			+ ", jvmMetaspaceSize=" + jvmMetaspaceSize.toString()
			+ ", jvmOverheadSize=" + jvmOverheadSize.toString()
			+ "}";
	}
}
