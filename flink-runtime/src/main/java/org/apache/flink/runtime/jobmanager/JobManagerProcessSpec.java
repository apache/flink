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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.util.ProcessMemoryUtils.MemoryProcessSpec;

/**
 * Describe the specifics of different resource dimensions of the JobManager process.
 *
 * <p>A JobManager's memory consists of the following components:
 * <ul>
 *     <li>JVM Heap Memory</li>
 *     <li>Off-heap Memory</li>
 *     <li>JVM Metaspace</li>
 *     <li>JVM Overhead</li>
 * </ul>
 * We use Total Process Memory to refer to all the memory components, while Total Flink Memory refering to all
 * the components except JVM Metaspace and JVM Overhead.
 *
 * <p>The relationships of JobManager memory components are shown below.
 * <pre>
 *               ┌ ─ ─ Total Process Memory  ─ ─ ┐
 *                ┌ ─ ─ Total Flink Memory  ─ ─ ┐
 *               │ ┌───────────────────────────┐ │
 *  On-Heap ----- ││      JVM Heap Memory      ││
 *               │ └───────────────────────────┘ │
 *               │ ┌───────────────────────────┐ │
 *            ┌─  ││       Off-heap Memory     ││
 *            │  │ └───────────────────────────┘ │
 *            │   └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 *            │  │┌─────────────────────────────┐│
 *  Off-Heap ─|   │        JVM Metaspace        │
 *            │  │└─────────────────────────────┘│
 *            │   ┌─────────────────────────────┐
 *            └─ ││        JVM Overhead         ││
 *                └─────────────────────────────┘
 *               └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
 * </pre>
 */
public class JobManagerProcessSpec implements MemoryProcessSpec {
	private static final long serialVersionUID = -1L;

	private final MemorySize jvmHeapSize;

	private final MemorySize offHeapMemorySize;

	private final MemorySize jvmMetaspaceSize;

	private final MemorySize jvmOverheadSize;

	JobManagerProcessSpec(
			MemorySize jvmHeapSize,
			MemorySize offHeapMemorySize,
			MemorySize jvmMetaspaceSize,
			MemorySize jvmOverheadSize) {
		this.jvmHeapSize = jvmHeapSize;
		this.offHeapMemorySize = offHeapMemorySize;
		this.jvmMetaspaceSize = jvmMetaspaceSize;
		this.jvmOverheadSize = jvmOverheadSize;
	}

	@Override
	public MemorySize getJvmHeapMemorySize() {
		return jvmHeapSize;
	}

	@Override
	public MemorySize getJvmDirectMemorySize() {
		return offHeapMemorySize;
	}

	@Override
	public MemorySize getJvmMetaspaceSize() {
		return jvmMetaspaceSize;
	}

	@Override
	public MemorySize getJvmOverheadSize() {
		return jvmOverheadSize;
	}

	@Override
	public MemorySize getTotalFlinkMemorySize() {
		return jvmHeapSize.add(offHeapMemorySize);
	}

	@Override
	public MemorySize getTotalProcessMemorySize() {
		return getTotalFlinkMemorySize().add(jvmMetaspaceSize).add(jvmOverheadSize);
	}
}
