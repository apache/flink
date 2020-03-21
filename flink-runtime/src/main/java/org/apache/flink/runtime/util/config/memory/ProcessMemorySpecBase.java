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

package org.apache.flink.runtime.util.config.memory;

import org.apache.flink.configuration.MemorySize;

/**
 * Process memory components.
 */
public class ProcessMemorySpecBase<IM extends FlinkMemory> implements ProcessMemorySpec {
	private static final long serialVersionUID = 1L;

	private final IM flinkMemory;
	private final JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead;

	protected ProcessMemorySpecBase(IM flinkMemory, JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead) {
		this.flinkMemory = flinkMemory;
		this.jvmMetaspaceAndOverhead = jvmMetaspaceAndOverhead;
	}

	public IM getFlinkMemory() {
		return flinkMemory;
	}

	public JvmMetaspaceAndOverhead getJvmMetaspaceAndOverhead() {
		return jvmMetaspaceAndOverhead;
	}

	@Override
	public MemorySize getJvmHeapMemorySize() {
		return flinkMemory.getJvmHeapMemorySize();
	}

	@Override
	public MemorySize getJvmDirectMemorySize() {
		return flinkMemory.getJvmDirectMemorySize();
	}

	public MemorySize getJvmMetaspaceSize() {
		return getJvmMetaspaceAndOverhead().getMetaspace();
	}

	public MemorySize getJvmOverheadSize() {
		return getJvmMetaspaceAndOverhead().getOverhead();
	}

	@Override
	public MemorySize getTotalFlinkMemorySize() {
		return flinkMemory.getTotalFlinkMemorySize();
	}

	public MemorySize getTotalProcessMemorySize() {
		return flinkMemory.getTotalFlinkMemorySize().add(getJvmMetaspaceSize()).add(getJvmOverheadSize());
	}
}
