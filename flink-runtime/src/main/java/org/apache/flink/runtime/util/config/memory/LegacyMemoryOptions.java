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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;

/**
 * Legacy JVM heap/process memory options.
 *
 * <p>If these memory options are set, they are interpreted as other new memory options for the backwards compatibility
 * in {@link MemoryBackwardsCompatibilityUtils}.
 */
public class LegacyMemoryOptions {
	private final String envVar;
	private final ConfigOption<MemorySize> heap;
	private final ConfigOption<Integer> heapMb;

	public LegacyMemoryOptions(String envVar, ConfigOption<MemorySize> heap, ConfigOption<Integer> heapMb) {
		this.envVar = envVar;
		this.heap = heap;
		this.heapMb = heapMb;
	}

	String getEnvVar() {
		return envVar;
	}

	ConfigOption<MemorySize> getHeap() {
		return heap;
	}

	ConfigOption<Integer> getHeapMb() {
		return heapMb;
	}
}
