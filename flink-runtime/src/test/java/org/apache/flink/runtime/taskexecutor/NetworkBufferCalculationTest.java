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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests the network buffer calculation from heap size.
 */
public class NetworkBufferCalculationTest extends TestLogger {

	/**
	 * Test for {@link NettyShuffleEnvironmentConfiguration#calculateNewNetworkBufferMemory(Configuration, long)}
	 * using the same (manual) test cases as in {@link NettyShuffleEnvironmentConfigurationTest#calculateHeapSizeMB()}.
	 */
	@Test
	public void calculateNetworkBufFromHeapSize() {
		Configuration config;

		final long networkBufMin = 64L << 20; // 64MB
		final long networkBufMax = 1L << 30; // 1GB

		config = getConfig(
			Long.valueOf(TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue()),
			TaskManagerOptions.MANAGED_MEMORY_FRACTION.defaultValue(),
			0.1f, networkBufMin, networkBufMax, MemoryType.HEAP);
		assertEquals(100L << 20,
			NettyShuffleEnvironmentConfiguration.calculateNewNetworkBufferMemory(config, 900L << 20)); // 900MB

		config = getConfig(
			Long.valueOf(TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue()),
			TaskManagerOptions.MANAGED_MEMORY_FRACTION.defaultValue(),
			0.2f, networkBufMin, networkBufMax, MemoryType.HEAP);
		assertEquals(200L << 20,
			NettyShuffleEnvironmentConfiguration.calculateNewNetworkBufferMemory(config, 800L << 20)); // 800MB

		config = getConfig(
			Long.valueOf(TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue()),
			TaskManagerOptions.MANAGED_MEMORY_FRACTION.defaultValue(),
			0.6f, networkBufMin, networkBufMax, MemoryType.HEAP);
		assertEquals(600L << 20,
			NettyShuffleEnvironmentConfiguration.calculateNewNetworkBufferMemory(config, 400L << 20)); // 400MB

		config = getConfig(10, TaskManagerOptions.MANAGED_MEMORY_FRACTION.defaultValue(),
			0.1f, networkBufMin, networkBufMax, MemoryType.OFF_HEAP);
		assertEquals(100L << 20,
			NettyShuffleEnvironmentConfiguration.calculateNewNetworkBufferMemory(config, 890L << 20)); // 890MB

		config = getConfig(10, TaskManagerOptions.MANAGED_MEMORY_FRACTION.defaultValue(),
				0.6f, networkBufMin, networkBufMax, MemoryType.OFF_HEAP);
		assertEquals(615L << 20,
			NettyShuffleEnvironmentConfiguration.calculateNewNetworkBufferMemory(config, 400L << 20)); // 400MB

		config = getConfig(0, 0.1f, 0.1f, networkBufMin, networkBufMax, MemoryType.OFF_HEAP);
		assertEquals(100L << 20,
			NettyShuffleEnvironmentConfiguration.calculateNewNetworkBufferMemory(config, 810L << 20)); // 810MB
	}

	/**
	 * Returns a configuration for the tests.
	 *
	 * @param managedMemory         see {@link TaskManagerOptions#MANAGED_MEMORY_SIZE}
	 * @param managedMemoryFraction see {@link TaskManagerOptions#MANAGED_MEMORY_FRACTION}
	 * @param networkBufFraction	see {@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_FRACTION}
	 * @param networkBufMin			see {@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_MIN}
	 * @param networkBufMax			see {@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_MEMORY_MAX}
	 * @param memoryType			on-heap or off-heap
	 *
	 * @return configuration object
	 */
	private static Configuration getConfig(
		final long managedMemory,
		final float managedMemoryFraction,
		float networkBufFraction,
		long networkBufMin,
		long networkBufMax,
		MemoryType memoryType) {

		final Configuration configuration = new Configuration();

		configuration.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), managedMemory);
		configuration.setFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION.key(), managedMemoryFraction);
		configuration.setFloat(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION.key(), networkBufFraction);
		configuration.setLong(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN.key(), networkBufMin);
		configuration.setLong(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX.key(), networkBufMax);
		configuration.setBoolean(TaskManagerOptions.MEMORY_OFF_HEAP.key(), memoryType == MemoryType.OFF_HEAP);

		return configuration;
	}
}
