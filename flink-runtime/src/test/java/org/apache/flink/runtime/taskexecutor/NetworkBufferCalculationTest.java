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

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.taskmanager.NetworkEnvironmentConfiguration;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.net.InetAddress;

import static org.apache.flink.util.MathUtils.checkedDownCast;
import static org.junit.Assert.assertEquals;

/**
 * Tests the network buffer calculation from heap size.
 */
public class NetworkBufferCalculationTest extends TestLogger {

	/**
	 * Test for {@link TaskManagerServices#calculateNetworkBufferMemory(TaskManagerServicesConfiguration, long)}
	 * using the same (manual) test cases as in {@link TaskManagerServicesTest#calculateHeapSizeMB()}.
	 */
	@Test
	public void calculateNetworkBufFromHeapSize() throws Exception {
		TaskManagerServicesConfiguration tmConfig;

		tmConfig = getTmConfig(Long.valueOf(TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue()),
			TaskManagerOptions.MANAGED_MEMORY_FRACTION.defaultValue(),
			0.1f, 60L << 20, 1L << 30, MemoryType.HEAP);
		assertEquals((100L << 20) + 1 /* one too many due to floating point imprecision */,
			TaskManagerServices.calculateNetworkBufferMemory(tmConfig, 900L << 20)); // 900MB

		tmConfig = getTmConfig(Long.valueOf(TaskManagerOptions.MANAGED_MEMORY_SIZE.defaultValue()),
			TaskManagerOptions.MANAGED_MEMORY_FRACTION.defaultValue(),
			0.2f, 60L << 20, 1L << 30, MemoryType.HEAP);
		assertEquals((200L << 20) + 3 /* slightly too many due to floating point imprecision */,
			TaskManagerServices.calculateNetworkBufferMemory(tmConfig, 800L << 20)); // 800MB

		tmConfig = getTmConfig(10, TaskManagerOptions.MANAGED_MEMORY_FRACTION.defaultValue(),
			0.1f, 60L << 20, 1L << 30, MemoryType.OFF_HEAP);
		assertEquals((100L << 20) + 1 /* one too many due to floating point imprecision */,
			TaskManagerServices.calculateNetworkBufferMemory(tmConfig, 890L << 20)); // 890MB

		tmConfig = getTmConfig(-1, 0.1f,
			0.1f, 60L << 20, 1L << 30, MemoryType.OFF_HEAP);
		assertEquals((100L << 20) + 1 /* one too many due to floating point imprecision */,
			TaskManagerServices.calculateNetworkBufferMemory(tmConfig, 810L << 20)); // 810MB
	}

	/**
	 * Returns a task manager services configuration for the tests
	 *
	 * @param managedMemory         see {@link TaskManagerOptions#MANAGED_MEMORY_SIZE}
	 * @param managedMemoryFraction see {@link TaskManagerOptions#MANAGED_MEMORY_FRACTION}
	 * @param networkBufFraction	see {@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_FRACTION}
	 * @param networkBufMin			see {@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MIN}
	 * @param networkBufMax			see {@link TaskManagerOptions#NETWORK_BUFFERS_MEMORY_MAX}
	 * @param memType				on-heap or off-heap
	 *
	 * @return configuration object
	 */
	private static TaskManagerServicesConfiguration getTmConfig(
		final long managedMemory, final float managedMemoryFraction, float networkBufFraction,
		long networkBufMin, long networkBufMax,
		final MemoryType memType) {

		final NetworkEnvironmentConfiguration networkConfig = new NetworkEnvironmentConfiguration(
			networkBufFraction,
			networkBufMin,
			networkBufMax,
			checkedDownCast(MemorySize.parse(TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue()).getBytes()),
			null,
			TaskManagerOptions.NETWORK_REQUEST_BACKOFF_INITIAL.defaultValue(),
			TaskManagerOptions.NETWORK_REQUEST_BACKOFF_MAX.defaultValue(),
			TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL.defaultValue(),
			TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE.defaultValue(),
			null);

		return new TaskManagerServicesConfiguration(
			InetAddress.getLoopbackAddress(),
			new String[] {},
			new String[] {},
			false,
			networkConfig,
			QueryableStateConfiguration.disabled(),
			1,
			managedMemory,
			memType,
			false,
			managedMemoryFraction,
			0);
	}
}
