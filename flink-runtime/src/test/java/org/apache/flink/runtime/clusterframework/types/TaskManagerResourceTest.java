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

package org.apache.flink.runtime.clusterframework.types;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;

/**
 * Test the computation of resources in TaskManagerResource.
 */
public class TaskManagerResourceTest {

	private static final int TM_NETTY_MEMORY_MB = 13;
	private static final int TM_NATIVE_MEMORY_MB = 15;
	private static final int TM_HEAP_MEMORY_MB = 17;
	private static final int NETTY_MEMORY_MB = 13;
	private static final int NUM_SLOTS = 2;

	private static final double CORE = 1.0;
	private static final int HEAP_MEMORY_MB = 10;
	private static final int DIRECT_MEMORY_MB = 100;
	private static final int NATIVE_MEMORY_MB = 1000;

	private static final int DEFAULT_NETWORK_MEMORY_MB = 64;

	@Test
	public void testBasicMemories() {
		final int managedMemoryInMB = 0;
		final int networkMemoryInMB = 0;

		Map<String, Resource> extendedResources = new HashMap<>();
		extendedResources.put(ResourceSpec.MANAGED_MEMORY_NAME,
				new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, managedMemoryInMB));
		final ResourceProfile taskResourceProfile = new ResourceProfile(CORE, HEAP_MEMORY_MB,
			DIRECT_MEMORY_MB, NATIVE_MEMORY_MB, networkMemoryInMB, extendedResources);

		final Configuration config = initializeConfiguration();
		final TaskManagerResource tmResource = TaskManagerResource.fromConfiguration(
			config,
			taskResourceProfile,
			NUM_SLOTS
		);

		assertEquals(TM_HEAP_MEMORY_MB + (HEAP_MEMORY_MB + managedMemoryInMB + networkMemoryInMB) * NUM_SLOTS,
			tmResource.getTotalHeapMemory());
		assertEquals(DIRECT_MEMORY_MB * NUM_SLOTS + TM_NETTY_MEMORY_MB + DEFAULT_NETWORK_MEMORY_MB, tmResource.getTotalDirectMemory());
		assertEquals(TM_NATIVE_MEMORY_MB + TM_HEAP_MEMORY_MB + TM_NETTY_MEMORY_MB + DEFAULT_NETWORK_MEMORY_MB
			+ (HEAP_MEMORY_MB + DIRECT_MEMORY_MB + NATIVE_MEMORY_MB + managedMemoryInMB + networkMemoryInMB) * NUM_SLOTS,
			tmResource.getTotalContainerMemory());
		assertEquals(taskResourceProfile.getManagedMemoryInMB() * NUM_SLOTS, tmResource.getManagedMemorySize());
		assertEquals(DEFAULT_NETWORK_MEMORY_MB, tmResource.getNetworkMemorySize());
	}

	@Test
	public void testAllMemories() {
		final int managedMemoryInMB = 10;
		final int networkMemoryInMB = 13;

		Map<String, Resource> extendedResources = new HashMap<>();
		extendedResources.put(ResourceSpec.MANAGED_MEMORY_NAME,
				new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, managedMemoryInMB));
		ResourceProfile taskResourceProfile = new ResourceProfile(CORE, HEAP_MEMORY_MB,
			DIRECT_MEMORY_MB, NATIVE_MEMORY_MB, networkMemoryInMB, extendedResources);

		final Configuration config = initializeConfiguration();
		config.setBoolean(TaskManagerOptions.MEMORY_OFF_HEAP, false);
		final TaskManagerResource tmResource = TaskManagerResource.fromConfiguration(
			config,
			taskResourceProfile,
			NUM_SLOTS
		);

		assertEquals(TM_HEAP_MEMORY_MB + (HEAP_MEMORY_MB + managedMemoryInMB) * NUM_SLOTS,
			tmResource.getTotalHeapMemory());
		assertEquals((DIRECT_MEMORY_MB + networkMemoryInMB) * NUM_SLOTS + TM_NETTY_MEMORY_MB, tmResource.getTotalDirectMemory());
		assertEquals(TM_NATIVE_MEMORY_MB + TM_HEAP_MEMORY_MB + TM_NETTY_MEMORY_MB
			+ (HEAP_MEMORY_MB + managedMemoryInMB + DIRECT_MEMORY_MB + NATIVE_MEMORY_MB + networkMemoryInMB) * NUM_SLOTS,
			tmResource.getTotalContainerMemory());
		assertEquals(taskResourceProfile.getManagedMemoryInMB() * NUM_SLOTS, tmResource.getManagedMemorySize());
		assertEquals(taskResourceProfile.getNetworkMemoryInMB() * NUM_SLOTS, tmResource.getNetworkMemorySize());

		config.setBoolean(TaskManagerOptions.MEMORY_OFF_HEAP, true);
		final TaskManagerResource tmResourceOffHeap = TaskManagerResource.fromConfiguration(
			config,
			taskResourceProfile,
			NUM_SLOTS
		);

		assertEquals(TM_HEAP_MEMORY_MB + HEAP_MEMORY_MB * NUM_SLOTS,
			tmResourceOffHeap.getTotalHeapMemory());
		assertEquals((DIRECT_MEMORY_MB + networkMemoryInMB + managedMemoryInMB) * NUM_SLOTS + TM_NETTY_MEMORY_MB,
			tmResourceOffHeap.getTotalDirectMemory());
		assertEquals(TM_NATIVE_MEMORY_MB + TM_HEAP_MEMORY_MB + TM_NETTY_MEMORY_MB
			+ (HEAP_MEMORY_MB + DIRECT_MEMORY_MB + NATIVE_MEMORY_MB + managedMemoryInMB + networkMemoryInMB) * NUM_SLOTS,
			tmResourceOffHeap.getTotalContainerMemory());
		assertEquals(taskResourceProfile.getManagedMemoryInMB() * NUM_SLOTS, tmResourceOffHeap.getManagedMemorySize());
		assertEquals(taskResourceProfile.getNetworkMemoryInMB() * NUM_SLOTS, tmResourceOffHeap.getNetworkMemorySize());
	}

	/**
	 * getTotalHeapMemory() will be used in -Xms and -Xmx.
	 * getTotalDirectMemory() will be used in -XX:MaxDirectMemorySize.
	 * Check on-heap and off-heap memory settings.
	 */
	@Test
	public void testSetFloatingManagedMemory() {
		final int managedMemoryInMB = 10;
		final int floatingMemoryInMB = 12;
		final int networkMemoryInMB = 32;

		Map<String, Resource> extendedResourceMap = new HashMap<>();
		extendedResourceMap.put(ResourceSpec.MANAGED_MEMORY_NAME,
				new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, managedMemoryInMB));
		extendedResourceMap.put(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME,
				new CommonExtendedResource(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME, floatingMemoryInMB));
		final ResourceProfile taskResourceProfile = new ResourceProfile(CORE, HEAP_MEMORY_MB,
				DIRECT_MEMORY_MB, NATIVE_MEMORY_MB, networkMemoryInMB, extendedResourceMap);

		// On heap.
		final Configuration config = initializeConfiguration();
		config.setBoolean(TaskManagerOptions.MEMORY_OFF_HEAP, false);
		TaskManagerResource tmResource = TaskManagerResource.fromConfiguration(
				config,
				taskResourceProfile,
				NUM_SLOTS
		);
		Assert.assertEquals(TM_HEAP_MEMORY_MB + (HEAP_MEMORY_MB + managedMemoryInMB + floatingMemoryInMB) * NUM_SLOTS,
				tmResource.getTotalHeapMemory());
		Assert.assertEquals((DIRECT_MEMORY_MB + networkMemoryInMB) * NUM_SLOTS + NETTY_MEMORY_MB, tmResource.getTotalDirectMemory());

		// Off heap.
		config.setBoolean(TaskManagerOptions.MEMORY_OFF_HEAP, true);
		tmResource = TaskManagerResource.fromConfiguration(
				config,
				taskResourceProfile,
				NUM_SLOTS
		);
		Assert.assertEquals(TM_HEAP_MEMORY_MB + HEAP_MEMORY_MB * NUM_SLOTS,
				tmResource.getTotalHeapMemory());
		Assert.assertEquals((DIRECT_MEMORY_MB + networkMemoryInMB + managedMemoryInMB + floatingMemoryInMB) * NUM_SLOTS
				+ NETTY_MEMORY_MB, tmResource.getTotalDirectMemory());
	}

	@Test
	public void testGetYoungHeapMemory() {
		int managedMemMB = 5;
		Map<String, Resource> extendedResourceMap = new HashMap<>();
		extendedResourceMap.put(ResourceSpec.MANAGED_MEMORY_NAME,
			new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, managedMemMB));
		int taskDirectMem = 1, taskNetworkMem = 1;
		final ResourceProfile taskResourceProfile = new ResourceProfile(CORE, HEAP_MEMORY_MB,
			taskDirectMem, 0, taskNetworkMem, extendedResourceMap);
		// pre-allocation is false
		final Configuration config = initializeConfiguration();
		config.setBoolean(TaskManagerOptions.MEMORY_OFF_HEAP, false);
		TaskManagerResource tmResource = TaskManagerResource.fromConfiguration(
			config,
			taskResourceProfile,
			NUM_SLOTS
		);
		Assert.assertEquals((int) (((HEAP_MEMORY_MB + managedMemMB) * NUM_SLOTS + TM_HEAP_MEMORY_MB)
				* TaskManagerOptions.TASK_MANAGER_MEMORY_DYNAMIC_YOUNG_RATIO.defaultValue()),
			tmResource.getYoungHeapMemory());

		// pre-allocation is true
		config.setBoolean(TaskManagerOptions.MANAGED_MEMORY_PRE_ALLOCATE, true);
		tmResource = TaskManagerResource.fromConfiguration(
			config,
			taskResourceProfile,
			NUM_SLOTS
		);
		Assert.assertEquals((int) ((HEAP_MEMORY_MB* NUM_SLOTS + TM_HEAP_MEMORY_MB)
			* TaskManagerOptions.TASK_MANAGER_MEMORY_DYNAMIC_YOUNG_RATIO.defaultValue()),
			tmResource.getYoungHeapMemory());
	}

	private Configuration initializeConfiguration() {
		final Configuration config = new Configuration();
		config.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NATIVE_MEMORY, TM_NATIVE_MEMORY_MB);
		config.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_HEAP_MEMORY, TM_HEAP_MEMORY_MB);
		config.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NETTY_MEMORY, TM_NETTY_MEMORY_MB);
		config.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, DEFAULT_NETWORK_MEMORY_MB << 20);
		return config;
	}
}
