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

package org.apache.flink.runtime.schedule;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.resources.Resource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;

import org.junit.Test;

import java.util.HashMap;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionGraphWithSharingSlots;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * Test the SummationSlotSharingResourceCalculator.
 */
public class SummationSlotSharingResourceCalculatorTest {
	private static final ResourceSpec[] TEST_RESOURCE_SPECS = {
		ResourceSpec
			.newBuilder()
			.setCpuCores(1)
			.setHeapMemoryInMB(100)
			.setDirectMemoryInMB(110)
			.setNativeMemoryInMB(120)
			.setStateSizeInMB(130)
			.setGPUResource(2)
			.addExtendedResource(new CommonExtendedResource("extend", 10))
			.build(),
		ResourceSpec
			.newBuilder()
			.setCpuCores(2)
			.setHeapMemoryInMB(200)
			.setDirectMemoryInMB(210)
			.setNativeMemoryInMB(220)
			.setStateSizeInMB(230)
			.setGPUResource(4)
			.addExtendedResource(new CommonExtendedResource("extend", 20))
			.build(),
		ResourceSpec
			.newBuilder()
			.setCpuCores(3)
			.setHeapMemoryInMB(300)
			.setDirectMemoryInMB(310)
			.setNativeMemoryInMB(320)
			.setStateSizeInMB(330)
			.setGPUResource(6)
			.addExtendedResource(new CommonExtendedResource("extend", 30))
			.build()
	};

	private static final Resource ZERO_MANAGED_MEMORY_RESOURCE = new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, 0);

	@Test
	public void testSingleVertex() throws Exception {
		ExecutionGraph executionGraph = createExecutionGraphWithSharingSlots(
			new Configuration(),
			SchedulingTestUtils.TestGraphManagerPlugin.class.getName(),
			mock(SlotProvider.class),
			new int[]{5},
			new ResourceSpec[]{TEST_RESOURCE_SPECS[0]},
			new DistributionPattern[0]);

		SlotSharingGroup sharingGroup = executionGraph.getVerticesTopologically().iterator().next().getSlotSharingGroup();
		assertNotNull(sharingGroup);

		SummationSlotSharingResourceCalculator computer = new SummationSlotSharingResourceCalculator();
		ResourceProfile profile = computer.calculateSharedGroupResource(sharingGroup, executionGraph);

		ResourceProfile expected = new ResourceProfile(
			TEST_RESOURCE_SPECS[0].getCpuCores(),
			TEST_RESOURCE_SPECS[0].getHeapMemory(),
			TEST_RESOURCE_SPECS[0].getDirectMemory(),
			TEST_RESOURCE_SPECS[0].getNativeMemory(),
			0,
			new HashMap<String, Resource>() {{
				putAll(TEST_RESOURCE_SPECS[0].getExtendedResources());
				put(ResourceSpec.GPU_NAME, new CommonExtendedResource(ResourceSpec.GPU_NAME, TEST_RESOURCE_SPECS[0].getGPUResource()));
				put(ResourceSpec.MANAGED_MEMORY_NAME, ZERO_MANAGED_MEMORY_RESOURCE);
			}});

		assertEquals("Expected to be \n" + expected + "\n, actually it is \n" + profile, 0, expected.compareTo(profile));
	}

	@Test
	public void testMultipleVertices() throws Exception {
		Configuration jobManagerConfiguration = new Configuration();
		jobManagerConfiguration.setInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE, 8);
		jobManagerConfiguration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_SUBPARTITION, 2);
		jobManagerConfiguration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL, 2);

		ExecutionGraph executionGraph = createExecutionGraphWithSharingSlots(
			jobManagerConfiguration,
			SchedulingTestUtils.TestGraphManagerPlugin.class.getName(),
			mock(SlotProvider.class),
			new int[]{5, 5, 2},
			new ResourceSpec[]{TEST_RESOURCE_SPECS[0], TEST_RESOURCE_SPECS[1], TEST_RESOURCE_SPECS[2]},
			new DistributionPattern[]{DistributionPattern.ALL_TO_ALL, DistributionPattern.POINTWISE});

		SlotSharingGroup sharingGroup = executionGraph.getVerticesTopologically().iterator().next().getSlotSharingGroup();
		assertNotNull(sharingGroup);

		SummationSlotSharingResourceCalculator computer = new SummationSlotSharingResourceCalculator();
		ResourceProfile profile = computer.calculateSharedGroupResource(sharingGroup, executionGraph);

		ResourceProfile expected = new ResourceProfile(
			TEST_RESOURCE_SPECS[0].getCpuCores() + TEST_RESOURCE_SPECS[1].getCpuCores() + TEST_RESOURCE_SPECS[2].getCpuCores(),
			TEST_RESOURCE_SPECS[0].getHeapMemory() + TEST_RESOURCE_SPECS[1].getHeapMemory() + TEST_RESOURCE_SPECS[2].getHeapMemory(),
			TEST_RESOURCE_SPECS[0].getDirectMemory() + TEST_RESOURCE_SPECS[1].getDirectMemory() + TEST_RESOURCE_SPECS[2].getDirectMemory(),
			TEST_RESOURCE_SPECS[0].getNativeMemory() + TEST_RESOURCE_SPECS[1].getNativeMemory() + TEST_RESOURCE_SPECS[2].getNativeMemory(),
			3,
			new HashMap<String, Resource>() {{
				put("extend", new CommonExtendedResource("extend", 60));
				put(ResourceSpec.GPU_NAME, new CommonExtendedResource(ResourceSpec.GPU_NAME, 12));
				put(ResourceSpec.MANAGED_MEMORY_NAME, ZERO_MANAGED_MEMORY_RESOURCE);
			}});

		assertEquals("Expected to be \n" + expected + "\n, actually it is \n" + profile, 0, expected.compareTo(profile));
	}

	@Test
	public void testMultipleVerticesWithDifferentNetworkMemory() throws Exception {
		Configuration jobManagerConfiguration = new Configuration();
		jobManagerConfiguration.setInteger(TaskManagerOptions.NETWORK_EXTRA_BUFFERS_PER_GATE, 252);
		jobManagerConfiguration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_SUBPARTITION, 2);
		jobManagerConfiguration.setInteger(TaskManagerOptions.NETWORK_BUFFERS_PER_CHANNEL, 2);

		// For the tasks of the third vertex, the first task has two inputs and its network memory is
		// ceil((252 + 2 * 2) * 32k) = 8M, and the second task has three inputs and its network memory is
		// ceil((252 + 2 * 3) * 32k) = 9M.
		ExecutionGraph executionGraph = createExecutionGraphWithSharingSlots(
			jobManagerConfiguration,
			SchedulingTestUtils.TestGraphManagerPlugin.class.getName(),
			mock(SlotProvider.class),
			new int[]{5, 5, 2},
			new ResourceSpec[]{TEST_RESOURCE_SPECS[0], TEST_RESOURCE_SPECS[1], TEST_RESOURCE_SPECS[2]},
			new DistributionPattern[]{DistributionPattern.ALL_TO_ALL, DistributionPattern.POINTWISE});

		SlotSharingGroup sharingGroup = executionGraph.getVerticesTopologically().iterator().next().getSlotSharingGroup();
		assertNotNull(sharingGroup);

		SummationSlotSharingResourceCalculator computer = new SummationSlotSharingResourceCalculator();
		ResourceProfile profile = computer.calculateSharedGroupResource(sharingGroup, executionGraph);

		ResourceProfile expected = new ResourceProfile(
			TEST_RESOURCE_SPECS[0].getCpuCores() + TEST_RESOURCE_SPECS[1].getCpuCores() + TEST_RESOURCE_SPECS[2].getCpuCores(),
			TEST_RESOURCE_SPECS[0].getHeapMemory() + TEST_RESOURCE_SPECS[1].getHeapMemory() + TEST_RESOURCE_SPECS[2].getHeapMemory(),
			TEST_RESOURCE_SPECS[0].getDirectMemory() + TEST_RESOURCE_SPECS[1].getDirectMemory() + TEST_RESOURCE_SPECS[2].getDirectMemory(),
			TEST_RESOURCE_SPECS[0].getNativeMemory() + TEST_RESOURCE_SPECS[1].getNativeMemory() + TEST_RESOURCE_SPECS[2].getNativeMemory(),
			35,
			new HashMap<String, Resource>() {{
				put("extend", new CommonExtendedResource("extend", 60));
				put(ResourceSpec.GPU_NAME, new CommonExtendedResource(ResourceSpec.GPU_NAME, 12));
				put(ResourceSpec.MANAGED_MEMORY_NAME, ZERO_MANAGED_MEMORY_RESOURCE);
			}});

		assertEquals("Expected to be \n" + expected + "\n, actually it is \n" + profile, 0, expected.compareTo(profile));
	}

	@Test
	public void testMultipleVerticesWithUnknownResource() throws Exception {
		ExecutionGraph executionGraph = createExecutionGraphWithSharingSlots(
			new Configuration(),
			SchedulingTestUtils.TestGraphManagerPlugin.class.getName(),
			mock(SlotProvider.class),
			new int[]{5, 5, 2},
			new ResourceSpec[]{ResourceSpec.DEFAULT, ResourceSpec.DEFAULT, ResourceSpec.DEFAULT},
			new DistributionPattern[]{DistributionPattern.ALL_TO_ALL, DistributionPattern.POINTWISE});

		SlotSharingGroup sharingGroup = executionGraph.getVerticesTopologically().iterator().next().getSlotSharingGroup();
		assertNotNull(sharingGroup);

		SummationSlotSharingResourceCalculator computer = new SummationSlotSharingResourceCalculator();
		ResourceProfile profile = computer.calculateSharedGroupResource(sharingGroup, executionGraph);

		assertEquals(profile, ResourceProfile.UNKNOWN);
	}
}
