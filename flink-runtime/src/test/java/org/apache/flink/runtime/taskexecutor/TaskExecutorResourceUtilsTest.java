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

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

/** Test suite for {@link TaskExecutorResourceUtils}. */
public class TaskExecutorResourceUtilsTest extends TestLogger {
	@Test
	public void testResourceSpecFromConfig() {
		double cpuCores = 1.0;
		MemorySize taskHeap = MemorySize.ofMebiBytes(1);
		MemorySize taskOffHeap = MemorySize.ofMebiBytes(2);
		MemorySize network = MemorySize.ofMebiBytes(3);
		MemorySize managed = MemorySize.ofMebiBytes(4);

		Configuration configuration = new Configuration();
		configuration.set(TaskManagerOptions.CPU_CORES, cpuCores);
		configuration.set(TaskManagerOptions.TASK_HEAP_MEMORY, taskHeap);
		configuration.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, taskOffHeap);
		configuration.set(TaskManagerOptions.NETWORK_MEMORY_MIN, network);
		configuration.set(TaskManagerOptions.NETWORK_MEMORY_MAX, network);
		configuration.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, managed);

		TaskExecutorResourceSpec resourceSpec = TaskExecutorResourceUtils.resourceSpecFromConfig(configuration);

		assertThat(resourceSpec.getCpuCores(), is(new CPUResource(cpuCores)));
		assertThat(resourceSpec.getTaskHeapSize(), is(taskHeap));
		assertThat(resourceSpec.getTaskOffHeapSize(), is(taskOffHeap));
		assertThat(resourceSpec.getNetworkMemSize(), is(network));
		assertThat(resourceSpec.getManagedMemorySize(), is(managed));
	}

	@Test
	public void testResourceSpecFromConfigFailsIfRequiredOptionIsNotSet() {
		TaskExecutorResourceUtils.CONFIG_OPTIONS.forEach(option -> {
			try {
				TaskExecutorResourceUtils.resourceSpecFromConfig(setAllRequiredOptionsExceptOne(option));
				fail("should fail with " + IllegalConfigurationException.class.getSimpleName());
			} catch (IllegalConfigurationException e) {
				// expected
			}
		});
	}

	private static Configuration setAllRequiredOptionsExceptOne(ConfigOption<?> optionToNotSet) {
		Configuration configuration = new Configuration();
		if (!TaskManagerOptions.CPU_CORES.equals(optionToNotSet)) {
			configuration.set(TaskManagerOptions.CPU_CORES, 1.0);
		}

		//noinspection unchecked
		TaskExecutorResourceUtils.CONFIG_OPTIONS
			.stream()
			.filter(option -> option.equals(TaskManagerOptions.CPU_CORES))
			.filter(option -> option.equals(optionToNotSet))
			.forEach(option -> configuration.set((ConfigOption<MemorySize>) option, MemorySize.ofMebiBytes(1)));

		return configuration;
	}

	@Test
	public void testAdjustForLocalExecution() {
		Configuration configuration = TaskExecutorResourceUtils.adjustForLocalExecution(new Configuration());

		assertThat(configuration.get(TaskManagerOptions.NETWORK_MEMORY_MIN), is(TaskExecutorResourceUtils.DEFAULT_SHUFFLE_MEMORY_SIZE));
		assertThat(configuration.get(TaskManagerOptions.NETWORK_MEMORY_MAX), is(TaskExecutorResourceUtils.DEFAULT_SHUFFLE_MEMORY_SIZE));
		assertThat(configuration.get(TaskManagerOptions.MANAGED_MEMORY_SIZE), is(TaskExecutorResourceUtils.DEFAULT_MANAGED_MEMORY_SIZE));
	}

	@Test
	public void testNetworkMinAdjustForLocalExecutionIfMaxSet() {
		MemorySize networkMemorySize = MemorySize.ofMebiBytes(1);
		Configuration configuration = new Configuration();
		configuration.set(TaskManagerOptions.NETWORK_MEMORY_MAX, networkMemorySize);
		TaskExecutorResourceUtils.adjustForLocalExecution(configuration);

		assertThat(configuration.get(TaskManagerOptions.NETWORK_MEMORY_MIN), is(networkMemorySize));
		assertThat(configuration.get(TaskManagerOptions.NETWORK_MEMORY_MAX), is(networkMemorySize));
	}

	@Test
	public void testNetworkMaxAdjustForLocalExecutionIfMinSet() {
		MemorySize networkMemorySize = MemorySize.ofMebiBytes(1);
		Configuration configuration = new Configuration();
		configuration.set(TaskManagerOptions.NETWORK_MEMORY_MIN, networkMemorySize);
		TaskExecutorResourceUtils.adjustForLocalExecution(configuration);

		assertThat(configuration.get(TaskManagerOptions.NETWORK_MEMORY_MIN), is(networkMemorySize));
		assertThat(configuration.get(TaskManagerOptions.NETWORK_MEMORY_MAX), is(networkMemorySize));
	}
}
