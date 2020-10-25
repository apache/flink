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

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.util.config.memory.CommonProcessMemorySpec;
import org.apache.flink.runtime.util.config.memory.JvmMetaspaceAndOverhead;
import org.apache.flink.runtime.util.config.memory.JvmMetaspaceAndOverheadOptions;
import org.apache.flink.runtime.util.config.memory.LegacyMemoryOptions;
import org.apache.flink.runtime.util.config.memory.MemoryBackwardsCompatibilityUtils;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryOptions;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.runtime.util.config.memory.taskmanager.TaskExecutorFlinkMemory;
import org.apache.flink.runtime.util.config.memory.taskmanager.TaskExecutorFlinkMemoryUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for TaskExecutor memory configurations.
 *
 * <p>See {@link TaskExecutorProcessSpec} for details about memory components of TaskExecutor and their relationships.
 */
public class TaskExecutorProcessUtils {

	static final ProcessMemoryOptions TM_PROCESS_MEMORY_OPTIONS = new ProcessMemoryOptions(
		Arrays.asList(TaskManagerOptions.TASK_HEAP_MEMORY, TaskManagerOptions.MANAGED_MEMORY_SIZE),
		TaskManagerOptions.TOTAL_FLINK_MEMORY,
		TaskManagerOptions.TOTAL_PROCESS_MEMORY,
		new JvmMetaspaceAndOverheadOptions(
			TaskManagerOptions.JVM_METASPACE,
			TaskManagerOptions.JVM_OVERHEAD_MIN,
			TaskManagerOptions.JVM_OVERHEAD_MAX,
			TaskManagerOptions.JVM_OVERHEAD_FRACTION));

	@SuppressWarnings("deprecation")
	static final LegacyMemoryOptions TM_LEGACY_HEAP_OPTIONS =
		new LegacyMemoryOptions(
			"FLINK_TM_HEAP",
			TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY,
			TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB);

	private static final ProcessMemoryUtils<TaskExecutorFlinkMemory> PROCESS_MEMORY_UTILS = new ProcessMemoryUtils<>(
		TM_PROCESS_MEMORY_OPTIONS,
		new TaskExecutorFlinkMemoryUtils());

	private static final MemoryBackwardsCompatibilityUtils LEGACY_MEMORY_UTILS = new MemoryBackwardsCompatibilityUtils(TM_LEGACY_HEAP_OPTIONS);

	private TaskExecutorProcessUtils() {}

	// ------------------------------------------------------------------------
	//  Generating Dynamic Config Options
	// ------------------------------------------------------------------------

	public static String generateDynamicConfigsStr(final TaskExecutorProcessSpec taskExecutorProcessSpec) {
		final Map<String, String> configs = new HashMap<>();
		configs.put(TaskManagerOptions.CPU_CORES.key(),
			String.valueOf(taskExecutorProcessSpec.getCpuCores().getValue().doubleValue()));
		configs.put(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key(), taskExecutorProcessSpec.getFrameworkHeapSize().getBytes() + "b");
		configs.put(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY.key(), taskExecutorProcessSpec.getFrameworkOffHeapMemorySize().getBytes() + "b");
		configs.put(TaskManagerOptions.TASK_HEAP_MEMORY.key(), taskExecutorProcessSpec.getTaskHeapSize().getBytes() + "b");
		configs.put(TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key(), taskExecutorProcessSpec.getTaskOffHeapSize().getBytes() + "b");
		configs.put(TaskManagerOptions.NETWORK_MEMORY_MIN.key(), taskExecutorProcessSpec.getNetworkMemSize().getBytes() + "b");
		configs.put(TaskManagerOptions.NETWORK_MEMORY_MAX.key(), taskExecutorProcessSpec.getNetworkMemSize().getBytes() + "b");
		configs.put(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), taskExecutorProcessSpec.getManagedMemorySize().getBytes() + "b");
		return assembleDynamicConfigsStr(configs);
	}

	private static String assembleDynamicConfigsStr(final Map<String, String> configs) {
		final StringBuilder sb = new StringBuilder();
		for (Map.Entry<String, String> entry : configs.entrySet()) {
			sb.append("-D ").append(entry.getKey()).append("=").append(entry.getValue()).append(" ");
		}
		return sb.toString();
	}

	// ------------------------------------------------------------------------
	//  Memory Configuration Calculations
	// ------------------------------------------------------------------------

	public static TaskExecutorProcessSpecBuilder newProcessSpecBuilder(final Configuration config) {
		return TaskExecutorProcessSpecBuilder.newBuilder(config);
	}

	public static TaskExecutorProcessSpec processSpecFromConfig(final Configuration config) {
		return createMemoryProcessSpec(config, PROCESS_MEMORY_UTILS.memoryProcessSpecFromConfig(config));
	}

	public static TaskExecutorProcessSpec processSpecFromWorkerResourceSpec(
		final Configuration config, final WorkerResourceSpec workerResourceSpec) {

		final MemorySize frameworkHeapMemorySize = TaskExecutorFlinkMemoryUtils.getFrameworkHeapMemorySize(config);
		final MemorySize frameworkOffHeapMemorySize = TaskExecutorFlinkMemoryUtils.getFrameworkOffHeapMemorySize(config);

		final TaskExecutorFlinkMemory flinkMemory = new TaskExecutorFlinkMemory(
			frameworkHeapMemorySize,
			frameworkOffHeapMemorySize,
			workerResourceSpec.getTaskHeapSize(),
			workerResourceSpec.getTaskOffHeapSize(),
			workerResourceSpec.getNetworkMemSize(),
			workerResourceSpec.getManagedMemSize());

		final JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead =
			PROCESS_MEMORY_UTILS.deriveJvmMetaspaceAndOverheadFromTotalFlinkMemory(
				config, flinkMemory.getTotalFlinkMemorySize());

		return new TaskExecutorProcessSpec(workerResourceSpec.getCpuCores(), flinkMemory, jvmMetaspaceAndOverhead);
	}

	private static TaskExecutorProcessSpec createMemoryProcessSpec(
			final Configuration config,
			final CommonProcessMemorySpec<TaskExecutorFlinkMemory> processMemory) {
		TaskExecutorFlinkMemory flinkMemory = processMemory.getFlinkMemory();
		JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead = processMemory.getJvmMetaspaceAndOverhead();
		return new TaskExecutorProcessSpec(getCpuCores(config), flinkMemory, jvmMetaspaceAndOverhead);
	}

	private static CPUResource getCpuCores(final Configuration config) {
		return getCpuCoresWithFallback(config, -1.0);
	}

	public static double getCpuCoresWithFallbackConfigOption(final Configuration config, ConfigOption<Double> fallbackOption) {
		double fallbackValue = config.getDouble(fallbackOption);
		return getCpuCoresWithFallback(config, fallbackValue).getValue().doubleValue();
	}

	public static CPUResource getCpuCoresWithFallback(final Configuration config, double fallback) {
		final double cpuCores;
		if (config.contains(TaskManagerOptions.CPU_CORES)) {
			cpuCores = config.getDouble(TaskManagerOptions.CPU_CORES);
		} else if (fallback > 0.0) {
			cpuCores = fallback;
		} else {
			cpuCores = config.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);
		}

		if (cpuCores <= 0) {
			throw new IllegalConfigurationException(
				String.format(
					"TaskExecutors need to be started with a positive number of CPU cores. Please configure %s accordingly.",
					TaskManagerOptions.CPU_CORES.key()));
		}

		return new CPUResource(cpuCores);
	}

	public static Configuration getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
			final Configuration configuration,
			final ConfigOption<MemorySize> configOption) {
		return LEGACY_MEMORY_UTILS.getConfWithLegacyHeapSizeMappedToNewConfigOption(configuration, configOption);
	}
}
