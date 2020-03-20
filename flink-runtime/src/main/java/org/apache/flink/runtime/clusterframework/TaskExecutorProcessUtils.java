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
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.ProcessMemoryUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Utility class for TaskExecutor memory configurations.
 *
 * <p>See {@link TaskExecutorProcessSpec} for details about memory components of TaskExecutor and their relationships.
 */
public class TaskExecutorProcessUtils
		extends ProcessMemoryUtils<TaskExecutorProcessSpec, TmFlinkInternalMemory> {
	private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorProcessUtils.class);

	static final JvmMetaspaceAndOverheadOptions TM_JVM_METASPACE_AND_OVERHEAD_OPTIONS =
		new JvmMetaspaceAndOverheadOptions(
			TaskManagerOptions.JVM_METASPACE,
			TaskManagerOptions.JVM_OVERHEAD_MIN,
			TaskManagerOptions.JVM_OVERHEAD_MAX,
			TaskManagerOptions.JVM_OVERHEAD_FRACTION);

	@SuppressWarnings("deprecation")
	static final LegacyHeapOptions TM_LEGACY_HEAP_OPTIONS =
		new LegacyHeapOptions(
			"FLINK_TM_HEAP",
			TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY,
			TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB);

	private static final TaskExecutorProcessUtils INSTANCE = new TaskExecutorProcessUtils();

	private TaskExecutorProcessUtils() {
		super(
			Arrays.asList(TaskManagerOptions.TASK_HEAP_MEMORY, TaskManagerOptions.MANAGED_MEMORY_SIZE),
			TaskManagerOptions.TOTAL_FLINK_MEMORY,
			TaskManagerOptions.TOTAL_PROCESS_MEMORY,
			TM_JVM_METASPACE_AND_OVERHEAD_OPTIONS,
			TM_LEGACY_HEAP_OPTIONS);
	}

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
	//  Generating Slot Resource Profiles
	// ------------------------------------------------------------------------

	public static List<ResourceProfile> createDefaultWorkerSlotProfiles(
			TaskExecutorProcessSpec taskExecutorProcessSpec,
			int numberOfSlots) {
		final ResourceProfile resourceProfile =
			generateDefaultSlotResourceProfile(taskExecutorProcessSpec, numberOfSlots);
		return Collections.nCopies(numberOfSlots, resourceProfile);
	}

	public static ResourceProfile generateDefaultSlotResourceProfile(
			TaskExecutorProcessSpec taskExecutorProcessSpec,
			int numberOfSlots) {
		return ResourceProfile.newBuilder()
			.setCpuCores(taskExecutorProcessSpec.getCpuCores().divide(numberOfSlots))
			.setTaskHeapMemory(taskExecutorProcessSpec.getTaskHeapSize().divide(numberOfSlots))
			.setTaskOffHeapMemory(taskExecutorProcessSpec.getTaskOffHeapSize().divide(numberOfSlots))
			.setManagedMemory(taskExecutorProcessSpec.getManagedMemorySize().divide(numberOfSlots))
			.setNetworkMemory(taskExecutorProcessSpec.getNetworkMemSize().divide(numberOfSlots))
			.build();
	}

	// ------------------------------------------------------------------------
	//  Memory Configuration Calculations
	// ------------------------------------------------------------------------

	public static TaskExecutorProcessSpecBuilder newProcessSpecBuilder(final Configuration config) {
		return TaskExecutorProcessSpecBuilder.newBuilder(config);
	}

	public static TaskExecutorProcessSpec processSpecFromConfig(final Configuration config) {
		return INSTANCE.memoryProcessSpecFromConfig(config);
	}

	@Override
	protected TmFlinkInternalMemory deriveTotalFlinkMemoryFromInternalMemory(Configuration config) {
		final MemorySize taskHeapMemorySize = getTaskHeapMemorySize(config);
		final MemorySize managedMemorySize = getManagedMemorySize(config);

		final MemorySize frameworkHeapMemorySize = getFrameworkHeapMemorySize(config);
		final MemorySize frameworkOffHeapMemorySize = getFrameworkOffHeapMemorySize(config);
		final MemorySize taskOffHeapMemorySize = getTaskOffHeapMemorySize(config);

		final MemorySize networkMemorySize;
		final MemorySize totalFlinkExcludeNetworkMemorySize =
			frameworkHeapMemorySize.add(frameworkOffHeapMemorySize).add(taskHeapMemorySize).add(taskOffHeapMemorySize).add(managedMemorySize);

		if (isTotalFlinkMemorySizeExplicitlyConfigured(config)) {
			// derive network memory from total flink memory, and check against network min/max
			final MemorySize totalFlinkMemorySize = getTotalFlinkMemorySize(config);
			if (totalFlinkExcludeNetworkMemorySize.getBytes() > totalFlinkMemorySize.getBytes()) {
				throw new IllegalConfigurationException(
					"Sum of configured Framework Heap Memory (" + frameworkHeapMemorySize.toHumanReadableString()
						+ "), Framework Off-Heap Memory (" + frameworkOffHeapMemorySize.toHumanReadableString()
						+ "), Task Heap Memory (" + taskHeapMemorySize.toHumanReadableString()
						+ "), Task Off-Heap Memory (" + taskOffHeapMemorySize.toHumanReadableString()
						+ ") and Managed Memory (" + managedMemorySize.toHumanReadableString()
						+ ") exceed configured Total Flink Memory (" + totalFlinkMemorySize.toHumanReadableString() + ").");
			}
			networkMemorySize = totalFlinkMemorySize.subtract(totalFlinkExcludeNetworkMemorySize);
			sanityCheckNetworkMemoryWithExplicitlySetTotalFlinkAndHeapMemory(config, networkMemorySize, totalFlinkMemorySize);
		} else {
			// derive network memory from network configs
			if (isUsingLegacyNetworkConfigs(config)) {
				networkMemorySize = getNetworkMemorySizeWithLegacyConfig(config);
			} else {
				networkMemorySize = deriveNetworkMemoryWithInverseFraction(config, totalFlinkExcludeNetworkMemorySize);
			}
		}

		final TmFlinkInternalMemory flinkInternalMemory = new TmFlinkInternalMemory(
			frameworkHeapMemorySize,
			frameworkOffHeapMemorySize,
			taskHeapMemorySize,
			taskOffHeapMemorySize,
			networkMemorySize,
			managedMemorySize);
		sanityCheckTotalFlinkMemory(config, flinkInternalMemory);

		return flinkInternalMemory;
	}

	@Override
	protected TmFlinkInternalMemory deriveInternalMemoryFromTotalFlinkMemory(
			final Configuration config,
			final MemorySize totalFlinkMemorySize) {
		final MemorySize frameworkHeapMemorySize = getFrameworkHeapMemorySize(config);
		final MemorySize frameworkOffHeapMemorySize = getFrameworkOffHeapMemorySize(config);
		final MemorySize taskOffHeapMemorySize = getTaskOffHeapMemorySize(config);

		final MemorySize taskHeapMemorySize;
		final MemorySize networkMemorySize;
		final MemorySize managedMemorySize;

		if (isTaskHeapMemorySizeExplicitlyConfigured(config)) {
			// task heap memory is configured,
			// derive managed memory first, leave the remaining to network memory and check against network min/max
			taskHeapMemorySize = getTaskHeapMemorySize(config);
			managedMemorySize = deriveManagedMemoryAbsoluteOrWithFraction(config, totalFlinkMemorySize);
			final MemorySize totalFlinkExcludeNetworkMemorySize =
				frameworkHeapMemorySize.add(frameworkOffHeapMemorySize).add(taskHeapMemorySize).add(taskOffHeapMemorySize).add(managedMemorySize);
			if (totalFlinkExcludeNetworkMemorySize.getBytes() > totalFlinkMemorySize.getBytes()) {
				throw new IllegalConfigurationException(
					"Sum of configured Framework Heap Memory (" + frameworkHeapMemorySize.toHumanReadableString()
						+ "), Framework Off-Heap Memory (" + frameworkOffHeapMemorySize.toHumanReadableString()
						+ "), Task Heap Memory (" + taskHeapMemorySize.toHumanReadableString()
						+ "), Task Off-Heap Memory (" + taskOffHeapMemorySize.toHumanReadableString()
						+ ") and Managed Memory (" + managedMemorySize.toHumanReadableString()
						+ ") exceed configured Total Flink Memory (" + totalFlinkMemorySize.toHumanReadableString() + ").");
			}
			networkMemorySize = totalFlinkMemorySize.subtract(totalFlinkExcludeNetworkMemorySize);
			sanityCheckNetworkMemoryWithExplicitlySetTotalFlinkAndHeapMemory(config, networkMemorySize, totalFlinkMemorySize);
		} else {
			// task heap memory is not configured
			// derive managed memory and network memory, leave the remaining to task heap memory
			managedMemorySize = deriveManagedMemoryAbsoluteOrWithFraction(config, totalFlinkMemorySize);

			if (isUsingLegacyNetworkConfigs(config)) {
				networkMemorySize = getNetworkMemorySizeWithLegacyConfig(config);
			} else {
				networkMemorySize = deriveNetworkMemoryWithFraction(config, totalFlinkMemorySize);
			}
			final MemorySize totalFlinkExcludeTaskHeapMemorySize =
				frameworkHeapMemorySize.add(frameworkOffHeapMemorySize).add(taskOffHeapMemorySize).add(managedMemorySize).add(networkMemorySize);
			if (totalFlinkExcludeTaskHeapMemorySize.getBytes() > totalFlinkMemorySize.getBytes()) {
				throw new IllegalConfigurationException(
					"Sum of configured Framework Heap Memory (" + frameworkHeapMemorySize.toHumanReadableString()
						+ "), Framework Off-Heap Memory (" + frameworkOffHeapMemorySize.toHumanReadableString()
						+ "), Task Off-Heap Memory (" + taskOffHeapMemorySize.toHumanReadableString()
						+ "), Managed Memory (" + managedMemorySize.toHumanReadableString()
						+ ") and Network Memory (" + networkMemorySize.toHumanReadableString()
						+ ") exceed configured Total Flink Memory (" + totalFlinkMemorySize.toHumanReadableString() + ").");
			}
			taskHeapMemorySize = totalFlinkMemorySize.subtract(totalFlinkExcludeTaskHeapMemorySize);
		}

		final TmFlinkInternalMemory flinkInternalMemory = new TmFlinkInternalMemory(
			frameworkHeapMemorySize,
			frameworkOffHeapMemorySize,
			taskHeapMemorySize,
			taskOffHeapMemorySize,
			networkMemorySize,
			managedMemorySize);
		sanityCheckTotalFlinkMemory(config, flinkInternalMemory);

		return flinkInternalMemory;
	}

	@Override
	protected TaskExecutorProcessSpec createMemoryProcessSpec(
			final Configuration config,
			final TmFlinkInternalMemory flinkInternalMemory,
			final JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead) {
		return new TaskExecutorProcessSpec(
			getCpuCores(config),
			flinkInternalMemory.getFrameworkHeap(),
			flinkInternalMemory.getFrameworkOffHeap(),
			flinkInternalMemory.getTaskHeap(),
			flinkInternalMemory.getTaskOffHeap(),
			flinkInternalMemory.getNetwork(),
			flinkInternalMemory.getManaged(),
			jvmMetaspaceAndOverhead.getMetaspace(),
			jvmMetaspaceAndOverhead.getOverhead());
	}

	@Override
	protected TaskExecutorProcessSpec failIfMandatoryOptionsNotConfigured() {
		throw new IllegalConfigurationException(String.format(
			"Either Task Heap Memory size (%s) and Managed Memory size (%s), or Total Flink " +
				"Memory size (%s), or Total Process Memory size (%s) need to be configured explicitly.",
			TaskManagerOptions.TASK_HEAP_MEMORY.key(),
			TaskManagerOptions.MANAGED_MEMORY_SIZE.key(),
			TaskManagerOptions.TOTAL_FLINK_MEMORY.key(),
			TaskManagerOptions.TOTAL_PROCESS_MEMORY.key()));
	}

	private static MemorySize deriveManagedMemoryAbsoluteOrWithFraction(final Configuration config, final MemorySize base) {
		if (isManagedMemorySizeExplicitlyConfigured(config)) {
			return getManagedMemorySize(config);
		} else {
			return deriveWithFraction("managed memory", base, getManagedMemoryRangeFraction(config));
		}
	}

	private static MemorySize deriveNetworkMemoryWithFraction(final Configuration config, final MemorySize base) {
		return deriveWithFraction("network memory", base, getNetworkMemoryRangeFraction(config));
	}

	private static MemorySize deriveNetworkMemoryWithInverseFraction(final Configuration config, final MemorySize base) {
		return deriveWithInverseFraction("network memory", base, getNetworkMemoryRangeFraction(config));
	}

	private static MemorySize getFrameworkHeapMemorySize(final Configuration config) {
		return getMemorySizeFromConfig(config, TaskManagerOptions.FRAMEWORK_HEAP_MEMORY);
	}

	private static MemorySize getFrameworkOffHeapMemorySize(final Configuration config) {
		return getMemorySizeFromConfig(config, TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY);
	}

	private static MemorySize getTaskHeapMemorySize(final Configuration config) {
		checkArgument(isTaskHeapMemorySizeExplicitlyConfigured(config));
		return getMemorySizeFromConfig(config, TaskManagerOptions.TASK_HEAP_MEMORY);
	}

	private static MemorySize getTaskOffHeapMemorySize(final Configuration config) {
		return getMemorySizeFromConfig(config, TaskManagerOptions.TASK_OFF_HEAP_MEMORY);
	}

	private static MemorySize getManagedMemorySize(final Configuration config) {
		checkArgument(isManagedMemorySizeExplicitlyConfigured(config));
		return getMemorySizeFromConfig(config, TaskManagerOptions.MANAGED_MEMORY_SIZE);
	}

	private static RangeFraction getManagedMemoryRangeFraction(final Configuration config) {
		return getRangeFraction(MemorySize.ZERO, MemorySize.MAX_VALUE, TaskManagerOptions.MANAGED_MEMORY_FRACTION, config);
	}

	private static MemorySize getNetworkMemorySizeWithLegacyConfig(final Configuration config) {
		checkArgument(isUsingLegacyNetworkConfigs(config));
		@SuppressWarnings("deprecation")
		final long numOfBuffers = config.getInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
		final long pageSize =  ConfigurationParserUtils.getPageSize(config);
		return new MemorySize(numOfBuffers * pageSize);
	}

	private static RangeFraction getNetworkMemoryRangeFraction(final Configuration config) {
		final MemorySize minSize = getMemorySizeFromConfig(config, TaskManagerOptions.NETWORK_MEMORY_MIN);
		final MemorySize maxSize = getMemorySizeFromConfig(config, TaskManagerOptions.NETWORK_MEMORY_MAX);
		return getRangeFraction(minSize, maxSize, TaskManagerOptions.NETWORK_MEMORY_FRACTION, config);
	}

	private static MemorySize getJvmMetaspaceSize(final Configuration config) {
		return getMemorySizeFromConfig(config, TaskManagerOptions.JVM_METASPACE);
	}

	private static MemorySize getTotalFlinkMemorySize(final Configuration config) {
		checkArgument(isTotalFlinkMemorySizeExplicitlyConfigured(config));
		return getMemorySizeFromConfig(config, TaskManagerOptions.TOTAL_FLINK_MEMORY);
	}

	private static MemorySize getTotalProcessMemorySize(final Configuration config) {
		checkArgument(isTotalProcessMemorySizeExplicitlyConfigured(config));
		return getMemorySizeFromConfig(config, TaskManagerOptions.TOTAL_PROCESS_MEMORY);
	}

	private static boolean isTaskHeapMemorySizeExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.TASK_HEAP_MEMORY);
	}

	public static boolean isManagedMemorySizeExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.MANAGED_MEMORY_SIZE);
	}

	private static boolean isUsingLegacyNetworkConfigs(final Configuration config) {
		// use the legacy number-of-buffer config option only when it is explicitly configured and
		// none of new config options is explicitly configured
		@SuppressWarnings("deprecation")
		final boolean legacyConfigured = config.contains(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
		return !config.contains(TaskManagerOptions.NETWORK_MEMORY_MIN) &&
			!config.contains(TaskManagerOptions.NETWORK_MEMORY_MAX) &&
			!config.contains(TaskManagerOptions.NETWORK_MEMORY_FRACTION) &&
			legacyConfigured;
	}

	private static boolean isNetworkMemoryFractionExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.NETWORK_MEMORY_FRACTION);
	}

	public static boolean isNetworkMemoryExplicitlyConfigured(final Configuration config) {
		@SuppressWarnings("deprecation")
		final boolean legacyConfigured = config.contains(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
		return config.contains(TaskManagerOptions.NETWORK_MEMORY_MAX) ||
			config.contains(TaskManagerOptions.NETWORK_MEMORY_MIN) ||
			config.contains(TaskManagerOptions.NETWORK_MEMORY_FRACTION) ||
			legacyConfigured;
	}

	private static boolean isTotalFlinkMemorySizeExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.TOTAL_FLINK_MEMORY);
	}

	private static boolean isTotalProcessMemorySizeExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.TOTAL_PROCESS_MEMORY);
	}

	private static void sanityCheckTotalFlinkMemory(final Configuration config, final TmFlinkInternalMemory flinkInternalMemory) {
		if (isTotalFlinkMemorySizeExplicitlyConfigured(config)) {
			final MemorySize configuredTotalFlinkMemorySize = getTotalFlinkMemorySize(config);
			if (!configuredTotalFlinkMemorySize.equals(flinkInternalMemory.getTotalFlinkMemorySize())) {
				throw new IllegalConfigurationException(
					"Configured/Derived Flink internal memory sizes (total " + flinkInternalMemory.getTotalFlinkMemorySize().toHumanReadableString()
						+ ") do not add up to the configured Total Flink Memory size (" + configuredTotalFlinkMemorySize.toHumanReadableString()
						+ "). Configured/Derived Flink internal memory sizes are: "
						+ "Framework Heap Memory (" + flinkInternalMemory.getFrameworkHeap().toHumanReadableString()
						+ "), Framework Off-Heap Memory (" + flinkInternalMemory.getFrameworkOffHeap().toHumanReadableString()
						+ "), Task Heap Memory (" + flinkInternalMemory.getTaskHeap().toHumanReadableString()
						+ "), Task Off-Heap Memory (" + flinkInternalMemory.getTaskOffHeap().toHumanReadableString()
						+ "), Network Memory (" + flinkInternalMemory.getNetwork().toHumanReadableString()
						+ "), Managed Memory (" + flinkInternalMemory.getManaged().toHumanReadableString() + ").");
			}
		}
	}

	private static void sanityCheckNetworkMemoryWithExplicitlySetTotalFlinkAndHeapMemory(
			final Configuration config,
			final MemorySize derivedNetworkMemorySize,
			final MemorySize totalFlinkMemorySize) {
		try {
			sanityCheckNetworkMemory(config, derivedNetworkMemorySize, totalFlinkMemorySize);
		} catch (IllegalConfigurationException e) {
			throw new IllegalConfigurationException(
				"If Total Flink, Task Heap and (or) Managed Memory sizes are explicitly configured then " +
					"the Network Memory size is the rest of the Total Flink memory after subtracting all other " +
					"configured types of memory, but the derived Network Memory is inconsistent with its configuration.",
				e);
		}
	}

	private static void sanityCheckNetworkMemory(
			final Configuration config,
			final MemorySize derivedNetworkMemorySize,
			final MemorySize totalFlinkMemorySize) {
		if (isUsingLegacyNetworkConfigs(config)) {
			final MemorySize configuredNetworkMemorySize = getNetworkMemorySizeWithLegacyConfig(config);
			if (!configuredNetworkMemorySize.equals(derivedNetworkMemorySize)) {
				throw new IllegalConfigurationException(
					"Derived Network Memory size (" + derivedNetworkMemorySize.toHumanReadableString()
					+ ") does not match configured Network Memory size (" + configuredNetworkMemorySize.toHumanReadableString() + ").");
			}
		} else {
			final RangeFraction networkRangeFraction = getNetworkMemoryRangeFraction(config);
			if (derivedNetworkMemorySize.getBytes() > networkRangeFraction.getMaxSize().getBytes() ||
				derivedNetworkMemorySize.getBytes() < networkRangeFraction.getMinSize().getBytes()) {
				throw new IllegalConfigurationException("Derived Network Memory size ("
					+ derivedNetworkMemorySize.toHumanReadableString() + ") is not in configured Network Memory range ["
					+ networkRangeFraction.getMinSize().toHumanReadableString() + ", "
					+ networkRangeFraction.getMaxSize().toHumanReadableString() + "].");
			}
			if (isNetworkMemoryFractionExplicitlyConfigured(config) &&
				!derivedNetworkMemorySize.equals(totalFlinkMemorySize.multiply(networkRangeFraction.getFraction()))) {
				LOG.info(
					"The derived Network Memory size ({}) does not match " +
						"the configured Network Memory fraction ({}) from the configured Total Flink Memory size ({}). " +
						"The derived Network Memory size will be used.",
					derivedNetworkMemorySize.toHumanReadableString(),
					networkRangeFraction.getFraction(),
					totalFlinkMemorySize.toHumanReadableString());
			}
		}
	}

	private static CPUResource getCpuCores(final Configuration config) {
		return getCpuCoresWithFallback(config, -1.0);
	}

	public static double getCpuCoresWithFallbackConfigOption(final Configuration config, ConfigOption<Double> fallbackOption) {
		double fallbackValue = config.getDouble(fallbackOption);
		return TaskExecutorProcessUtils.getCpuCoresWithFallback(config, fallbackValue).getValue().doubleValue();
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
		return INSTANCE.getConfWithLegacyHeapSizeMappedToNewConfigOption(configuration, configOption);
	}
}
