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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class for TaskExecutor memory configurations.
 *
 * <p>See {@link TaskExecutorResourceSpec} for details about memory components of TaskExecutor and their relationships.
 */
public class TaskExecutorResourceUtils {
	private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorResourceUtils.class);

	private TaskExecutorResourceUtils() {}

	// ------------------------------------------------------------------------
	//  Generating JVM Parameters
	// ------------------------------------------------------------------------

	public static String generateJvmParametersStr(final TaskExecutorResourceSpec taskExecutorResourceSpec) {
		final MemorySize jvmHeapSize = taskExecutorResourceSpec.getJvmHeapMemorySize();
		final MemorySize jvmDirectSize = taskExecutorResourceSpec.getJvmDirectMemorySize();
		final MemorySize jvmMetaspaceSize = taskExecutorResourceSpec.getJvmMetaspaceSize();

		return "-Xmx" + jvmHeapSize.getBytes()
			+ " -Xms" + jvmHeapSize.getBytes()
			+ " -XX:MaxDirectMemorySize=" + jvmDirectSize.getBytes()
			+ " -XX:MaxMetaspaceSize=" + jvmMetaspaceSize.getBytes();
	}

	// ------------------------------------------------------------------------
	//  Generating Dynamic Config Options
	// ------------------------------------------------------------------------

	public static String generateDynamicConfigsStr(final TaskExecutorResourceSpec taskExecutorResourceSpec) {
		final Map<String, String> configs = new HashMap<>();
		configs.put(TaskManagerOptions.CPU_CORES.key(),
			String.valueOf(taskExecutorResourceSpec.getCpuCores().getValue().doubleValue()));
		configs.put(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key(), taskExecutorResourceSpec.getFrameworkHeapSize().getBytes() + "b");
		configs.put(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY.key(), taskExecutorResourceSpec.getFrameworkOffHeapMemorySize().getBytes() + "b");
		configs.put(TaskManagerOptions.TASK_HEAP_MEMORY.key(), taskExecutorResourceSpec.getTaskHeapSize().getBytes() + "b");
		configs.put(TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key(), taskExecutorResourceSpec.getTaskOffHeapSize().getBytes() + "b");
		configs.put(TaskManagerOptions.NETWORK_MEMORY_MIN.key(), taskExecutorResourceSpec.getNetworkMemSize().getBytes() + "b");
		configs.put(TaskManagerOptions.NETWORK_MEMORY_MAX.key(), taskExecutorResourceSpec.getNetworkMemSize().getBytes() + "b");
		configs.put(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), taskExecutorResourceSpec.getManagedMemorySize().getBytes() + "b");
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
			TaskExecutorResourceSpec taskExecutorResourceSpec,
			int numberOfSlots) {
		final ResourceProfile resourceProfile =
			generateDefaultSlotResourceProfile(taskExecutorResourceSpec, numberOfSlots);
		return Collections.nCopies(numberOfSlots, resourceProfile);
	}

	public static ResourceProfile generateDefaultSlotResourceProfile(
			TaskExecutorResourceSpec taskExecutorResourceSpec,
			int numberOfSlots) {
		return ResourceProfile.newBuilder()
			.setCpuCores(taskExecutorResourceSpec.getCpuCores().divide(numberOfSlots))
			.setTaskHeapMemory(taskExecutorResourceSpec.getTaskHeapSize().divide(numberOfSlots))
			.setTaskOffHeapMemory(taskExecutorResourceSpec.getTaskOffHeapSize().divide(numberOfSlots))
			.setManagedMemory(taskExecutorResourceSpec.getManagedMemorySize().divide(numberOfSlots))
			.setNetworkMemory(taskExecutorResourceSpec.getNetworkMemSize().divide(numberOfSlots))
			.build();
	}

	public static ResourceProfile generateTotalAvailableResourceProfile(TaskExecutorResourceSpec taskExecutorResourceSpec) {
		return ResourceProfile.newBuilder()
			.setCpuCores(taskExecutorResourceSpec.getCpuCores())
			.setTaskHeapMemory(taskExecutorResourceSpec.getTaskHeapSize())
			.setTaskOffHeapMemory(taskExecutorResourceSpec.getTaskOffHeapSize())
			.setManagedMemory(taskExecutorResourceSpec.getManagedMemorySize())
			.setNetworkMemory(taskExecutorResourceSpec.getNetworkMemSize())
			.build();
	}

	// ------------------------------------------------------------------------
	//  Memory Configuration Calculations
	// ------------------------------------------------------------------------

	public static TaskExecutorResourceSpecBuilder newResourceSpecBuilder(final Configuration config) {
		return TaskExecutorResourceSpecBuilder.newBuilder(config);
	}

	public static TaskExecutorResourceSpec resourceSpecFromConfig(final Configuration config) {
		if (isTaskHeapMemorySizeExplicitlyConfigured(config) && isManagedMemorySizeExplicitlyConfigured(config)) {
			// both task heap memory and managed memory are configured, use these to derive total flink memory
			return deriveResourceSpecWithExplicitTaskAndManagedMemory(config);
		} else if (isTotalFlinkMemorySizeExplicitlyConfigured(config)) {
			// either of task heap memory and managed memory is not configured, total flink memory is configured,
			// derive from total flink memory
			return deriveResourceSpecWithTotalFlinkMemory(config);
		} else if (isTotalProcessMemorySizeExplicitlyConfigured(config)) {
			// total flink memory is not configured, total process memory is configured,
			// derive from total process memory
			return deriveResourceSpecWithTotalProcessMemory(config);
		} else {
			throw new IllegalConfigurationException(String.format("Either Task Heap Memory size (%s) and Managed Memory size (%s), or Total Flink"
				+ " Memory size (%s), or Total Process Memory size (%s) need to be configured explicitly.",
				TaskManagerOptions.TASK_HEAP_MEMORY.key(),
				TaskManagerOptions.MANAGED_MEMORY_SIZE.key(),
				TaskManagerOptions.TOTAL_FLINK_MEMORY.key(),
				TaskManagerOptions.TOTAL_PROCESS_MEMORY.key()));
		}
	}

	public static boolean isTaskExecutorResourceExplicitlyConfigured(final Configuration config) {
		return (isTaskHeapMemorySizeExplicitlyConfigured(config) && isManagedMemorySizeExplicitlyConfigured(config))
			|| isTotalFlinkMemorySizeExplicitlyConfigured(config)
			|| isTotalProcessMemorySizeExplicitlyConfigured(config);
	}

	private static TaskExecutorResourceSpec deriveResourceSpecWithExplicitTaskAndManagedMemory(final Configuration config) {
		// derive flink internal memory from explicitly configure task heap memory size and managed memory size

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

		final FlinkInternalMemory flinkInternalMemory = new FlinkInternalMemory(
			frameworkHeapMemorySize,
			frameworkOffHeapMemorySize,
			taskHeapMemorySize,
			taskOffHeapMemorySize,
			networkMemorySize,
			managedMemorySize);
		sanityCheckTotalFlinkMemory(config, flinkInternalMemory);

		// derive jvm metaspace and overhead

		final JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead = deriveJvmMetaspaceAndOverheadFromTotalFlinkMemory(config, flinkInternalMemory.getTotalFlinkMemorySize());

		return createTaskExecutorResourceSpec(config, flinkInternalMemory, jvmMetaspaceAndOverhead);
	}

	private static TaskExecutorResourceSpec deriveResourceSpecWithTotalFlinkMemory(final Configuration config) {
		// derive flink internal memory from explicitly configured total flink memory

		final MemorySize totalFlinkMemorySize = getTotalFlinkMemorySize(config);
		final FlinkInternalMemory flinkInternalMemory = deriveInternalMemoryFromTotalFlinkMemory(config, totalFlinkMemorySize);

		// derive jvm metaspace and overhead

		final JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead = deriveJvmMetaspaceAndOverheadFromTotalFlinkMemory(config, totalFlinkMemorySize);

		return createTaskExecutorResourceSpec(config, flinkInternalMemory, jvmMetaspaceAndOverhead);
	}

	private static TaskExecutorResourceSpec deriveResourceSpecWithTotalProcessMemory(final Configuration config) {
		// derive total flink memory from explicitly configured total process memory size

		final MemorySize totalProcessMemorySize = getTotalProcessMemorySize(config);
		final MemorySize jvmMetaspaceSize = getJvmMetaspaceSize(config);
		final MemorySize jvmOverheadSize = deriveJvmOverheadWithFraction(config, totalProcessMemorySize);
		final JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead = new JvmMetaspaceAndOverhead(jvmMetaspaceSize, jvmOverheadSize);

		if (jvmMetaspaceAndOverhead.getTotalJvmMetaspaceAndOverheadSize().getBytes() > totalProcessMemorySize.getBytes()) {
			throw new IllegalConfigurationException(
				"Sum of configured JVM Metaspace (" + jvmMetaspaceAndOverhead.metaspace.toHumanReadableString()
				+ ") and JVM Overhead (" + jvmMetaspaceAndOverhead.overhead.toHumanReadableString()
				+ ") exceed configured Total Process Memory (" + totalProcessMemorySize.toHumanReadableString() + ").");
		}
		final MemorySize totalFlinkMemorySize = totalProcessMemorySize.subtract(jvmMetaspaceAndOverhead.getTotalJvmMetaspaceAndOverheadSize());

		// derive flink internal memory

		final FlinkInternalMemory flinkInternalMemory = deriveInternalMemoryFromTotalFlinkMemory(config, totalFlinkMemorySize);

		return createTaskExecutorResourceSpec(config, flinkInternalMemory, jvmMetaspaceAndOverhead);
	}

	private static JvmMetaspaceAndOverhead deriveJvmMetaspaceAndOverheadFromTotalFlinkMemory(
			final Configuration config,
			final MemorySize totalFlinkMemorySize) {
		final MemorySize jvmMetaspaceSize = getJvmMetaspaceSize(config);
		final MemorySize totalFlinkAndJvmMetaspaceSize = totalFlinkMemorySize.add(jvmMetaspaceSize);
		final JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead;
		if (isTotalProcessMemorySizeExplicitlyConfigured(config)) {
			final MemorySize totalProcessMemorySize = getTotalProcessMemorySize(config);
			final MemorySize jvmOverheadSize = totalProcessMemorySize.subtract(totalFlinkAndJvmMetaspaceSize);
			sanityCheckJvmOverhead(config, jvmOverheadSize, totalProcessMemorySize);
			jvmMetaspaceAndOverhead = new JvmMetaspaceAndOverhead(jvmMetaspaceSize, jvmOverheadSize);
		} else {
			final MemorySize jvmOverheadSize = deriveJvmOverheadWithInverseFraction(config, totalFlinkAndJvmMetaspaceSize);
			jvmMetaspaceAndOverhead = new JvmMetaspaceAndOverhead(jvmMetaspaceSize, jvmOverheadSize);
			sanityCheckTotalProcessMemory(config, totalFlinkMemorySize, jvmMetaspaceAndOverhead);
		}
		return jvmMetaspaceAndOverhead;
	}

	private static FlinkInternalMemory deriveInternalMemoryFromTotalFlinkMemory(
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

		final FlinkInternalMemory flinkInternalMemory = new FlinkInternalMemory(
			frameworkHeapMemorySize,
			frameworkOffHeapMemorySize,
			taskHeapMemorySize,
			taskOffHeapMemorySize,
			networkMemorySize,
			managedMemorySize);
		sanityCheckTotalFlinkMemory(config, flinkInternalMemory);

		return flinkInternalMemory;
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

	private static MemorySize deriveJvmOverheadWithFraction(final Configuration config, final MemorySize base) {
		return deriveWithFraction("jvm overhead memory", base, getJvmOverheadRangeFraction(config));
	}

	private static MemorySize deriveJvmOverheadWithInverseFraction(final Configuration config, final MemorySize base) {
		return deriveWithInverseFraction("jvm overhead memory", base, getJvmOverheadRangeFraction(config));
	}

	private static MemorySize deriveWithFraction(
			final String memoryDescription,
			final MemorySize base,
			final RangeFraction rangeFraction) {
		final MemorySize relative = base.multiply(rangeFraction.fraction);
		return capToMinMax(memoryDescription, relative, rangeFraction);
	}

	private static MemorySize deriveWithInverseFraction(
			final String memoryDescription,
			final MemorySize base,
			final RangeFraction rangeFraction) {
		checkArgument(rangeFraction.fraction < 1);
		final MemorySize relative = base.multiply(rangeFraction.fraction / (1 - rangeFraction.fraction));
		return capToMinMax(memoryDescription, relative, rangeFraction);
	}

	private static MemorySize capToMinMax(
			final String memoryDescription,
			final MemorySize relative,
			final RangeFraction rangeFraction) {
		long size = relative.getBytes();
		if (size > rangeFraction.maxSize.getBytes()) {
			LOG.info(
				"The derived from fraction {} ({}) is greater than its max value {}, max value will be used instead",
				memoryDescription,
				relative.toHumanReadableString(),
				rangeFraction.maxSize.toHumanReadableString());
			size = rangeFraction.maxSize.getBytes();
		} else if (size < rangeFraction.minSize.getBytes()) {
			LOG.info(
				"The derived from fraction {} ({}) is less than its min value {}, max value will be used instead",
				memoryDescription,
				relative.toHumanReadableString(),
				rangeFraction.minSize.toHumanReadableString());
			size = rangeFraction.minSize.getBytes();
		}
		return new MemorySize(size);
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

	private static RangeFraction getJvmOverheadRangeFraction(final Configuration config) {
		final MemorySize minSize = getMemorySizeFromConfig(config, TaskManagerOptions.JVM_OVERHEAD_MIN);
		final MemorySize maxSize = getMemorySizeFromConfig(config, TaskManagerOptions.JVM_OVERHEAD_MAX);
		return getRangeFraction(minSize, maxSize, TaskManagerOptions.JVM_OVERHEAD_FRACTION, config);
	}

	private static RangeFraction getRangeFraction(
			final MemorySize minSize,
			final MemorySize maxSize,
			ConfigOption<Float> fractionOption,
			final Configuration config) {
		final double fraction = config.getFloat(fractionOption);
		try {
			return new RangeFraction(minSize, maxSize, fraction);
		} catch (IllegalArgumentException e) {
			throw new IllegalConfigurationException(
				String.format(
					"Inconsistently configured %s (%s) and its min (%s), max (%s) value",
					fractionOption,
					fraction,
					minSize.toHumanReadableString(),
					maxSize.toHumanReadableString()),
				e);
		}
	}

	private static MemorySize getTotalFlinkMemorySize(final Configuration config) {
		checkArgument(isTotalFlinkMemorySizeExplicitlyConfigured(config));
		return getMemorySizeFromConfig(config, TaskManagerOptions.TOTAL_FLINK_MEMORY);
	}

	private static MemorySize getTotalProcessMemorySize(final Configuration config) {
		checkArgument(isTotalProcessMemorySizeExplicitlyConfigured(config));
		return getMemorySizeFromConfig(config, TaskManagerOptions.TOTAL_PROCESS_MEMORY);
	}

	private static MemorySize getMemorySizeFromConfig(final Configuration config, final ConfigOption<MemorySize> option) {
		try {
			return config.get(option);
		} catch (Throwable t) {
			throw new IllegalConfigurationException("Cannot read memory size from config option '" + option.key() + "'.", t);
		}
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

	private static boolean isJvmOverheadFractionExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.JVM_OVERHEAD_FRACTION);
	}

	private static boolean isTotalFlinkMemorySizeExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.TOTAL_FLINK_MEMORY);
	}

	private static boolean isTotalProcessMemorySizeExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.TOTAL_PROCESS_MEMORY);
	}

	private static void sanityCheckTotalFlinkMemory(final Configuration config, final FlinkInternalMemory flinkInternalMemory) {
		if (isTotalFlinkMemorySizeExplicitlyConfigured(config)) {
			final MemorySize configuredTotalFlinkMemorySize = getTotalFlinkMemorySize(config);
			if (!configuredTotalFlinkMemorySize.equals(flinkInternalMemory.getTotalFlinkMemorySize())) {
				throw new IllegalConfigurationException(
					"Configured/Derived Flink internal memory sizes (total " + flinkInternalMemory.getTotalFlinkMemorySize().toHumanReadableString()
						+ ") do not add up to the configured Total Flink Memory size (" + configuredTotalFlinkMemorySize.toHumanReadableString()
						+ "). Configured/Derived Flink internal memory sizes are: "
						+ "Framework Heap Memory (" + flinkInternalMemory.frameworkHeap.toHumanReadableString()
						+ "), Framework Off-Heap Memory (" + flinkInternalMemory.frameworkOffHeap.toHumanReadableString()
						+ "), Task Heap Memory (" + flinkInternalMemory.taskHeap.toHumanReadableString()
						+ "), Task Off-Heap Memory (" + flinkInternalMemory.taskOffHeap.toHumanReadableString()
						+ "), Network Memory (" + flinkInternalMemory.network.toHumanReadableString()
						+ "), Managed Memory (" + flinkInternalMemory.managed.toHumanReadableString() + ").");
			}
		}
	}

	private static void sanityCheckTotalProcessMemory(
			final Configuration config,
			final MemorySize totalFlinkMemory,
			final JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead) {
		final MemorySize derivedTotalProcessMemorySize =
			totalFlinkMemory.add(jvmMetaspaceAndOverhead.metaspace).add(jvmMetaspaceAndOverhead.overhead);
		if (isTotalProcessMemorySizeExplicitlyConfigured(config)) {
			final MemorySize configuredTotalProcessMemorySize = getTotalProcessMemorySize(config);
			if (!configuredTotalProcessMemorySize.equals(derivedTotalProcessMemorySize)) {
				throw new IllegalConfigurationException(
					"Configured/Derived memory sizes (total " + derivedTotalProcessMemorySize.toHumanReadableString()
						+ ") do not add up to the configured Total Process Memory size (" + configuredTotalProcessMemorySize.toHumanReadableString()
						+ "). Configured/Derived memory sizes are: "
						+ "Total Flink Memory (" + totalFlinkMemory.toHumanReadableString()
						+ "), JVM Metaspace (" + jvmMetaspaceAndOverhead.metaspace.toHumanReadableString()
						+ "), JVM Overhead (" + jvmMetaspaceAndOverhead.overhead.toHumanReadableString() + ").");
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
			if (derivedNetworkMemorySize.getBytes() > networkRangeFraction.maxSize.getBytes() ||
				derivedNetworkMemorySize.getBytes() < networkRangeFraction.minSize.getBytes()) {
				throw new IllegalConfigurationException("Derived Network Memory size ("
					+ derivedNetworkMemorySize.toHumanReadableString() + ") is not in configured Network Memory range ["
					+ networkRangeFraction.minSize.toHumanReadableString() + ", "
					+ networkRangeFraction.maxSize.toHumanReadableString() + "].");
			}
			if (isNetworkMemoryFractionExplicitlyConfigured(config) &&
				!derivedNetworkMemorySize.equals(totalFlinkMemorySize.multiply(networkRangeFraction.fraction))) {
				LOG.info(
					"The derived Network Memory size ({}) does not match " +
						"the configured Network Memory fraction ({}) from the configured Total Flink Memory size ({}). " +
						"The derived Network Memory size will be used.",
					derivedNetworkMemorySize.toHumanReadableString(),
					networkRangeFraction.fraction,
					totalFlinkMemorySize.toHumanReadableString());
			}
		}
	}

	private static void sanityCheckJvmOverhead(
			final Configuration config,
			final MemorySize derivedJvmOverheadSize,
			final MemorySize totalProcessMemorySize) {
		final RangeFraction jvmOverheadRangeFraction = getJvmOverheadRangeFraction(config);
		if (derivedJvmOverheadSize.getBytes() > jvmOverheadRangeFraction.maxSize.getBytes() ||
			derivedJvmOverheadSize.getBytes() < jvmOverheadRangeFraction.minSize.getBytes()) {
			throw new IllegalConfigurationException("Derived JVM Overhead size ("
				+ derivedJvmOverheadSize.toHumanReadableString() + ") is not in configured JVM Overhead range ["
				+ jvmOverheadRangeFraction.minSize.toHumanReadableString() + ", "
				+ jvmOverheadRangeFraction.maxSize.toHumanReadableString() + "].");
		}
		if (isJvmOverheadFractionExplicitlyConfigured(config) &&
			!derivedJvmOverheadSize.equals(totalProcessMemorySize.multiply(jvmOverheadRangeFraction.fraction))) {
			LOG.info(
				"The derived JVM Overhead size ({}) does not match " +
					"the configured JVM Overhead fraction ({}) from the configured Total Process Memory size ({}). " +
					"The derived JVM OVerhead size will be used.",
				derivedJvmOverheadSize.toHumanReadableString(),
				jvmOverheadRangeFraction.fraction,
				totalProcessMemorySize.toHumanReadableString());
		}
	}

	public static CPUResource getCpuCoresWithFallback(final Configuration config, double fallback) {
		return getCpuCores(config, fallback);
	}

	private static CPUResource getCpuCores(final Configuration config) {
		return getCpuCores(config, -1.0);
	}

	private static CPUResource getCpuCores(final Configuration config, double fallback) {
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

	private static TaskExecutorResourceSpec createTaskExecutorResourceSpec(
			final Configuration config,
			final FlinkInternalMemory flinkInternalMemory,
			final JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead) {
		return new TaskExecutorResourceSpec(
			getCpuCores(config),
			flinkInternalMemory.frameworkHeap,
			flinkInternalMemory.frameworkOffHeap,
			flinkInternalMemory.taskHeap,
			flinkInternalMemory.taskOffHeap,
			flinkInternalMemory.network,
			flinkInternalMemory.managed,
			jvmMetaspaceAndOverhead.metaspace,
			jvmMetaspaceAndOverhead.overhead);
	}

	private static class RangeFraction {
		final MemorySize minSize;
		final MemorySize maxSize;
		final double fraction;

		RangeFraction(final MemorySize minSize, final MemorySize maxSize, final double fraction) {
			this.minSize = minSize;
			this.maxSize = maxSize;
			this.fraction = fraction;
			checkArgument(minSize.getBytes() <= maxSize.getBytes(), "min value must be less or equal to max value");
			checkArgument(fraction >= 0 && fraction < 1, "fraction must be in range [0, 1)");
		}
	}

	private static class FlinkInternalMemory {
		final MemorySize frameworkHeap;
		final MemorySize frameworkOffHeap;
		final MemorySize taskHeap;
		final MemorySize taskOffHeap;
		final MemorySize network;
		final MemorySize managed;

		FlinkInternalMemory(
			final MemorySize frameworkHeap,
			final MemorySize frameworkOffHeap,
			final MemorySize taskHeap,
			final MemorySize taskOffHeap,
			final MemorySize network,
			final MemorySize managed) {

			this.frameworkHeap = checkNotNull(frameworkHeap);
			this.frameworkOffHeap = checkNotNull(frameworkOffHeap);
			this.taskHeap = checkNotNull(taskHeap);
			this.taskOffHeap = checkNotNull(taskOffHeap);
			this.network = checkNotNull(network);
			this.managed = checkNotNull(managed);
		}

		MemorySize getTotalFlinkMemorySize() {
			return frameworkHeap.add(frameworkOffHeap).add(taskHeap).add(taskOffHeap).add(network).add(managed);
		}
	}

	private static class JvmMetaspaceAndOverhead {
		final MemorySize metaspace;
		final MemorySize overhead;

		JvmMetaspaceAndOverhead(final MemorySize jvmMetaspace, final MemorySize jvmOverhead) {
			this.metaspace = checkNotNull(jvmMetaspace);
			this.overhead = checkNotNull(jvmOverhead);
		}

		MemorySize getTotalJvmMetaspaceAndOverheadSize() {
			return metaspace.add(overhead);
		}
	}

	public static Configuration getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
		final Configuration configuration, ConfigOption<MemorySize> configOption) {

		if (configuration.contains(configOption)) {
			return configuration;
		}

		return getLegacyTaskManagerHeapMemoryIfExplicitlyConfigured(configuration).map(legacyHeapSize -> {
			final Configuration copiedConfig = new Configuration(configuration);
			copiedConfig.set(configOption, legacyHeapSize);

			LOG.info(
				"'{}' is not specified, use the configured deprecated task manager heap value ({}) for it.",
				configOption.key(),
				legacyHeapSize.toHumanReadableString());

			return copiedConfig;
		}).orElse(configuration);
	}

	@SuppressWarnings("deprecation")
	private static Optional<MemorySize> getLegacyTaskManagerHeapMemoryIfExplicitlyConfigured(final Configuration configuration) {
		String totalProcessEnv = System.getenv("FLINK_TM_HEAP");
		if (totalProcessEnv != null) {
			try {
				return Optional.of(MemorySize.parse(totalProcessEnv));
			} catch (Throwable t) {
				throw new IllegalConfigurationException("Cannot read total process memory size from environment variable value "
					+ totalProcessEnv + ".", t);
			}
		}

		if (configuration.contains(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY)) {
			return Optional.of(getMemorySizeFromConfig(configuration, TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY));
		}

		if (configuration.contains(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB)) {
			final long legacyHeapMemoryMB = configuration.getInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB);
			if (legacyHeapMemoryMB < 0) {
				throw new IllegalConfigurationException("Configured total process memory size ("
					+ legacyHeapMemoryMB + "MB) must not be less than 0.");
			}
			return Optional.of(new MemorySize(legacyHeapMemoryMB << 20)); // megabytes to bytes;
		}

		return Optional.empty();
	}
}
