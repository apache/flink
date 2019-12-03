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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility class for TaskExecutor memory configurations.
 *
 * <p>See {@link TaskExecutorResourceSpec} for details about memory components of TaskExecutor and their relationships.
 */
public class TaskExecutorResourceUtils {

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
		configs.put(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key(), taskExecutorResourceSpec.getFrameworkHeapSize().getBytes() + "b");
		configs.put(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY.key(), taskExecutorResourceSpec.getFrameworkOffHeapMemorySize().getBytes() + "b");
		configs.put(TaskManagerOptions.TASK_HEAP_MEMORY.key(), taskExecutorResourceSpec.getTaskHeapSize().getBytes() + "b");
		configs.put(TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key(), taskExecutorResourceSpec.getTaskOffHeapSize().getBytes() + "b");
		configs.put(TaskManagerOptions.SHUFFLE_MEMORY_MIN.key(), taskExecutorResourceSpec.getShuffleMemSize().getBytes() + "b");
		configs.put(TaskManagerOptions.SHUFFLE_MEMORY_MAX.key(), taskExecutorResourceSpec.getShuffleMemSize().getBytes() + "b");
		configs.put(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), taskExecutorResourceSpec.getManagedMemorySize().getBytes() + "b");
		configs.put(TaskManagerOptions.MANAGED_MEMORY_OFFHEAP_SIZE.key(), taskExecutorResourceSpec.getOffHeapManagedMemorySize().getBytes() + "b");
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
			throw new IllegalConfigurationException("Either Task Heap Memory size and Managed Memory size, or Total Flink"
				+ " Memory size, or Total Process Memory size need to be configured explicitly.");
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

		final OnHeapAndOffHeapManagedMemory onHeapAndOffHeapManagedMemory = deriveOnHeapAndOffHeapMemoryFromManagedMemory(config, managedMemorySize);

		final MemorySize shuffleMemorySize;
		final MemorySize totalFlinkExcludeShuffleMemorySize =
			frameworkHeapMemorySize.add(frameworkOffHeapMemorySize).add(taskHeapMemorySize).add(taskOffHeapMemorySize).add(managedMemorySize);

		if (isTotalFlinkMemorySizeExplicitlyConfigured(config)) {
			// derive shuffle memory from total flink memory, and check against shuffle min/max
			final MemorySize totalFlinkMemorySize = getTotalFlinkMemorySize(config);
			if (totalFlinkExcludeShuffleMemorySize.getBytes() > totalFlinkMemorySize.getBytes()) {
				throw new IllegalConfigurationException(
					"Sum of configured Framework Heap Memory (" + frameworkHeapMemorySize.toString()
					+ "), Framework Off-Heap Memory (" + frameworkOffHeapMemorySize.toString()
					+ "), Task Heap Memory (" + taskHeapMemorySize.toString()
					+ "), Task Off-Heap Memory (" + taskOffHeapMemorySize.toString()
					+ ") and Managed Memory (" + managedMemorySize.toString()
					+ ") exceed configured Total Flink Memory (" + totalFlinkMemorySize.toString() + ").");
			}
			shuffleMemorySize = totalFlinkMemorySize.subtract(totalFlinkExcludeShuffleMemorySize);
			sanityCheckShuffleMemory(config, shuffleMemorySize, totalFlinkMemorySize);
		} else {
			// derive shuffle memory from shuffle configs
			if (isUsingLegacyShuffleConfigs(config)) {
				shuffleMemorySize = getShuffleMemorySizeWithLegacyConfig(config);
			} else {
				shuffleMemorySize = deriveShuffleMemoryWithInverseFraction(config, totalFlinkExcludeShuffleMemorySize);
			}
		}

		final FlinkInternalMemory flinkInternalMemory = new FlinkInternalMemory(
			frameworkHeapMemorySize,
			frameworkOffHeapMemorySize,
			taskHeapMemorySize,
			taskOffHeapMemorySize,
			shuffleMemorySize,
			onHeapAndOffHeapManagedMemory.onHeap,
			onHeapAndOffHeapManagedMemory.offHeap);
		sanityCheckTotalFlinkMemory(config, flinkInternalMemory);

		// derive jvm metaspace and overhead

		final JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead = deriveJvmMetaspaceAndOverheadFromTotalFlinkMemory(config, flinkInternalMemory.getTotalFlinkMemorySize());
		sanityCheckTotalProcessMemory(config, flinkInternalMemory.getTotalFlinkMemorySize(), jvmMetaspaceAndOverhead);

		return createTaskExecutorResourceSpec(flinkInternalMemory, jvmMetaspaceAndOverhead);
	}

	private static TaskExecutorResourceSpec deriveResourceSpecWithTotalFlinkMemory(final Configuration config) {
		// derive flink internal memory from explicitly configured total flink memory

		final MemorySize totalFlinkMemorySize = getTotalFlinkMemorySize(config);
		final FlinkInternalMemory flinkInternalMemory = deriveInternalMemoryFromTotalFlinkMemory(config, totalFlinkMemorySize);

		// derive jvm metaspace and overhead

		final JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead = deriveJvmMetaspaceAndOverheadFromTotalFlinkMemory(config, totalFlinkMemorySize);
		sanityCheckTotalProcessMemory(config, totalFlinkMemorySize, jvmMetaspaceAndOverhead);

		return createTaskExecutorResourceSpec(flinkInternalMemory, jvmMetaspaceAndOverhead);
	}

	private static TaskExecutorResourceSpec deriveResourceSpecWithTotalProcessMemory(final Configuration config) {
		// derive total flink memory from explicitly configured total process memory size

		final MemorySize totalProcessMemorySize = getTotalProcessMemorySize(config);
		final MemorySize jvmMetaspaceSize = getJvmMetaspaceSize(config);
		final MemorySize jvmOverheadSize = deriveJvmOverheadWithFraction(config, totalProcessMemorySize);
		final JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead = new JvmMetaspaceAndOverhead(jvmMetaspaceSize, jvmOverheadSize);

		if (jvmMetaspaceAndOverhead.getTotalJvmMetaspaceAndOverheadSize().getBytes() > totalProcessMemorySize.getBytes()) {
			throw new IllegalConfigurationException(
				"Sum of configured JVM Metaspace (" + jvmMetaspaceAndOverhead.metaspace.toString()
				+ ") and JVM Overhead (" + jvmMetaspaceAndOverhead.overhead.toString()
				+ ") exceed configured Total Process Memory (" + totalProcessMemorySize.toString() + ").");
		}
		final MemorySize totalFlinkMemorySize = totalProcessMemorySize.subtract(jvmMetaspaceAndOverhead.getTotalJvmMetaspaceAndOverheadSize());

		// derive flink internal memory

		final FlinkInternalMemory flinkInternalMemory = deriveInternalMemoryFromTotalFlinkMemory(config, totalFlinkMemorySize);

		return createTaskExecutorResourceSpec(flinkInternalMemory, jvmMetaspaceAndOverhead);
	}

	private static JvmMetaspaceAndOverhead deriveJvmMetaspaceAndOverheadFromTotalFlinkMemory(
		final Configuration config, final MemorySize totalFlinkMemorySize) {
		final MemorySize jvmMetaspaceSize = getJvmMetaspaceSize(config);
		final MemorySize jvmOverheadSize = deriveJvmOverheadWithInverseFraction(config,
			totalFlinkMemorySize.add(jvmMetaspaceSize));
		return new JvmMetaspaceAndOverhead(jvmMetaspaceSize, jvmOverheadSize);
	}

	private static FlinkInternalMemory deriveInternalMemoryFromTotalFlinkMemory(
		final Configuration config, final MemorySize totalFlinkMemorySize) {
		final MemorySize frameworkHeapMemorySize = getFrameworkHeapMemorySize(config);
		final MemorySize frameworkOffHeapMemorySize = getFrameworkOffHeapMemorySize(config);
		final MemorySize taskOffHeapMemorySize = getTaskOffHeapMemorySize(config);

		final MemorySize taskHeapMemorySize;
		final MemorySize shuffleMemorySize;
		final MemorySize managedMemorySize;

		if (isTaskHeapMemorySizeExplicitlyConfigured(config)) {
			// task heap memory is configured,
			// derive managed memory first, leave the remaining to shuffle memory and check against shuffle min/max
			taskHeapMemorySize = getTaskHeapMemorySize(config);
			managedMemorySize = deriveManagedMemoryAbsoluteOrWithFraction(config, totalFlinkMemorySize);
			final MemorySize totalFlinkExcludeShuffleMemorySize =
				frameworkHeapMemorySize.add(frameworkOffHeapMemorySize).add(taskHeapMemorySize).add(taskOffHeapMemorySize).add(managedMemorySize);
			if (totalFlinkExcludeShuffleMemorySize.getBytes() > totalFlinkMemorySize.getBytes()) {
				throw new IllegalConfigurationException(
					"Sum of configured Framework Heap Memory (" + frameworkHeapMemorySize.toString()
						+ "), Framework Off-Heap Memory (" + frameworkOffHeapMemorySize.toString()
						+ "), Task Heap Memory (" + taskHeapMemorySize.toString()
						+ "), Task Off-Heap Memory (" + taskOffHeapMemorySize.toString()
						+ ") and Managed Memory (" + managedMemorySize.toString()
						+ ") exceed configured Total Flink Memory (" + totalFlinkMemorySize.toString() + ").");
			}
			shuffleMemorySize = totalFlinkMemorySize.subtract(totalFlinkExcludeShuffleMemorySize);
			sanityCheckShuffleMemory(config, shuffleMemorySize, totalFlinkMemorySize);
		} else {
			// task heap memory is not configured
			// derive managed memory and shuffle memory, leave the remaining to task heap memory
			if (isManagedMemorySizeExplicitlyConfigured(config)) {
				managedMemorySize = getManagedMemorySize(config);
			} else {
				managedMemorySize = deriveManagedMemoryAbsoluteOrWithFraction(config, totalFlinkMemorySize);
			}
			if (isUsingLegacyShuffleConfigs(config)) {
				shuffleMemorySize = getShuffleMemorySizeWithLegacyConfig(config);
			} else {
				shuffleMemorySize = deriveShuffleMemoryWithFraction(config, totalFlinkMemorySize);
			}
			final MemorySize totalFlinkExcludeTaskHeapMemorySize =
				frameworkHeapMemorySize.add(frameworkOffHeapMemorySize).add(taskOffHeapMemorySize).add(managedMemorySize).add(shuffleMemorySize);
			if (totalFlinkExcludeTaskHeapMemorySize.getBytes() > totalFlinkMemorySize.getBytes()) {
				throw new IllegalConfigurationException(
					"Sum of configured Framework Heap Memory (" + frameworkHeapMemorySize.toString()
						+ "), Framework Off-Heap Memory (" + frameworkOffHeapMemorySize.toString()
						+ "), Task Off-Heap Memory (" + taskOffHeapMemorySize.toString()
						+ "), Managed Memory (" + managedMemorySize.toString()
						+ ") and Shuffle Memory (" + shuffleMemorySize.toString()
						+ ") exceed configured Total Flink Memory (" + totalFlinkMemorySize.toString() + ").");
			}
			taskHeapMemorySize = totalFlinkMemorySize.subtract(totalFlinkExcludeTaskHeapMemorySize);
		}

		final OnHeapAndOffHeapManagedMemory onHeapAndOffHeapManagedMemory = deriveOnHeapAndOffHeapMemoryFromManagedMemory(config, managedMemorySize);
		final FlinkInternalMemory flinkInternalMemory = new FlinkInternalMemory(
			frameworkHeapMemorySize,
			frameworkOffHeapMemorySize,
			taskHeapMemorySize,
			taskOffHeapMemorySize,
			shuffleMemorySize,
			onHeapAndOffHeapManagedMemory.onHeap,
			onHeapAndOffHeapManagedMemory.offHeap);
		sanityCheckTotalFlinkMemory(config, flinkInternalMemory);

		return flinkInternalMemory;
	}

	private static OnHeapAndOffHeapManagedMemory deriveOnHeapAndOffHeapMemoryFromManagedMemory(
		final Configuration config, final MemorySize managedMemorySize) {

		final MemorySize offHeapSize;

		if (isManagedMemoryOffHeapSizeExplicitlyConfigured(config)) {
			offHeapSize = getManagedMemoryOffHeapSize(config);
			// sanity check
			if (offHeapSize.getBytes() > managedMemorySize.getBytes()) {
				throw new IllegalConfigurationException("Configured Off-Heap Managed Memory size (" + offHeapSize.toString()
					+ ") is larger than configured/derived total Managed Memory size (" + managedMemorySize.toString() + ").");
			}
		} else {
			final double offHeapFraction;
			if (isManagedMemoryOffHeapFractionExplicitlyConfigured(config)) {
				offHeapFraction = getManagedMemoryOffHeapFraction(config);
			} else {
				@SuppressWarnings("deprecation")
				final boolean legacyManagedMemoryOffHeap = config.getBoolean(TaskManagerOptions.MEMORY_OFF_HEAP);
				offHeapFraction = legacyManagedMemoryOffHeap ? 1.0 : 0.0;
			}
			offHeapSize = managedMemorySize.multiply(offHeapFraction);
		}

		final MemorySize onHeapSize = managedMemorySize.subtract(offHeapSize);
		return new OnHeapAndOffHeapManagedMemory(onHeapSize, offHeapSize);
	}

	private static MemorySize deriveManagedMemoryAbsoluteOrWithFraction(final Configuration config, final MemorySize base) {
		if (isManagedMemorySizeExplicitlyConfigured(config)) {
			return getManagedMemorySize(config);
		} else {
			return deriveWithFraction(base, getManagedMemoryRangeFraction(config));
		}
	}

	private static MemorySize deriveShuffleMemoryWithFraction(final Configuration config, final MemorySize base) {
		return deriveWithFraction(base, getShuffleMemoryRangeFraction(config));
	}

	private static MemorySize deriveShuffleMemoryWithInverseFraction(final Configuration config, final MemorySize base) {
		return deriveWithInverseFraction(base, getShuffleMemoryRangeFraction(config));
	}

	private static MemorySize deriveJvmOverheadWithFraction(final Configuration config, final MemorySize base) {
		return deriveWithFraction(base, getJvmOverheadRangeFraction(config));
	}

	private static MemorySize deriveJvmOverheadWithInverseFraction(final Configuration config, final MemorySize base) {
		return deriveWithInverseFraction(base, getJvmOverheadRangeFraction(config));
	}

	private static MemorySize deriveWithFraction(final MemorySize base, final RangeFraction rangeFraction) {
		final long relative = (long) (rangeFraction.fraction * base.getBytes());
		return new MemorySize(Math.max(rangeFraction.minSize.getBytes(), Math.min(rangeFraction.maxSize.getBytes(), relative)));
	}

	private static MemorySize deriveWithInverseFraction(final MemorySize base, final RangeFraction rangeFraction) {
		checkArgument(rangeFraction.fraction < 1);
		final long relative = (long) (rangeFraction.fraction / (1 - rangeFraction.fraction) * base.getBytes());
		return new MemorySize(Math.max(rangeFraction.minSize.getBytes(), Math.min(rangeFraction.maxSize.getBytes(), relative)));
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
		final MemorySize minSize = MemorySize.ZERO;
		final MemorySize maxSize = MemorySize.MAX_VALUE;
		final double fraction = config.getFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION);
		if (fraction >= 1 || fraction < 0) {
			throw new IllegalConfigurationException("Configured Managed Memory fraction ("
				+ fraction + ") must be in [0, 1).");
		}
		return new RangeFraction(minSize, maxSize, fraction);
	}

	private static double getManagedMemoryOffHeapFraction(final Configuration config) {
		checkArgument(isManagedMemoryOffHeapFractionExplicitlyConfigured(config));
		final double offHeapFraction = config.getFloat(TaskManagerOptions.MANAGED_MEMORY_OFFHEAP_FRACTION);
		if (offHeapFraction > 1 || offHeapFraction < 0) {
			throw new IllegalConfigurationException("Configured Off-Heap Managed Memory fraction ("
				+ offHeapFraction + ") must be in [0, 1].");
		}
		return offHeapFraction;
	}

	private static MemorySize getManagedMemoryOffHeapSize(final Configuration config) {
		checkArgument(isManagedMemoryOffHeapSizeExplicitlyConfigured(config));
		return getMemorySizeFromConfig(config, TaskManagerOptions.MANAGED_MEMORY_OFFHEAP_SIZE);
	}

	private static MemorySize getShuffleMemorySizeWithLegacyConfig(final Configuration config) {
		checkArgument(isUsingLegacyShuffleConfigs(config));
		@SuppressWarnings("deprecation")
		final long numOfBuffers = config.getInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
		final long pageSize =  ConfigurationParserUtils.getPageSize(config);
		return new MemorySize(numOfBuffers * pageSize);
	}

	private static RangeFraction getShuffleMemoryRangeFraction(final Configuration config) {
		final MemorySize minSize = getMemorySizeFromConfig(config, TaskManagerOptions.SHUFFLE_MEMORY_MIN);
		final MemorySize maxSize = getMemorySizeFromConfig(config, TaskManagerOptions.SHUFFLE_MEMORY_MAX);
		final double fraction = config.getFloat(TaskManagerOptions.SHUFFLE_MEMORY_FRACTION);
		if (fraction >= 1 || fraction < 0) {
			throw new IllegalConfigurationException("Configured Shuffle Memory fraction ("
				+ fraction + ") must be in [0, 1).");
		}
		return new RangeFraction(minSize, maxSize, fraction);
	}

	private static MemorySize getJvmMetaspaceSize(final Configuration config) {
		return getMemorySizeFromConfig(config, TaskManagerOptions.JVM_METASPACE);
	}

	private static RangeFraction getJvmOverheadRangeFraction(final Configuration config) {
		final MemorySize minSize = getMemorySizeFromConfig(config, TaskManagerOptions.JVM_OVERHEAD_MIN);
		final MemorySize maxSize = getMemorySizeFromConfig(config, TaskManagerOptions.JVM_OVERHEAD_MAX);
		final double fraction = config.getFloat(TaskManagerOptions.JVM_OVERHEAD_FRACTION);
		if (fraction >= 1 || fraction < 0) {
			throw new IllegalConfigurationException("Configured JVM Overhead fraction ("
				+ fraction + ") must be in [0, 1).");
		}
		return new RangeFraction(minSize, maxSize, fraction);
	}

	private static MemorySize getTotalFlinkMemorySize(final Configuration config) {
		checkArgument(isTotalFlinkMemorySizeExplicitlyConfigured(config));
		return getMemorySizeFromConfig(config, TaskManagerOptions.TOTAL_FLINK_MEMORY);
	}

	private static MemorySize getTotalProcessMemorySize(final Configuration config) {
		checkArgument(isTotalProcessMemorySizeExplicitlyConfigured(config));
		String totalProcessEnv = System.getenv("FLINK_TM_HEAP");
		if (totalProcessEnv != null) {
			try {
				return MemorySize.parse(totalProcessEnv);
			} catch (Throwable t) {
				throw new IllegalConfigurationException("Cannot read total process memory size from environment variable value "
					+ totalProcessEnv + ".", t);
			}
		} else if (config.contains(TaskManagerOptions.TOTAL_PROCESS_MEMORY)) {
			return getMemorySizeFromConfig(config, TaskManagerOptions.TOTAL_PROCESS_MEMORY);
		} else {
			@SuppressWarnings("deprecation")
			final long legacyHeapMemoryMB = config.getInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB);
			if (legacyHeapMemoryMB < 0) {
				throw new IllegalConfigurationException("Configured total process memory size ("
					+ legacyHeapMemoryMB + "MB) must not be less than 0.");
			}
			return new MemorySize(legacyHeapMemoryMB << 20); // megabytes to bytes
		}
	}

	private static MemorySize getMemorySizeFromConfig(final Configuration config, final ConfigOption<String> option) {
		try {
			return MemorySize.parse(config.getString(option));
		} catch (Throwable t) {
			throw new IllegalConfigurationException("Cannot read memory size from config option '" + option.key() + "'.", t);
		}
	}

	private static boolean isTaskHeapMemorySizeExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.TASK_HEAP_MEMORY);
	}

	private static boolean isManagedMemorySizeExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.MANAGED_MEMORY_SIZE);
	}

	private static boolean isManagedMemoryOffHeapFractionExplicitlyConfigured(final Configuration config) {
		return config.getFloat(TaskManagerOptions.MANAGED_MEMORY_OFFHEAP_FRACTION) >= 0;
	}

	private static boolean isManagedMemoryOffHeapSizeExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.MANAGED_MEMORY_OFFHEAP_SIZE);
	}

	private static boolean isUsingLegacyShuffleConfigs(final Configuration config) {
		// use the legacy number-of-buffer config option only when it is explicitly configured and
		// none of new config options is explicitly configured
		@SuppressWarnings("deprecation")
		final boolean legacyConfigured = config.contains(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
		return !config.contains(TaskManagerOptions.SHUFFLE_MEMORY_MIN) &&
			!config.contains(TaskManagerOptions.SHUFFLE_MEMORY_MAX) &&
			!config.contains(TaskManagerOptions.SHUFFLE_MEMORY_FRACTION) &&
			legacyConfigured;
	}

	private static boolean isShuffleMemoryFractionExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.SHUFFLE_MEMORY_FRACTION);
	}

	private static boolean isTotalFlinkMemorySizeExplicitlyConfigured(final Configuration config) {
		return config.contains(TaskManagerOptions.TOTAL_FLINK_MEMORY);
	}

	private static boolean isTotalProcessMemorySizeExplicitlyConfigured(final Configuration config) {
		// backward compatible with the deprecated config options TASK_MANAGER_HEAP_MEMORY and TASK_MANAGER_HEAP_MEMORY_MB
		// only when they are explicitly configured by the user
		@SuppressWarnings("deprecation")
		final boolean legacyConfigured =
			config.contains(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY_MB) || config.contains(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY);
		final boolean configuredWithEnv = System.getenv("FLINK_TM_HEAP") != null;
		return config.contains(TaskManagerOptions.TOTAL_PROCESS_MEMORY) || legacyConfigured || configuredWithEnv;
	}

	private static void sanityCheckTotalFlinkMemory(final Configuration config, final FlinkInternalMemory flinkInternalMemory) {
		if (isTotalFlinkMemorySizeExplicitlyConfigured(config)) {
			final MemorySize configuredTotalFlinkMemorySize = getTotalFlinkMemorySize(config);
			if (!configuredTotalFlinkMemorySize.equals(flinkInternalMemory.getTotalFlinkMemorySize())) {
				throw new IllegalConfigurationException(
					"Configured/Derived Flink internal memory sizes (total " + flinkInternalMemory.getTotalFlinkMemorySize().toString()
						+ ") do not add up to the configured Total Flink Memory size (" + configuredTotalFlinkMemorySize.toString()
						+ "). Configured/Derived Flink internal memory sizes are: "
						+ "Framework Heap Memory (" + flinkInternalMemory.frameworkHeap.toString()
						+ "), Framework Off-Heap Memory (" + flinkInternalMemory.frameworkOffHeap.toString()
						+ "), Task Heap Memory (" + flinkInternalMemory.taskHeap.toString()
						+ "), Task Off-Heap Memory (" + flinkInternalMemory.taskOffHeap.toString()
						+ "), Shuffle Memory (" + flinkInternalMemory.shuffle.toString()
						+ "), Managed Memory (" + flinkInternalMemory.getManagedMemorySize().toString() + ").");
			}
		}
	}

	private static void sanityCheckTotalProcessMemory(final Configuration config, final MemorySize totalFlinkMemory, final JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead) {
		final MemorySize derivedTotalProcessMemorySize =
			totalFlinkMemory.add(jvmMetaspaceAndOverhead.metaspace).add(jvmMetaspaceAndOverhead.overhead);
		if (isTotalProcessMemorySizeExplicitlyConfigured(config)) {
			final MemorySize configuredTotalProcessMemorySize = getTotalProcessMemorySize(config);
			if (!configuredTotalProcessMemorySize.equals(derivedTotalProcessMemorySize)) {
				throw new IllegalConfigurationException(
					"Configured/Derived memory sizes (total " + derivedTotalProcessMemorySize.toString()
						+ ") do not add up to the configured Total Process Memory size (" + configuredTotalProcessMemorySize.toString()
						+ "). Configured/Derived memory sizes are: "
						+ "Total Flink Memory (" + totalFlinkMemory.toString()
						+ "), JVM Metaspace (" + jvmMetaspaceAndOverhead.metaspace.toString()
						+ "), JVM Overhead (" + jvmMetaspaceAndOverhead.overhead.toString() + ").");
			}
		}
	}

	private static void sanityCheckShuffleMemory(final Configuration config, final MemorySize derivedShuffleMemorySize, final MemorySize totalFlinkMemorySize) {
		if (isUsingLegacyShuffleConfigs(config)) {
			final MemorySize configuredShuffleMemorySize = getShuffleMemorySizeWithLegacyConfig(config);
			if (!configuredShuffleMemorySize.equals(derivedShuffleMemorySize)) {
				throw new IllegalConfigurationException(
					"Derived Shuffle Memory size (" + derivedShuffleMemorySize.toString()
					+ ") does not match configured Shuffle Memory size (" + configuredShuffleMemorySize.toString() + ").");
			}
		} else {
			final RangeFraction shuffleRangeFraction = getShuffleMemoryRangeFraction(config);
			if (derivedShuffleMemorySize.getBytes() > shuffleRangeFraction.maxSize.getBytes() ||
				derivedShuffleMemorySize.getBytes() < shuffleRangeFraction.minSize.getBytes()) {
				throw new IllegalConfigurationException("Derived Shuffle Memory size ("
					+ derivedShuffleMemorySize.toString() + ") is not in configured Shuffle Memory range ["
					+ shuffleRangeFraction.minSize.toString() + ", "
					+ shuffleRangeFraction.maxSize.toString() + "].");
			}
			if (isShuffleMemoryFractionExplicitlyConfigured(config) &&
				!derivedShuffleMemorySize.equals(totalFlinkMemorySize.multiply(shuffleRangeFraction.fraction))) {
				throw new IllegalConfigurationException("Derived Shuffle Memory size("
					+ derivedShuffleMemorySize.toString() + ") does not match configured Shuffle Memory fraction ("
					+ shuffleRangeFraction.fraction + ").");
			}
		}
	}

	private static TaskExecutorResourceSpec createTaskExecutorResourceSpec(
		final FlinkInternalMemory flinkInternalMemory, final JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead) {
		return new TaskExecutorResourceSpec(
			flinkInternalMemory.frameworkHeap,
			flinkInternalMemory.frameworkOffHeap,
			flinkInternalMemory.taskHeap,
			flinkInternalMemory.taskOffHeap,
			flinkInternalMemory.shuffle,
			flinkInternalMemory.onHeapManaged,
			flinkInternalMemory.offHeapManaged,
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
			checkArgument(minSize.getBytes() <= maxSize.getBytes());
			checkArgument(fraction >= 0 && fraction <= 1);
		}
	}

	private static class FlinkInternalMemory {
		final MemorySize frameworkHeap;
		final MemorySize frameworkOffHeap;
		final MemorySize taskHeap;
		final MemorySize taskOffHeap;
		final MemorySize shuffle;
		final MemorySize onHeapManaged;
		final MemorySize offHeapManaged;

		FlinkInternalMemory(
			final MemorySize frameworkHeap,
			final MemorySize frameworkOffHeap,
			final MemorySize taskHeap,
			final MemorySize taskOffHeap,
			final MemorySize shuffle,
			final MemorySize onHeapManaged,
			final MemorySize offHeapManaged) {

			this.frameworkHeap = checkNotNull(frameworkHeap);
			this.frameworkOffHeap = checkNotNull(frameworkOffHeap);
			this.taskHeap = checkNotNull(taskHeap);
			this.taskOffHeap = checkNotNull(taskOffHeap);
			this.shuffle = checkNotNull(shuffle);
			this.onHeapManaged = checkNotNull(onHeapManaged);
			this.offHeapManaged = checkNotNull(offHeapManaged);
		}

		MemorySize getTotalFlinkMemorySize() {
			return frameworkHeap.add(frameworkOffHeap).add(taskHeap).add(taskOffHeap).add(shuffle).add(getManagedMemorySize());
		}

		MemorySize getManagedMemorySize() {
			return onHeapManaged.add(offHeapManaged);
		}
	}

	private static class OnHeapAndOffHeapManagedMemory {
		final MemorySize onHeap;
		final MemorySize offHeap;

		OnHeapAndOffHeapManagedMemory(final MemorySize onHeap, final MemorySize offHeap) {
			this.onHeap = onHeap;
			this.offHeap = offHeap;
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

	public static Configuration adjustConfigurationForLocalExecution(final Configuration configuration, int numberOfTaskExecutors) {

		final long jvmFreeHeapMemBytesPerTm = EnvironmentInformation.getSizeOfFreeHeapMemoryWithDefrag() / numberOfTaskExecutors;
		final MemorySize totalHeapMemory = new MemorySize(jvmFreeHeapMemBytesPerTm);

		final MemorySize frameworkOffHeap = getFrameworkOffHeapMemorySize(configuration);
		final MemorySize taskOffHeap = getTaskOffHeapMemorySize(configuration);
		final MemorySize totalFlinkMemoryExceptShuffleAndManaged = totalHeapMemory.add(frameworkOffHeap).add(taskOffHeap);

		final double shuffleFraction = getShuffleMemoryRangeFraction(configuration).fraction;
		final double managedFraction = getManagedMemoryRangeFraction(configuration).fraction;
		final double managedOffHeapFraction;
		if (isManagedMemoryOffHeapFractionExplicitlyConfigured(configuration)) {
			managedOffHeapFraction = getManagedMemoryOffHeapFraction(configuration);
		} else {
			final boolean legacyManagedMemoryOffHeap = configuration.getBoolean(TaskManagerOptions.MEMORY_OFF_HEAP);
			managedOffHeapFraction = legacyManagedMemoryOffHeap ? 1.0 : 0.0;
		}

		final MemorySize estimatedTotalFlinkMemory = totalFlinkMemoryExceptShuffleAndManaged
			.multiply(1 / (1 - shuffleFraction - managedFraction * managedOffHeapFraction));

		final Configuration modifiedConfig = new Configuration(configuration);
		modifiedConfig.setString(TaskManagerOptions.TOTAL_FLINK_MEMORY, estimatedTotalFlinkMemory.toString());

		return modifiedConfig;
	}
}
