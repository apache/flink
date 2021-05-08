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

package org.apache.flink.runtime.util.config.memory.taskmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.config.memory.FlinkMemoryUtils;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtils;
import org.apache.flink.runtime.util.config.memory.RangeFraction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * {@link FlinkMemoryUtils} for Task Executor.
 *
 * <p>The required fine-grained components are {@link TaskManagerOptions#TASK_HEAP_MEMORY} and
 * {@link TaskManagerOptions#MANAGED_MEMORY_SIZE}.
 */
public class TaskExecutorFlinkMemoryUtils implements FlinkMemoryUtils<TaskExecutorFlinkMemory> {
    private static final Logger LOG = LoggerFactory.getLogger(TaskExecutorFlinkMemoryUtils.class);

    @Override
    public TaskExecutorFlinkMemory deriveFromRequiredFineGrainedOptions(Configuration config) {
        final MemorySize taskHeapMemorySize = getTaskHeapMemorySize(config);
        final MemorySize managedMemorySize = getManagedMemorySize(config);

        final MemorySize frameworkHeapMemorySize = getFrameworkHeapMemorySize(config);
        final MemorySize frameworkOffHeapMemorySize = getFrameworkOffHeapMemorySize(config);
        final MemorySize taskOffHeapMemorySize = getTaskOffHeapMemorySize(config);

        final MemorySize networkMemorySize;
        final MemorySize totalFlinkExcludeNetworkMemorySize =
                frameworkHeapMemorySize
                        .add(frameworkOffHeapMemorySize)
                        .add(taskHeapMemorySize)
                        .add(taskOffHeapMemorySize)
                        .add(managedMemorySize);

        if (isTotalFlinkMemorySizeExplicitlyConfigured(config)) {
            // derive network memory from total flink memory, and check against network min/max
            final MemorySize totalFlinkMemorySize = getTotalFlinkMemorySize(config);
            if (totalFlinkExcludeNetworkMemorySize.getBytes() > totalFlinkMemorySize.getBytes()) {
                throw new IllegalConfigurationException(
                        "Sum of configured Framework Heap Memory ("
                                + frameworkHeapMemorySize.toHumanReadableString()
                                + "), Framework Off-Heap Memory ("
                                + frameworkOffHeapMemorySize.toHumanReadableString()
                                + "), Task Heap Memory ("
                                + taskHeapMemorySize.toHumanReadableString()
                                + "), Task Off-Heap Memory ("
                                + taskOffHeapMemorySize.toHumanReadableString()
                                + ") and Managed Memory ("
                                + managedMemorySize.toHumanReadableString()
                                + ") exceed configured Total Flink Memory ("
                                + totalFlinkMemorySize.toHumanReadableString()
                                + ").");
            }
            networkMemorySize = totalFlinkMemorySize.subtract(totalFlinkExcludeNetworkMemorySize);
            sanityCheckNetworkMemoryWithExplicitlySetTotalFlinkAndHeapMemory(
                    config, networkMemorySize, totalFlinkMemorySize);
        } else {
            // derive network memory from network configs
            networkMemorySize =
                    isUsingLegacyNetworkConfigs(config)
                            ? getNetworkMemorySizeWithLegacyConfig(config)
                            : deriveNetworkMemoryWithInverseFraction(
                                    config, totalFlinkExcludeNetworkMemorySize);
        }

        final TaskExecutorFlinkMemory flinkInternalMemory =
                new TaskExecutorFlinkMemory(
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
    public TaskExecutorFlinkMemory deriveFromTotalFlinkMemory(
            final Configuration config, final MemorySize totalFlinkMemorySize) {
        final MemorySize frameworkHeapMemorySize = getFrameworkHeapMemorySize(config);
        final MemorySize frameworkOffHeapMemorySize = getFrameworkOffHeapMemorySize(config);
        final MemorySize taskOffHeapMemorySize = getTaskOffHeapMemorySize(config);

        final MemorySize taskHeapMemorySize;
        final MemorySize networkMemorySize;
        final MemorySize managedMemorySize;

        if (isTaskHeapMemorySizeExplicitlyConfigured(config)) {
            // task heap memory is configured,
            // derive managed memory first, leave the remaining to network memory and check against
            // network min/max
            taskHeapMemorySize = getTaskHeapMemorySize(config);
            managedMemorySize =
                    deriveManagedMemoryAbsoluteOrWithFraction(config, totalFlinkMemorySize);
            final MemorySize totalFlinkExcludeNetworkMemorySize =
                    frameworkHeapMemorySize
                            .add(frameworkOffHeapMemorySize)
                            .add(taskHeapMemorySize)
                            .add(taskOffHeapMemorySize)
                            .add(managedMemorySize);
            if (totalFlinkExcludeNetworkMemorySize.getBytes() > totalFlinkMemorySize.getBytes()) {
                throw new IllegalConfigurationException(
                        "Sum of configured Framework Heap Memory ("
                                + frameworkHeapMemorySize.toHumanReadableString()
                                + "), Framework Off-Heap Memory ("
                                + frameworkOffHeapMemorySize.toHumanReadableString()
                                + "), Task Heap Memory ("
                                + taskHeapMemorySize.toHumanReadableString()
                                + "), Task Off-Heap Memory ("
                                + taskOffHeapMemorySize.toHumanReadableString()
                                + ") and Managed Memory ("
                                + managedMemorySize.toHumanReadableString()
                                + ") exceed configured Total Flink Memory ("
                                + totalFlinkMemorySize.toHumanReadableString()
                                + ").");
            }
            networkMemorySize = totalFlinkMemorySize.subtract(totalFlinkExcludeNetworkMemorySize);
            sanityCheckNetworkMemoryWithExplicitlySetTotalFlinkAndHeapMemory(
                    config, networkMemorySize, totalFlinkMemorySize);
        } else {
            // task heap memory is not configured
            // derive managed memory and network memory, leave the remaining to task heap memory
            managedMemorySize =
                    deriveManagedMemoryAbsoluteOrWithFraction(config, totalFlinkMemorySize);

            networkMemorySize =
                    isUsingLegacyNetworkConfigs(config)
                            ? getNetworkMemorySizeWithLegacyConfig(config)
                            : deriveNetworkMemoryWithFraction(config, totalFlinkMemorySize);
            final MemorySize totalFlinkExcludeTaskHeapMemorySize =
                    frameworkHeapMemorySize
                            .add(frameworkOffHeapMemorySize)
                            .add(taskOffHeapMemorySize)
                            .add(managedMemorySize)
                            .add(networkMemorySize);
            if (totalFlinkExcludeTaskHeapMemorySize.getBytes() > totalFlinkMemorySize.getBytes()) {
                throw new IllegalConfigurationException(
                        "Sum of configured Framework Heap Memory ("
                                + frameworkHeapMemorySize.toHumanReadableString()
                                + "), Framework Off-Heap Memory ("
                                + frameworkOffHeapMemorySize.toHumanReadableString()
                                + "), Task Off-Heap Memory ("
                                + taskOffHeapMemorySize.toHumanReadableString()
                                + "), Managed Memory ("
                                + managedMemorySize.toHumanReadableString()
                                + ") and Network Memory ("
                                + networkMemorySize.toHumanReadableString()
                                + ") exceed configured Total Flink Memory ("
                                + totalFlinkMemorySize.toHumanReadableString()
                                + ").");
            }
            taskHeapMemorySize = totalFlinkMemorySize.subtract(totalFlinkExcludeTaskHeapMemorySize);
        }

        final TaskExecutorFlinkMemory flinkInternalMemory =
                new TaskExecutorFlinkMemory(
                        frameworkHeapMemorySize,
                        frameworkOffHeapMemorySize,
                        taskHeapMemorySize,
                        taskOffHeapMemorySize,
                        networkMemorySize,
                        managedMemorySize);
        sanityCheckTotalFlinkMemory(config, flinkInternalMemory);

        return flinkInternalMemory;
    }

    private static MemorySize deriveManagedMemoryAbsoluteOrWithFraction(
            final Configuration config, final MemorySize base) {
        return isManagedMemorySizeExplicitlyConfigured(config)
                ? getManagedMemorySize(config)
                : ProcessMemoryUtils.deriveWithFraction(
                        "managed memory", base, getManagedMemoryRangeFraction(config));
    }

    private static MemorySize deriveNetworkMemoryWithFraction(
            final Configuration config, final MemorySize base) {
        return ProcessMemoryUtils.deriveWithFraction(
                "network memory", base, getNetworkMemoryRangeFraction(config));
    }

    private static MemorySize deriveNetworkMemoryWithInverseFraction(
            final Configuration config, final MemorySize base) {
        return ProcessMemoryUtils.deriveWithInverseFraction(
                "network memory", base, getNetworkMemoryRangeFraction(config));
    }

    public static MemorySize getFrameworkHeapMemorySize(final Configuration config) {
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.FRAMEWORK_HEAP_MEMORY);
    }

    public static MemorySize getFrameworkOffHeapMemorySize(final Configuration config) {
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY);
    }

    private static MemorySize getTaskHeapMemorySize(final Configuration config) {
        checkArgument(isTaskHeapMemorySizeExplicitlyConfigured(config));
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.TASK_HEAP_MEMORY);
    }

    private static MemorySize getTaskOffHeapMemorySize(final Configuration config) {
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.TASK_OFF_HEAP_MEMORY);
    }

    private static MemorySize getManagedMemorySize(final Configuration config) {
        checkArgument(isManagedMemorySizeExplicitlyConfigured(config));
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.MANAGED_MEMORY_SIZE);
    }

    private static RangeFraction getManagedMemoryRangeFraction(final Configuration config) {
        return ProcessMemoryUtils.getRangeFraction(
                MemorySize.ZERO,
                MemorySize.MAX_VALUE,
                TaskManagerOptions.MANAGED_MEMORY_FRACTION,
                config);
    }

    private static MemorySize getNetworkMemorySizeWithLegacyConfig(final Configuration config) {
        checkArgument(isUsingLegacyNetworkConfigs(config));
        @SuppressWarnings("deprecation")
        final long numOfBuffers =
                config.getInteger(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
        final long pageSize = ConfigurationParserUtils.getPageSize(config);
        return new MemorySize(numOfBuffers * pageSize);
    }

    private static RangeFraction getNetworkMemoryRangeFraction(final Configuration config) {
        final MemorySize minSize =
                ProcessMemoryUtils.getMemorySizeFromConfig(
                        config, TaskManagerOptions.NETWORK_MEMORY_MIN);
        final MemorySize maxSize =
                ProcessMemoryUtils.getMemorySizeFromConfig(
                        config, TaskManagerOptions.NETWORK_MEMORY_MAX);
        return ProcessMemoryUtils.getRangeFraction(
                minSize, maxSize, TaskManagerOptions.NETWORK_MEMORY_FRACTION, config);
    }

    private static MemorySize getTotalFlinkMemorySize(final Configuration config) {
        checkArgument(isTotalFlinkMemorySizeExplicitlyConfigured(config));
        return ProcessMemoryUtils.getMemorySizeFromConfig(
                config, TaskManagerOptions.TOTAL_FLINK_MEMORY);
    }

    private static boolean isTaskHeapMemorySizeExplicitlyConfigured(final Configuration config) {
        return config.contains(TaskManagerOptions.TASK_HEAP_MEMORY);
    }

    private static boolean isManagedMemorySizeExplicitlyConfigured(final Configuration config) {
        return config.contains(TaskManagerOptions.MANAGED_MEMORY_SIZE);
    }

    private static boolean isUsingLegacyNetworkConfigs(final Configuration config) {
        // use the legacy number-of-buffer config option only when it is explicitly configured and
        // none of new config options is explicitly configured
        final boolean anyNetworkConfigured =
                config.contains(TaskManagerOptions.NETWORK_MEMORY_MIN)
                        || config.contains(TaskManagerOptions.NETWORK_MEMORY_MAX)
                        || config.contains(TaskManagerOptions.NETWORK_MEMORY_FRACTION);
        final boolean legacyConfigured =
                config.contains(NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS);
        return !anyNetworkConfigured && legacyConfigured;
    }

    private static boolean isNetworkMemoryFractionExplicitlyConfigured(final Configuration config) {
        return config.contains(TaskManagerOptions.NETWORK_MEMORY_FRACTION);
    }

    private static boolean isTotalFlinkMemorySizeExplicitlyConfigured(final Configuration config) {
        return config.contains(TaskManagerOptions.TOTAL_FLINK_MEMORY);
    }

    private static void sanityCheckTotalFlinkMemory(
            final Configuration config, final TaskExecutorFlinkMemory flinkInternalMemory) {
        if (isTotalFlinkMemorySizeExplicitlyConfigured(config)) {
            final MemorySize configuredTotalFlinkMemorySize = getTotalFlinkMemorySize(config);
            if (!configuredTotalFlinkMemorySize.equals(
                    flinkInternalMemory.getTotalFlinkMemorySize())) {
                throw new IllegalConfigurationException(
                        "Configured and Derived Flink internal memory sizes (total "
                                + flinkInternalMemory
                                        .getTotalFlinkMemorySize()
                                        .toHumanReadableString()
                                + ") do not add up to the configured Total Flink Memory size ("
                                + configuredTotalFlinkMemorySize.toHumanReadableString()
                                + "). Configured and Derived Flink internal memory sizes are: "
                                + "Framework Heap Memory ("
                                + flinkInternalMemory.getFrameworkHeap().toHumanReadableString()
                                + "), Framework Off-Heap Memory ("
                                + flinkInternalMemory.getFrameworkOffHeap().toHumanReadableString()
                                + "), Task Heap Memory ("
                                + flinkInternalMemory.getTaskHeap().toHumanReadableString()
                                + "), Task Off-Heap Memory ("
                                + flinkInternalMemory.getTaskOffHeap().toHumanReadableString()
                                + "), Network Memory ("
                                + flinkInternalMemory.getNetwork().toHumanReadableString()
                                + "), Managed Memory ("
                                + flinkInternalMemory.getManaged().toHumanReadableString()
                                + ").");
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
                    "If Total Flink, Task Heap and (or) Managed Memory sizes are explicitly configured then "
                            + "the Network Memory size is the rest of the Total Flink memory after subtracting all other "
                            + "configured types of memory, but the derived Network Memory is inconsistent with its configuration.",
                    e);
        }
    }

    private static void sanityCheckNetworkMemory(
            final Configuration config,
            final MemorySize derivedNetworkMemorySize,
            final MemorySize totalFlinkMemorySize) {
        if (isUsingLegacyNetworkConfigs(config)) {
            final MemorySize configuredNetworkMemorySize =
                    getNetworkMemorySizeWithLegacyConfig(config);
            if (!configuredNetworkMemorySize.equals(derivedNetworkMemorySize)) {
                throw new IllegalConfigurationException(
                        "Derived Network Memory size ("
                                + derivedNetworkMemorySize.toHumanReadableString()
                                + ") does not match configured Network Memory size ("
                                + configuredNetworkMemorySize.toHumanReadableString()
                                + ").");
            }
        } else {
            final RangeFraction networkRangeFraction = getNetworkMemoryRangeFraction(config);
            if (derivedNetworkMemorySize.getBytes() > networkRangeFraction.getMaxSize().getBytes()
                    || derivedNetworkMemorySize.getBytes()
                            < networkRangeFraction.getMinSize().getBytes()) {
                throw new IllegalConfigurationException(
                        "Derived Network Memory size ("
                                + derivedNetworkMemorySize.toHumanReadableString()
                                + ") is not in configured Network Memory range ["
                                + networkRangeFraction.getMinSize().toHumanReadableString()
                                + ", "
                                + networkRangeFraction.getMaxSize().toHumanReadableString()
                                + "].");
            }
            if (isNetworkMemoryFractionExplicitlyConfigured(config)
                    && !derivedNetworkMemorySize.equals(
                            totalFlinkMemorySize.multiply(networkRangeFraction.getFraction()))) {
                LOG.info(
                        "The derived Network Memory size ({}) does not match "
                                + "the configured Network Memory fraction ({}) from the configured Total Flink Memory size ({}). "
                                + "The derived Network Memory size will be used.",
                        derivedNetworkMemorySize.toHumanReadableString(),
                        networkRangeFraction.getFraction(),
                        totalFlinkMemorySize.toHumanReadableString());
            }
        }
    }
}
