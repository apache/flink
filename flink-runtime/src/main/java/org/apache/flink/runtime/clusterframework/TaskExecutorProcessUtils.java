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
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
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

import static org.apache.flink.configuration.ConfigurationUtils.assembleDynamicConfigsStr;

/**
 * Utility class for TaskExecutor memory configurations.
 *
 * <p>See {@link TaskExecutorProcessSpec} for details about memory components of TaskExecutor and
 * their relationships.
 */
public class TaskExecutorProcessUtils {

    static final ProcessMemoryOptions TM_PROCESS_MEMORY_OPTIONS =
            new ProcessMemoryOptions(
                    Arrays.asList(
                            TaskManagerOptions.TASK_HEAP_MEMORY,
                            TaskManagerOptions.MANAGED_MEMORY_SIZE),
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

    private static final ProcessMemoryUtils<TaskExecutorFlinkMemory> PROCESS_MEMORY_UTILS =
            new ProcessMemoryUtils<>(TM_PROCESS_MEMORY_OPTIONS, new TaskExecutorFlinkMemoryUtils());

    private static final MemoryBackwardsCompatibilityUtils LEGACY_MEMORY_UTILS =
            new MemoryBackwardsCompatibilityUtils(TM_LEGACY_HEAP_OPTIONS);

    private TaskExecutorProcessUtils() {}

    // ------------------------------------------------------------------------
    //  Generating Dynamic Config Options
    // ------------------------------------------------------------------------

    public static String generateDynamicConfigsStr(
            final TaskExecutorProcessSpec taskExecutorProcessSpec) {
        final Map<String, String> configs = new HashMap<>();
        configs.put(
                TaskManagerOptions.CPU_CORES.key(),
                String.valueOf(taskExecutorProcessSpec.getCpuCores().getValue().doubleValue()));
        configs.put(
                TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key(),
                taskExecutorProcessSpec.getFrameworkHeapSize().getBytes() + "b");
        configs.put(
                TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY.key(),
                taskExecutorProcessSpec.getFrameworkOffHeapMemorySize().getBytes() + "b");
        configs.put(
                TaskManagerOptions.TASK_HEAP_MEMORY.key(),
                taskExecutorProcessSpec.getTaskHeapSize().getBytes() + "b");
        configs.put(
                TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key(),
                taskExecutorProcessSpec.getTaskOffHeapSize().getBytes() + "b");
        configs.put(
                TaskManagerOptions.NETWORK_MEMORY_MIN.key(),
                taskExecutorProcessSpec.getNetworkMemSize().getBytes() + "b");
        configs.put(
                TaskManagerOptions.NETWORK_MEMORY_MAX.key(),
                taskExecutorProcessSpec.getNetworkMemSize().getBytes() + "b");
        configs.put(
                TaskManagerOptions.MANAGED_MEMORY_SIZE.key(),
                taskExecutorProcessSpec.getManagedMemorySize().getBytes() + "b");
        configs.put(
                TaskManagerOptions.JVM_METASPACE.key(),
                taskExecutorProcessSpec.getJvmMetaspaceAndOverhead().getMetaspace().getBytes()
                        + "b");
        configs.put(
                TaskManagerOptions.JVM_OVERHEAD_MIN.key(),
                taskExecutorProcessSpec.getJvmMetaspaceAndOverhead().getOverhead().getBytes()
                        + "b");
        configs.put(
                TaskManagerOptions.JVM_OVERHEAD_MAX.key(),
                taskExecutorProcessSpec.getJvmMetaspaceAndOverhead().getOverhead().getBytes()
                        + "b");
        configs.put(
                TaskManagerOptions.NUM_TASK_SLOTS.key(),
                String.valueOf(taskExecutorProcessSpec.getNumSlots()));
        if (!taskExecutorProcessSpec.getExtendedResources().isEmpty()) {
            configs.put(
                    ExternalResourceOptions.EXTERNAL_RESOURCE_LIST.key(),
                    String.join(";", taskExecutorProcessSpec.getExtendedResources().keySet()));
            taskExecutorProcessSpec
                    .getExtendedResources()
                    .forEach(
                            (resourceName, resource) ->
                                    configs.put(
                                            ExternalResourceOptions
                                                    .getAmountConfigOptionForResource(resourceName),
                                            String.valueOf(resource.getValue().longValue())));
        } else {
            configs.put(
                    ExternalResourceOptions.EXTERNAL_RESOURCE_LIST.key(),
                    ExternalResourceOptions.NONE);
        }

        return assembleDynamicConfigsStr(configs);
    }

    // ------------------------------------------------------------------------
    //  Memory Configuration Calculations
    // ------------------------------------------------------------------------

    public static TaskExecutorProcessSpecBuilder newProcessSpecBuilder(final Configuration config) {
        return TaskExecutorProcessSpecBuilder.newBuilder(config);
    }

    public static TaskExecutorProcessSpec processSpecFromConfig(final Configuration config) {
        try {
            return createMemoryProcessSpec(
                    config, PROCESS_MEMORY_UTILS.memoryProcessSpecFromConfig(config));
        } catch (IllegalConfigurationException e) {
            throw new IllegalConfigurationException(
                    "TaskManager memory configuration failed: " + e.getMessage(), e);
        }
    }

    public static TaskExecutorProcessSpec processSpecFromWorkerResourceSpec(
            final Configuration config, final WorkerResourceSpec workerResourceSpec) {

        final MemorySize frameworkHeapMemorySize =
                TaskExecutorFlinkMemoryUtils.getFrameworkHeapMemorySize(config);
        final MemorySize frameworkOffHeapMemorySize =
                TaskExecutorFlinkMemoryUtils.getFrameworkOffHeapMemorySize(config);

        final TaskExecutorFlinkMemory flinkMemory =
                new TaskExecutorFlinkMemory(
                        frameworkHeapMemorySize,
                        frameworkOffHeapMemorySize,
                        workerResourceSpec.getTaskHeapSize(),
                        workerResourceSpec.getTaskOffHeapSize(),
                        workerResourceSpec.getNetworkMemSize(),
                        workerResourceSpec.getManagedMemSize());

        final JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead =
                PROCESS_MEMORY_UTILS.deriveJvmMetaspaceAndOverheadFromTotalFlinkMemory(
                        config, flinkMemory.getTotalFlinkMemorySize());

        return new TaskExecutorProcessSpec(
                workerResourceSpec.getCpuCores(),
                flinkMemory,
                jvmMetaspaceAndOverhead,
                workerResourceSpec.getNumSlots(),
                workerResourceSpec.getExtendedResources().values());
    }

    private static TaskExecutorProcessSpec createMemoryProcessSpec(
            final Configuration config,
            final CommonProcessMemorySpec<TaskExecutorFlinkMemory> processMemory) {
        TaskExecutorFlinkMemory flinkMemory = processMemory.getFlinkMemory();
        JvmMetaspaceAndOverhead jvmMetaspaceAndOverhead =
                processMemory.getJvmMetaspaceAndOverhead();
        return new TaskExecutorProcessSpec(
                getCpuCores(config),
                flinkMemory,
                jvmMetaspaceAndOverhead,
                getNumSlots(config),
                ExternalResourceUtils.getExternalResourcesCollection(config));
    }

    private static CPUResource getCpuCores(final Configuration config) {
        return getCpuCoresWithFallback(config, -1.0);
    }

    private static int getNumSlots(final Configuration config) {
        return config.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);
    }

    public static double getCpuCoresWithFallbackConfigOption(
            final Configuration config, ConfigOption<Double> fallbackOption) {
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
            final Configuration configuration, final ConfigOption<MemorySize> configOption) {
        try {
            return LEGACY_MEMORY_UTILS.getConfWithLegacyHeapSizeMappedToNewConfigOption(
                    configuration, configOption);
        } catch (IllegalConfigurationException e) {
            throw new IllegalConfigurationException(
                    "TaskManager failed to map legacy JVM heap option to the new one: "
                            + e.getMessage(),
                    e);
        }
    }
}
