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
import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.util.config.memory.ProcessMemoryUtilsTestBase;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils.TM_LEGACY_HEAP_OPTIONS;
import static org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils.TM_PROCESS_MEMORY_OPTIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TaskExecutorProcessUtils}. */
class TaskExecutorProcessUtilsTest extends ProcessMemoryUtilsTestBase<TaskExecutorProcessSpec> {

    private static final MemorySize TASK_HEAP_SIZE = MemorySize.parse("100m");
    private static final MemorySize MANAGED_MEM_SIZE = MemorySize.parse("200m");
    private static final MemorySize TOTAL_FLINK_MEM_SIZE = MemorySize.parse("1280m");
    private static final MemorySize TOTAL_PROCESS_MEM_SIZE = MemorySize.parse("1536m");
    private static final String EXTERNAL_RESOURCE_NAME_1 = "gpu";
    private static final String EXTERNAL_RESOURCE_NAME_2 = "custom";

    private static final TaskExecutorProcessSpec TM_RESOURCE_SPEC =
            new TaskExecutorProcessSpec(
                    new CPUResource(1.0),
                    MemorySize.parse("1m"),
                    MemorySize.parse("2m"),
                    MemorySize.parse("3m"),
                    MemorySize.parse("4m"),
                    MemorySize.parse("5m"),
                    MemorySize.parse("6m"),
                    MemorySize.parse("7m"),
                    MemorySize.parse("8m"),
                    Arrays.asList(
                            new ExternalResource(EXTERNAL_RESOURCE_NAME_1, 1),
                            new ExternalResource(EXTERNAL_RESOURCE_NAME_2, 2)));

    public TaskExecutorProcessUtilsTest() {
        super(
                TM_PROCESS_MEMORY_OPTIONS,
                TM_LEGACY_HEAP_OPTIONS,
                TaskManagerOptions.TOTAL_PROCESS_MEMORY);
    }

    @Test
    void testGenerateDynamicConfigurations() {
        String dynamicConfigsStr =
                TaskExecutorProcessUtils.generateDynamicConfigsStr(TM_RESOURCE_SPEC);
        Map<String, String> configs =
                ConfigurationUtils.parseTmResourceDynamicConfigs(dynamicConfigsStr);

        assertThat(
                        new CPUResource(
                                Double.parseDouble(
                                        configs.get(TaskManagerOptions.CPU_CORES.key()))))
                .isEqualTo(TM_RESOURCE_SPEC.getCpuCores());
        assertThat(MemorySize.parse(configs.get(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY.key())))
                .isEqualTo(TM_RESOURCE_SPEC.getFrameworkHeapSize());
        assertThat(
                        MemorySize.parse(
                                configs.get(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY.key())))
                .isEqualTo(TM_RESOURCE_SPEC.getFrameworkOffHeapMemorySize());
        assertThat(MemorySize.parse(configs.get(TaskManagerOptions.TASK_HEAP_MEMORY.key())))
                .isEqualTo(TM_RESOURCE_SPEC.getTaskHeapSize());
        assertThat(MemorySize.parse(configs.get(TaskManagerOptions.TASK_OFF_HEAP_MEMORY.key())))
                .isEqualTo(TM_RESOURCE_SPEC.getTaskOffHeapSize());
        assertThat(MemorySize.parse(configs.get(TaskManagerOptions.NETWORK_MEMORY_MAX.key())))
                .isEqualTo(TM_RESOURCE_SPEC.getNetworkMemSize());
        assertThat(MemorySize.parse(configs.get(TaskManagerOptions.NETWORK_MEMORY_MIN.key())))
                .isEqualTo(TM_RESOURCE_SPEC.getNetworkMemSize());
        assertThat(MemorySize.parse(configs.get(TaskManagerOptions.MANAGED_MEMORY_SIZE.key())))
                .isEqualTo(TM_RESOURCE_SPEC.getManagedMemorySize());
        assertThat(MemorySize.parse(configs.get(TaskManagerOptions.JVM_METASPACE.key())))
                .isEqualTo(TM_RESOURCE_SPEC.getJvmMetaspaceAndOverhead().getMetaspace());
        assertThat(MemorySize.parse(configs.get(TaskManagerOptions.JVM_OVERHEAD_MIN.key())))
                .isEqualTo(TM_RESOURCE_SPEC.getJvmMetaspaceAndOverhead().getOverhead());
        assertThat(MemorySize.parse(configs.get(TaskManagerOptions.JVM_OVERHEAD_MAX.key())))
                .isEqualTo(TM_RESOURCE_SPEC.getJvmMetaspaceAndOverhead().getOverhead());
        assertThat(Integer.valueOf(configs.get(TaskManagerOptions.NUM_TASK_SLOTS.key())))
                .isEqualTo(TM_RESOURCE_SPEC.getNumSlots());
        assertThat(configs.get(ExternalResourceOptions.EXTERNAL_RESOURCE_LIST.key()))
                .isEqualTo(
                        '"'
                                + String.join(";", TM_RESOURCE_SPEC.getExtendedResources().keySet())
                                + '"');
        assertThat(
                        configs.get(
                                ExternalResourceOptions.getAmountConfigOptionForResource(
                                        EXTERNAL_RESOURCE_NAME_1)))
                .isEqualTo(
                        String.valueOf(
                                TM_RESOURCE_SPEC
                                        .getExtendedResources()
                                        .get(EXTERNAL_RESOURCE_NAME_1)
                                        .getValue()
                                        .longValue()));
        assertThat(
                        configs.get(
                                ExternalResourceOptions.getAmountConfigOptionForResource(
                                        EXTERNAL_RESOURCE_NAME_2)))
                .isEqualTo(
                        String.valueOf(
                                TM_RESOURCE_SPEC
                                        .getExtendedResources()
                                        .get(EXTERNAL_RESOURCE_NAME_2)
                                        .getValue()
                                        .longValue()));
    }

    @Test
    void testProcessSpecFromWorkerResourceSpec() {
        final WorkerResourceSpec workerResourceSpec =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(100)
                        .setTaskOffHeapMemoryMB(200)
                        .setNetworkMemoryMB(300)
                        .setManagedMemoryMB(400)
                        .setNumSlots(5)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME_1, 1))
                        .build();
        final TaskExecutorProcessSpec taskExecutorProcessSpec =
                TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(
                        new Configuration(), workerResourceSpec);
        assertThat(taskExecutorProcessSpec.getCpuCores())
                .isEqualTo(workerResourceSpec.getCpuCores());
        assertThat(taskExecutorProcessSpec.getTaskHeapSize())
                .isEqualTo(workerResourceSpec.getTaskHeapSize());
        assertThat(taskExecutorProcessSpec.getTaskOffHeapSize())
                .isEqualTo(workerResourceSpec.getTaskOffHeapSize());
        assertThat(taskExecutorProcessSpec.getNetworkMemSize())
                .isEqualTo(workerResourceSpec.getNetworkMemSize());
        assertThat(taskExecutorProcessSpec.getManagedMemorySize())
                .isEqualTo(workerResourceSpec.getManagedMemSize());
        assertThat(taskExecutorProcessSpec.getNumSlots())
                .isEqualTo(workerResourceSpec.getNumSlots());
        assertThat(taskExecutorProcessSpec.getExtendedResources())
                .isEqualTo(workerResourceSpec.getExtendedResources());
    }

    @Test
    void testConfigCpuCores() {
        final double cpuCores = 1.0;

        Configuration conf = new Configuration();
        conf.setDouble(TaskManagerOptions.CPU_CORES, cpuCores);

        validateInAllConfigurations(
                conf,
                taskExecutorProcessSpec ->
                        assertThat(taskExecutorProcessSpec.getCpuCores())
                                .isEqualTo(new CPUResource(cpuCores)));
    }

    @Test
    void testConfigNoCpuCores() {
        Configuration conf = new Configuration();
        conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);
        validateInAllConfigurations(
                conf,
                taskExecutorProcessSpec ->
                        assertThat(taskExecutorProcessSpec.getCpuCores())
                                .isEqualTo(new CPUResource(3.0)));
    }

    @Test
    void testConfigNegativeCpuCores() {
        Configuration conf = new Configuration();
        conf.setDouble(TaskManagerOptions.CPU_CORES, -0.1f);
        validateFailInAllConfigurations(conf);
    }

    @Test
    void testConfigFrameworkHeapMemory() {
        final MemorySize frameworkHeapSize = MemorySize.parse("100m");

        Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, frameworkHeapSize);

        validateInAllConfigurations(
                conf,
                taskExecutorProcessSpec ->
                        assertThat(taskExecutorProcessSpec.getFrameworkHeapSize())
                                .isEqualTo(frameworkHeapSize));
    }

    @Test
    void testConfigFrameworkOffHeapMemory() {
        final MemorySize frameworkOffHeapSize = MemorySize.parse("10m");

        Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY, frameworkOffHeapSize);

        validateInAllConfigurations(
                conf,
                taskExecutorProcessSpec ->
                        assertThat(taskExecutorProcessSpec.getFrameworkOffHeapMemorySize())
                                .isEqualTo(frameworkOffHeapSize));
    }

    @Test
    void testConfigTaskHeapMemory() {
        final MemorySize taskHeapSize = MemorySize.parse("50m");

        Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.TASK_HEAP_MEMORY, taskHeapSize);

        // validate in configurations without explicit task heap memory size,
        // to avoid checking against overwritten task heap memory size
        validateInConfigurationsWithoutExplicitTaskHeapMem(
                conf,
                taskExecutorProcessSpec ->
                        assertThat(taskExecutorProcessSpec.getTaskHeapSize())
                                .isEqualTo(taskHeapSize));
    }

    @Test
    void testConfigTaskOffheapMemory() {
        final MemorySize taskOffHeapSize = MemorySize.parse("50m");

        Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, taskOffHeapSize);

        validateInAllConfigurations(
                conf,
                taskExecutorProcessSpec ->
                        assertThat(taskExecutorProcessSpec.getTaskOffHeapSize())
                                .isEqualTo(taskOffHeapSize));
    }

    @Test
    void testConfigNetworkMemoryRange() {
        final MemorySize networkMin = MemorySize.parse("200m");
        final MemorySize networkMax = MemorySize.parse("500m");

        Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, networkMax);
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, networkMin);

        validateInAllConfigurations(
                conf,
                taskExecutorProcessSpec -> {
                    assertThat(taskExecutorProcessSpec.getNetworkMemSize().getBytes())
                            .isGreaterThanOrEqualTo(networkMin.getBytes());
                    assertThat(taskExecutorProcessSpec.getNetworkMemSize().getBytes())
                            .isLessThanOrEqualTo(networkMax.getBytes());
                });
    }

    @Test
    void testConsistencyCheckOfDerivedNetworkMemoryWithinMinMaxRangeNotMatchingFractionPasses() {
        final Configuration configuration =
                setupConfigWithFlinkAndTaskHeapToDeriveGivenNetworkMem(400);
        // set fraction to be extremely low to not match the derived network memory
        configuration.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, 0.001f);
        // internal validation should pass
        TaskExecutorProcessUtils.processSpecFromConfig(configuration);
    }

    @Test
    public void testConsistencyCheckOfDerivedNetworkMemoryLessThanMinFails() {
        final Configuration configuration =
                setupConfigWithFlinkAndTaskHeapToDeriveGivenNetworkMem(500);
        configuration.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("900m"));
        configuration.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("1000m"));

        // internal validation should fail
        assertThatExceptionOfType(IllegalConfigurationException.class)
                .isThrownBy(() -> TaskExecutorProcessUtils.processSpecFromConfig(configuration));
    }

    @Test
    public void testConsistencyCheckOfDerivedNetworkMemoryGreaterThanMaxFails() {
        final Configuration configuration =
                setupConfigWithFlinkAndTaskHeapToDeriveGivenNetworkMem(500);
        configuration.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("100m"));
        configuration.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("150m"));
        // internal validation should fail
        assertThatExceptionOfType(IllegalConfigurationException.class)
                .isThrownBy(() -> TaskExecutorProcessUtils.processSpecFromConfig(configuration));
    }

    @Test
    public void testConsistencyCheckOfDerivedNetworkMemoryDoesNotMatchLegacyConfigFails() {
        final int numberOfNetworkBuffers = 10;
        final int pageSizeMb = 16;
        // derive network memory which is bigger than the number of legacy network buffers
        final int networkMemorySizeMbToDerive = pageSizeMb * (numberOfNetworkBuffers + 1);
        final Configuration configuration =
                setupConfigWithFlinkAndTaskHeapToDeriveGivenNetworkMem(networkMemorySizeMbToDerive);
        configuration.set(
                TaskManagerOptions.MEMORY_SEGMENT_SIZE, MemorySize.ofMebiBytes(pageSizeMb));
        configuration.setInteger(
                NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS, numberOfNetworkBuffers);
        // internal validation should fail
        assertThatExceptionOfType(IllegalConfigurationException.class)
                .isThrownBy(() -> TaskExecutorProcessUtils.processSpecFromConfig(configuration));
    }

    private static Configuration setupConfigWithFlinkAndTaskHeapToDeriveGivenNetworkMem(
            final int networkMemorySizeToDeriveMb) {
        final Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, TOTAL_FLINK_MEM_SIZE);
        conf.set(TaskManagerOptions.TASK_HEAP_MEMORY, TASK_HEAP_SIZE);
        conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MANAGED_MEM_SIZE);

        final TaskExecutorProcessSpec taskExecutorProcessSpec =
                TaskExecutorProcessUtils.processSpecFromConfig(conf);
        final int derivedNetworkMemorySizeMb =
                taskExecutorProcessSpec.getNetworkMemSize().getMebiBytes();
        if (derivedNetworkMemorySizeMb < networkMemorySizeToDeriveMb) {
            // adjust total Flink memory size to accommodate for more network memory
            final int adjustedTotalFlinkMemoryMb =
                    taskExecutorProcessSpec.getTotalFlinkMemorySize().getMebiBytes()
                            - derivedNetworkMemorySizeMb
                            + networkMemorySizeToDeriveMb;
            conf.set(
                    TaskManagerOptions.TOTAL_FLINK_MEMORY,
                    MemorySize.ofMebiBytes(adjustedTotalFlinkMemoryMb));
        } else if (derivedNetworkMemorySizeMb > networkMemorySizeToDeriveMb) {
            // reduce derived network memory by increasing task heap size
            final int adjustedTaskHeapMemoryMb =
                    taskExecutorProcessSpec.getTaskHeapSize().getMebiBytes()
                            + derivedNetworkMemorySizeMb
                            - networkMemorySizeToDeriveMb;
            conf.set(
                    TaskManagerOptions.TASK_HEAP_MEMORY,
                    MemorySize.ofMebiBytes(adjustedTaskHeapMemoryMb));
        }

        final TaskExecutorProcessSpec adjusteedTaskExecutorProcessSpec =
                TaskExecutorProcessUtils.processSpecFromConfig(conf);
        assertThat(adjusteedTaskExecutorProcessSpec.getNetworkMemSize().getMebiBytes())
                .isEqualTo(networkMemorySizeToDeriveMb);

        return conf;
    }

    @Test
    void testConfigNetworkMemoryRangeFailure() {
        final MemorySize networkMin = MemorySize.parse("200m");
        final MemorySize networkMax = MemorySize.parse("50m");

        Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, networkMax);
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, networkMin);

        validateFailInAllConfigurations(conf);
    }

    @Test
    void testConfigNetworkMemoryFraction() {
        final MemorySize networkMin = MemorySize.ZERO;
        final MemorySize networkMax = MemorySize.parse("1t");
        final float fraction = 0.2f;

        Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, networkMax);
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, networkMin);
        conf.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, fraction);

        // validate in configurations without explicit total flink/process memory, otherwise
        // explicit configured
        // network memory fraction might conflict with total flink/process memory minus other memory
        // sizes
        validateInConfigWithExplicitTaskHeapAndManagedMem(
                conf,
                taskExecutorProcessSpec ->
                        assertThat(taskExecutorProcessSpec.getNetworkMemSize())
                                .isEqualTo(
                                        taskExecutorProcessSpec
                                                .getTotalFlinkMemorySize()
                                                .multiply(fraction)));
    }

    @Test
    void testConfigNetworkMemoryFractionFailure() {
        Configuration conf = new Configuration();
        conf.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, -0.1f);
        validateFailInAllConfigurations(conf);

        conf.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, 1.0f);
        validateFailInAllConfigurations(conf);
    }

    @Test
    void testConfigNetworkMemoryLegacyRangeFraction() {
        final MemorySize networkMin = MemorySize.parse("200m");
        final MemorySize networkMax = MemorySize.parse("500m");

        final float fraction = 0.2f;

        @SuppressWarnings("deprecation")
        final ConfigOption<String> legacyOptionMin =
                NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MIN;
        @SuppressWarnings("deprecation")
        final ConfigOption<String> legacyOptionMax =
                NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_MAX;
        @SuppressWarnings("deprecation")
        final ConfigOption<Float> legacyOptionFraction =
                NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_MEMORY_FRACTION;

        Configuration conf = new Configuration();
        conf.setString(legacyOptionMin, networkMin.getMebiBytes() + "m");
        conf.setString(legacyOptionMax, networkMax.getMebiBytes() + "m");

        validateInAllConfigurations(
                conf,
                taskExecutorProcessSpec -> {
                    assertThat(taskExecutorProcessSpec.getNetworkMemSize().getBytes())
                            .isGreaterThanOrEqualTo(networkMin.getBytes());
                    assertThat(taskExecutorProcessSpec.getNetworkMemSize().getBytes())
                            .isLessThanOrEqualTo(networkMax.getBytes());
                });

        conf.setString(legacyOptionMin, "0m");
        conf.setString(legacyOptionMax, "1t");
        conf.setFloat(legacyOptionFraction, fraction);

        validateInConfigWithExplicitTaskHeapAndManagedMem(
                conf,
                taskExecutorProcessSpec ->
                        assertThat(taskExecutorProcessSpec.getNetworkMemSize())
                                .isEqualTo(
                                        taskExecutorProcessSpec
                                                .getTotalFlinkMemorySize()
                                                .multiply(fraction)));
    }

    @Test
    void testConfigNetworkMemoryLegacyNumOfBuffers() {
        final MemorySize pageSize = MemorySize.parse("32k");
        final int numOfBuffers = 1024;
        final MemorySize networkSize = pageSize.multiply(numOfBuffers);

        @SuppressWarnings("deprecation")
        final ConfigOption<Integer> legacyOption =
                NettyShuffleEnvironmentOptions.NETWORK_NUM_BUFFERS;

        Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.MEMORY_SEGMENT_SIZE, pageSize);
        conf.setInteger(legacyOption, numOfBuffers);

        // validate in configurations without explicit total flink/process memory, otherwise
        // explicit configured
        // network memory size might conflict with total flink/process memory minus other memory
        // sizes
        validateInConfigWithExplicitTaskHeapAndManagedMem(
                conf,
                taskExecutorProcessSpec ->
                        assertThat(taskExecutorProcessSpec.getNetworkMemSize())
                                .isEqualTo(networkSize));
        validateInConfigurationsWithoutExplicitTaskHeapMem(
                conf,
                taskExecutorProcessSpec ->
                        assertThat(taskExecutorProcessSpec.getNetworkMemSize())
                                .isEqualTo(networkSize));
    }

    @Test
    void testConfigManagedMemorySize() {
        final MemorySize managedMemSize = MemorySize.parse("100m");

        Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, managedMemSize);

        // validate in configurations without explicit managed memory size,
        // to avoid checking against overwritten managed memory size
        validateInConfigurationsWithoutExplicitManagedMem(
                conf,
                taskExecutorProcessSpec ->
                        assertThat(taskExecutorProcessSpec.getManagedMemorySize())
                                .isEqualTo(managedMemSize));
    }

    @Test
    void testConfigManagedMemoryLegacySize() {
        final MemorySize managedMemSize = MemorySize.parse("100m");

        @SuppressWarnings("deprecation")
        final ConfigOption<MemorySize> legacyOption = TaskManagerOptions.MANAGED_MEMORY_SIZE;

        Configuration conf = new Configuration();
        conf.set(legacyOption, managedMemSize);

        // validate in configurations without explicit managed memory size,
        // to avoid checking against overwritten managed memory size
        validateInConfigurationsWithoutExplicitManagedMem(
                conf,
                taskExecutorProcessSpec ->
                        assertThat(taskExecutorProcessSpec.getManagedMemorySize())
                                .isEqualTo(managedMemSize));
    }

    @Test
    void testConfigManagedMemoryFraction() {
        final float fraction = 0.5f;

        Configuration conf = new Configuration();
        conf.setFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION, fraction);

        // managed memory fraction is only used when managed memory size is not explicitly
        // configured
        validateInConfigurationsWithoutExplicitManagedMem(
                conf,
                taskExecutorProcessSpec ->
                        assertThat(taskExecutorProcessSpec.getManagedMemorySize())
                                .isEqualTo(
                                        taskExecutorProcessSpec
                                                .getTotalFlinkMemorySize()
                                                .multiply(fraction)));
    }

    @Test
    void testConfigManagedMemoryFractionFailure() {
        final Configuration conf = new Configuration();
        conf.setFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION, -0.1f);
        validateFailInConfigurationsWithoutExplicitManagedMem(conf);

        conf.setFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION, 1.0f);
        validateFailInConfigurationsWithoutExplicitManagedMem(conf);
    }

    @Test
    void testFlinkInternalMemorySizeAddUpFailure() {
        final MemorySize totalFlinkMemory = MemorySize.parse("499m");
        final MemorySize frameworkHeap = MemorySize.parse("100m");
        final MemorySize taskHeap = MemorySize.parse("100m");
        final MemorySize taskOffHeap = MemorySize.parse("100m");
        final MemorySize network = MemorySize.parse("100m");
        final MemorySize managed = MemorySize.parse("100m");

        Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, totalFlinkMemory);
        conf.set(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, frameworkHeap);
        conf.set(TaskManagerOptions.TASK_HEAP_MEMORY, taskHeap);
        conf.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, taskOffHeap);
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MIN, network);
        conf.set(TaskManagerOptions.NETWORK_MEMORY_MAX, network);
        conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, managed);

        validateFail(conf);
    }

    @Test
    void testFlinkInternalMemoryFractionAddUpFailure() {
        final float networkFraction = 0.6f;
        final float managedFraction = 0.6f;

        Configuration conf = new Configuration();
        conf.setFloat(TaskManagerOptions.NETWORK_MEMORY_FRACTION, networkFraction);
        conf.setFloat(TaskManagerOptions.MANAGED_MEMORY_FRACTION, managedFraction);

        // if managed memory size is explicitly configured, then managed memory fraction will be
        // ignored
        validateFailInConfigWithExplicitTotalFlinkMem(conf);
        validateFailInConfigWithExplicitTotalProcessMem(conf);
    }

    @Test
    void testConfigTotalProcessMemoryLegacySize() {
        final MemorySize totalProcessMemorySize = MemorySize.parse("2g");

        @SuppressWarnings("deprecation")
        final ConfigOption<MemorySize> legacyOption = TaskManagerOptions.TOTAL_PROCESS_MEMORY;

        Configuration conf = new Configuration();
        conf.set(legacyOption, totalProcessMemorySize);

        TaskExecutorProcessSpec taskExecutorProcessSpec =
                TaskExecutorProcessUtils.processSpecFromConfig(conf);
        assertThat(taskExecutorProcessSpec.getTotalProcessMemorySize())
                .isEqualTo(totalProcessMemorySize);
    }

    @Test
    public void testExceptionShouldContainRequiredConfigOptions() {
        assertThatThrownBy(
                        () -> TaskExecutorProcessUtils.processSpecFromConfig(new Configuration()))
                .isInstanceOf(IllegalConfigurationException.class)
                .hasMessageContaining(TaskManagerOptions.TASK_HEAP_MEMORY.key())
                .hasMessageContaining(TaskManagerOptions.MANAGED_MEMORY_SIZE.key())
                .hasMessageContaining(TaskManagerOptions.TOTAL_FLINK_MEMORY.key())
                .hasMessageContaining(TaskManagerOptions.TOTAL_PROCESS_MEMORY.key());
    }

    @Test
    void testConfigNumSlots() {
        final int numSlots = 5;

        Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.NUM_TASK_SLOTS, numSlots);

        validateInAllConfigurations(
                conf,
                taskExecutorProcessSpec ->
                        assertThat(taskExecutorProcessSpec.getNumSlots()).isEqualTo(numSlots));
    }

    @Test
    void testProcessSpecFromConfigWithExternalResource() {
        final Configuration config = new Configuration();
        config.setString(
                ExternalResourceOptions.EXTERNAL_RESOURCE_LIST.key(), EXTERNAL_RESOURCE_NAME_1);
        config.setLong(
                ExternalResourceOptions.getAmountConfigOptionForResource(EXTERNAL_RESOURCE_NAME_1),
                1);
        config.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.ofMebiBytes(4096));
        final TaskExecutorProcessSpec taskExecutorProcessSpec =
                TaskExecutorProcessUtils.processSpecFromConfig(config);
        assertThat(taskExecutorProcessSpec.getExtendedResources()).hasSize(1);
        assertThat(
                        taskExecutorProcessSpec
                                .getExtendedResources()
                                .get(EXTERNAL_RESOURCE_NAME_1)
                                .getValue()
                                .longValue())
                .isOne();
    }

    @Override
    protected void validateInAllConfigurations(
            final Configuration customConfig, Consumer<TaskExecutorProcessSpec> validateFunc) {
        validateInConfigWithExplicitTaskHeapAndManagedMem(customConfig, validateFunc);
        validateInConfigWithExplicitTotalFlinkMem(customConfig, validateFunc);
        validateInConfigWithExplicitTotalFlinkAndTaskHeapMem(customConfig, validateFunc);
        validateInConfigWithExplicitTotalFlinkAndManagedMem(customConfig, validateFunc);
        validateInConfigWithExplicitTotalProcessMem(customConfig, validateFunc);
    }

    @Override
    protected void validateFailInAllConfigurations(final Configuration customConfig) {
        validateFailInConfigWithExplicitTaskHeapAndManagedMem(customConfig);
        validateFailInConfigWithExplicitTotalFlinkMem(customConfig);
        validateFailInConfigWithExplicitTotalFlinkAndTaskHeapMem(customConfig);
        validateFailInConfigWithExplicitTotalFlinkAndManagedMem(customConfig);
        validateFailInConfigWithExplicitTotalProcessMem(customConfig);
    }

    private void validateInConfigurationsWithoutExplicitTaskHeapMem(
            final Configuration customConfig, Consumer<TaskExecutorProcessSpec> validateFunc) {
        validateInConfigWithExplicitTotalFlinkMem(customConfig, validateFunc);
        validateInConfigWithExplicitTotalFlinkAndManagedMem(customConfig, validateFunc);
        validateInConfigWithExplicitTotalProcessMem(customConfig, validateFunc);
    }

    private void validateInConfigurationsWithoutExplicitManagedMem(
            final Configuration customConfig, Consumer<TaskExecutorProcessSpec> validateFunc) {
        validateInConfigWithExplicitTotalFlinkMem(customConfig, validateFunc);
        validateInConfigWithExplicitTotalFlinkAndTaskHeapMem(customConfig, validateFunc);
        validateInConfigWithExplicitTotalProcessMem(customConfig, validateFunc);
    }

    private void validateFailInConfigurationsWithoutExplicitManagedMem(
            final Configuration customConfig) {
        validateFailInConfigWithExplicitTotalFlinkMem(customConfig);
        validateFailInConfigWithExplicitTotalFlinkAndTaskHeapMem(customConfig);
        validateFailInConfigWithExplicitTotalProcessMem(customConfig);
    }

    private void validateInConfigWithExplicitTaskHeapAndManagedMem(
            final Configuration customConfig, Consumer<TaskExecutorProcessSpec> validateFunc) {
        log.info("Validating in configuration with explicit task heap and managed memory size.");
        final Configuration config = configWithExplicitTaskHeapAndManageMem();
        config.addAll(customConfig);
        TaskExecutorProcessSpec taskExecutorProcessSpec =
                TaskExecutorProcessUtils.processSpecFromConfig(config);
        assertThat(taskExecutorProcessSpec.getTaskHeapSize()).isEqualTo(TASK_HEAP_SIZE);
        assertThat(taskExecutorProcessSpec.getManagedMemorySize()).isEqualTo(MANAGED_MEM_SIZE);
        validateFunc.accept(taskExecutorProcessSpec);
    }

    private void validateFailInConfigWithExplicitTaskHeapAndManagedMem(
            final Configuration customConfig) {
        log.info(
                "Validating failing in configuration with explicit task heap and managed memory size.");
        final Configuration config = configWithExplicitTaskHeapAndManageMem();
        config.addAll(customConfig);
        validateFail(config);
    }

    private void validateInConfigWithExplicitTotalFlinkMem(
            final Configuration customConfig, Consumer<TaskExecutorProcessSpec> validateFunc) {
        log.info("Validating in configuration with explicit total flink memory size.");
        final Configuration config = configWithExplicitTotalFlinkMem();
        config.addAll(customConfig);
        TaskExecutorProcessSpec taskExecutorProcessSpec =
                TaskExecutorProcessUtils.processSpecFromConfig(config);
        assertThat(taskExecutorProcessSpec.getTotalFlinkMemorySize())
                .isEqualTo(TOTAL_FLINK_MEM_SIZE);
        validateFunc.accept(taskExecutorProcessSpec);
    }

    private void validateFailInConfigWithExplicitTotalFlinkMem(final Configuration customConfig) {
        log.info("Validating failing in configuration with explicit total flink memory size.");
        final Configuration config = configWithExplicitTotalFlinkMem();
        config.addAll(customConfig);
        validateFail(config);
    }

    private void validateInConfigWithExplicitTotalFlinkAndTaskHeapMem(
            final Configuration customConfig, Consumer<TaskExecutorProcessSpec> validateFunc) {
        log.info(
                "Validating in configuration with explicit total flink and task heap memory size.");
        final Configuration config = configWithExplicitTotalFlinkAndTaskHeapMem();
        config.addAll(customConfig);
        TaskExecutorProcessSpec taskExecutorProcessSpec =
                TaskExecutorProcessUtils.processSpecFromConfig(config);
        assertThat(taskExecutorProcessSpec.getTotalFlinkMemorySize())
                .isEqualTo(TOTAL_FLINK_MEM_SIZE);
        assertThat(taskExecutorProcessSpec.getTaskHeapSize()).isEqualTo(TASK_HEAP_SIZE);
        validateFunc.accept(taskExecutorProcessSpec);
    }

    private void validateFailInConfigWithExplicitTotalFlinkAndTaskHeapMem(
            final Configuration customConfig) {
        log.info(
                "Validating failing in configuration with explicit total flink and task heap memory size.");
        final Configuration config = configWithExplicitTotalFlinkAndTaskHeapMem();
        config.addAll(customConfig);
        validateFail(config);
    }

    private void validateInConfigWithExplicitTotalFlinkAndManagedMem(
            final Configuration customConfig, Consumer<TaskExecutorProcessSpec> validateFunc) {
        log.info("Validating in configuration with explicit total flink and managed memory size.");
        final Configuration config = configWithExplicitTotalFlinkAndManagedMem();
        config.addAll(customConfig);
        TaskExecutorProcessSpec taskExecutorProcessSpec =
                TaskExecutorProcessUtils.processSpecFromConfig(config);
        assertThat(taskExecutorProcessSpec.getTotalFlinkMemorySize())
                .isEqualTo(TOTAL_FLINK_MEM_SIZE);
        assertThat(taskExecutorProcessSpec.getManagedMemorySize()).isEqualTo(MANAGED_MEM_SIZE);
        validateFunc.accept(taskExecutorProcessSpec);
    }

    private void validateFailInConfigWithExplicitTotalFlinkAndManagedMem(
            final Configuration customConfig) {
        log.info(
                "Validating failing in configuration with explicit total flink and managed memory size.");
        final Configuration config = configWithExplicitTotalFlinkAndManagedMem();
        config.addAll(customConfig);
        validateFail(config);
    }

    private void validateInConfigWithExplicitTotalProcessMem(
            final Configuration customConfig, Consumer<TaskExecutorProcessSpec> validateFunc) {
        log.info("Validating in configuration with explicit total process memory size.");
        final Configuration config = configWithExplicitTotalProcessMem();
        config.addAll(customConfig);
        TaskExecutorProcessSpec taskExecutorProcessSpec =
                TaskExecutorProcessUtils.processSpecFromConfig(config);
        assertThat(taskExecutorProcessSpec.getTotalProcessMemorySize())
                .isEqualTo(TOTAL_PROCESS_MEM_SIZE);
        validateFunc.accept(taskExecutorProcessSpec);
    }

    private void validateFailInConfigWithExplicitTotalProcessMem(final Configuration customConfig) {
        log.info("Validating failing in configuration with explicit total process memory size.");
        final Configuration config = configWithExplicitTotalProcessMem();
        config.addAll(customConfig);
        validateFail(config);
    }

    @Override
    protected void validateFail(final Configuration config) {
        assertThatExceptionOfType(IllegalConfigurationException.class)
                .as("Configuration did not fail as expected.")
                .isThrownBy(() -> TaskExecutorProcessUtils.processSpecFromConfig(config));
    }

    @Override
    protected void configWithFineGrainedOptions(
            Configuration configuration, MemorySize totalFlinkMemorySize) {
        MemorySize componentSize = new MemorySize(totalFlinkMemorySize.getBytes() / 6);
        configuration.set(TaskManagerOptions.TASK_HEAP_MEMORY, componentSize);
        configuration.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, componentSize);
        configuration.set(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, componentSize);
        configuration.set(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY, componentSize);
        configuration.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, componentSize);
        // network is the 6th component, fixed implicitly
    }

    private static Configuration configWithExplicitTaskHeapAndManageMem() {
        final Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.TASK_HEAP_MEMORY, TASK_HEAP_SIZE);
        conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MANAGED_MEM_SIZE);
        return conf;
    }

    private static Configuration configWithExplicitTotalFlinkMem() {
        final Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, TOTAL_FLINK_MEM_SIZE);
        return conf;
    }

    private static Configuration configWithExplicitTotalFlinkAndTaskHeapMem() {
        final Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, TOTAL_FLINK_MEM_SIZE);
        conf.set(TaskManagerOptions.TASK_HEAP_MEMORY, TASK_HEAP_SIZE);
        return conf;
    }

    private static Configuration configWithExplicitTotalFlinkAndManagedMem() {
        final Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, TOTAL_FLINK_MEM_SIZE);
        conf.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MANAGED_MEM_SIZE);
        return conf;
    }

    private static Configuration configWithExplicitTotalProcessMem() {
        final Configuration conf = new Configuration();
        conf.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, TOTAL_PROCESS_MEM_SIZE);
        return conf;
    }

    @Override
    protected TaskExecutorProcessSpec processSpecFromConfig(Configuration config) {
        return TaskExecutorProcessUtils.processSpecFromConfig(config);
    }

    @Override
    protected Configuration getConfigurationWithLegacyHeapSizeMappedToNewConfigOption(
            Configuration config) {
        return TaskExecutorProcessUtils.getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
                config, getNewOptionForLegacyHeapOption());
    }
}
