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
import org.apache.flink.configuration.ExternalResourceOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Test suite for {@link TaskExecutorResourceUtils}. */
public class TaskExecutorResourceUtilsTest extends TestLogger {
    private static final double CPU_CORES = 1.0;
    private static final MemorySize TASK_HEAP = MemorySize.ofMebiBytes(1);
    private static final MemorySize TASK_OFF_HEAP = MemorySize.ofMebiBytes(2);
    private static final MemorySize NETWORK = MemorySize.ofMebiBytes(3);
    private static final MemorySize MANAGED = MemorySize.ofMebiBytes(4);
    private static final String EXTERNAL_RESOURCE_NAME = "test";
    private static final long EXTERNAL_RESOURCE_AMOUNT = 1;

    @Test
    public void testResourceSpecFromConfig() {
        TaskExecutorResourceSpec resourceSpec =
                TaskExecutorResourceUtils.resourceSpecFromConfig(createValidConfig());
        assertThat(resourceSpec.getCpuCores(), is(new CPUResource(CPU_CORES)));
        assertThat(resourceSpec.getTaskHeapSize(), is(TASK_HEAP));
        assertThat(resourceSpec.getTaskOffHeapSize(), is(TASK_OFF_HEAP));
        assertThat(resourceSpec.getNetworkMemSize(), is(NETWORK));
        assertThat(resourceSpec.getManagedMemorySize(), is(MANAGED));
        assertThat(
                resourceSpec
                        .getExtendedResources()
                        .get(EXTERNAL_RESOURCE_NAME)
                        .getValue()
                        .longValue(),
                is(EXTERNAL_RESOURCE_AMOUNT));
    }

    @Test(expected = IllegalConfigurationException.class)
    public void testResourceSpecFromConfigFailsIfNetworkSizeIsNotFixed() {
        Configuration configuration = createValidConfig();
        configuration.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.ofMebiBytes(1));
        configuration.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.ofMebiBytes(2));
        TaskExecutorResourceUtils.resourceSpecFromConfig(configuration);
    }

    @Test
    public void testResourceSpecFromConfigFailsIfRequiredOptionIsNotSet() {
        TaskExecutorResourceUtils.CONFIG_OPTIONS.stream()
                .filter(option -> !option.hasDefaultValue())
                .forEach(
                        option -> {
                            try {
                                TaskExecutorResourceUtils.resourceSpecFromConfig(
                                        setAllRequiredOptionsExceptOne(option));
                                fail(
                                        "should fail with "
                                                + IllegalConfigurationException.class
                                                        .getSimpleName());
                            } catch (IllegalConfigurationException e) {
                                // expected
                            }
                        });
    }

    @Test
    public void testAdjustForLocalExecution() {
        Configuration configuration =
                TaskExecutorResourceUtils.adjustForLocalExecution(new Configuration());

        assertThat(
                configuration.get(TaskManagerOptions.NETWORK_MEMORY_MIN),
                is(TaskExecutorResourceUtils.DEFAULT_SHUFFLE_MEMORY_SIZE));
        assertThat(
                configuration.get(TaskManagerOptions.NETWORK_MEMORY_MAX),
                is(TaskExecutorResourceUtils.DEFAULT_SHUFFLE_MEMORY_SIZE));
        assertThat(
                configuration.get(TaskManagerOptions.MANAGED_MEMORY_SIZE),
                is(TaskExecutorResourceUtils.DEFAULT_MANAGED_MEMORY_SIZE));
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

    private static Configuration createValidConfig() {
        Configuration configuration = new Configuration();
        configuration.set(TaskManagerOptions.CPU_CORES, CPU_CORES);
        configuration.set(TaskManagerOptions.TASK_HEAP_MEMORY, TASK_HEAP);
        configuration.set(TaskManagerOptions.TASK_OFF_HEAP_MEMORY, TASK_OFF_HEAP);
        configuration.set(TaskManagerOptions.NETWORK_MEMORY_MIN, NETWORK);
        configuration.set(TaskManagerOptions.NETWORK_MEMORY_MAX, NETWORK);
        configuration.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MANAGED);
        configuration.setString(
                ExternalResourceOptions.EXTERNAL_RESOURCE_LIST.key(), EXTERNAL_RESOURCE_NAME);
        configuration.setString(
                ExternalResourceOptions.getAmountConfigOptionForResource(EXTERNAL_RESOURCE_NAME),
                String.valueOf(EXTERNAL_RESOURCE_AMOUNT));
        return configuration;
    }

    private static Configuration setAllRequiredOptionsExceptOne(ConfigOption<?> optionToNotSet) {
        Configuration configuration = new Configuration();
        if (!TaskManagerOptions.CPU_CORES.equals(optionToNotSet)) {
            configuration.set(TaskManagerOptions.CPU_CORES, 1.0);
        }

        // skip network to exclude min/max mismatch config failure
        MemorySize network = MemorySize.ofMebiBytes(3);
        configuration.set(TaskManagerOptions.NETWORK_MEMORY_MIN, network);
        configuration.set(TaskManagerOptions.NETWORK_MEMORY_MAX, network);

        //noinspection unchecked
        TaskExecutorResourceUtils.CONFIG_OPTIONS.stream()
                .filter(option -> !option.equals(TaskManagerOptions.CPU_CORES))
                .filter(option -> !option.equals(optionToNotSet))
                .forEach(
                        option ->
                                configuration.set(
                                        (ConfigOption<MemorySize>) option,
                                        MemorySize.ofMebiBytes(1)));

        return configuration;
    }
}
