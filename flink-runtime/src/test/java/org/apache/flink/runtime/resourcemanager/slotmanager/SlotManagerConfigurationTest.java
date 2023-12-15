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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

/** Tests for {@link SlotManagerConfiguration}. */
class SlotManagerConfigurationTest {
    @Test
    void testComputeMinTotalCpu() throws Exception {
        final Configuration configuration = new Configuration();
        final int minSlotNum = 9;
        final int numSlots = 3;
        final double cpuCores = 10;
        configuration.set(ResourceManagerOptions.MIN_SLOT_NUM, minSlotNum);
        final SlotManagerConfiguration slotManagerConfiguration =
                SlotManagerConfiguration.fromConfiguration(
                        configuration,
                        new WorkerResourceSpec.Builder()
                                .setNumSlots(numSlots)
                                .setCpuCores(cpuCores)
                                .build());
        assertThat(slotManagerConfiguration.getMinTotalCpu().getValue().doubleValue())
                .isEqualTo(cpuCores * minSlotNum / numSlots);
    }

    @Test
    void testComputeMaxTotalCpu() throws Exception {
        final Configuration configuration = new Configuration();
        final int maxSlotNum = 9;
        final int numSlots = 3;
        final double cpuCores = 10;
        configuration.set(ResourceManagerOptions.MAX_SLOT_NUM, maxSlotNum);
        final SlotManagerConfiguration slotManagerConfiguration =
                SlotManagerConfiguration.fromConfiguration(
                        configuration,
                        new WorkerResourceSpec.Builder()
                                .setNumSlots(numSlots)
                                .setCpuCores(cpuCores)
                                .build());
        assertThat(slotManagerConfiguration.getMaxTotalCpu().getValue().doubleValue())
                .isEqualTo(cpuCores * maxSlotNum / numSlots);
    }

    @Test
    void testComputeMinTotalMemory() throws Exception {
        final Configuration configuration = new Configuration();
        final int minSlotNum = 1000;
        final int numSlots = 10;
        final int totalTaskManagerMB =
                MemorySize.parse("1", MemorySize.MemoryUnit.TERA_BYTES).getMebiBytes();
        configuration.set(ResourceManagerOptions.MIN_SLOT_NUM, minSlotNum);
        final SlotManagerConfiguration slotManagerConfiguration =
                SlotManagerConfiguration.fromConfiguration(
                        configuration,
                        new WorkerResourceSpec.Builder()
                                .setNumSlots(numSlots)
                                .setTaskHeapMemoryMB(totalTaskManagerMB)
                                .build());
        assertThat(slotManagerConfiguration.getMinTotalMem().getBytes())
                .isEqualTo(
                        BigDecimal.valueOf(MemorySize.ofMebiBytes(totalTaskManagerMB).getBytes())
                                .multiply(BigDecimal.valueOf(minSlotNum))
                                .divide(BigDecimal.valueOf(numSlots))
                                .longValue());
    }

    @Test
    void testComputeMaxTotalMemory() throws Exception {
        final Configuration configuration = new Configuration();
        final int maxSlotNum = 1_000_000;
        final int numSlots = 10;
        final int totalTaskManagerMB =
                MemorySize.parse("1", MemorySize.MemoryUnit.TERA_BYTES).getMebiBytes();
        configuration.set(ResourceManagerOptions.MAX_SLOT_NUM, maxSlotNum);
        final SlotManagerConfiguration slotManagerConfiguration =
                SlotManagerConfiguration.fromConfiguration(
                        configuration,
                        new WorkerResourceSpec.Builder()
                                .setNumSlots(numSlots)
                                .setTaskHeapMemoryMB(totalTaskManagerMB)
                                .build());
        assertThat(slotManagerConfiguration.getMaxTotalMem().getBytes())
                .isEqualTo(
                        BigDecimal.valueOf(MemorySize.ofMebiBytes(totalTaskManagerMB).getBytes())
                                .multiply(BigDecimal.valueOf(maxSlotNum))
                                .divide(BigDecimal.valueOf(numSlots))
                                .longValue());
    }

    @Test
    void testComputeMinMaxSlotNumIsValid() throws Exception {
        final Configuration configuration = new Configuration();
        final int minSlotNum = 9;
        final int maxSlotNum = 12;
        final int numSlots = 3;
        final double cpuCores = 10;
        configuration.set(ResourceManagerOptions.MIN_SLOT_NUM, minSlotNum);
        configuration.set(ResourceManagerOptions.MAX_SLOT_NUM, maxSlotNum);

        final SlotManagerConfiguration slotManagerConfiguration =
                SlotManagerConfiguration.fromConfiguration(
                        configuration,
                        new WorkerResourceSpec.Builder()
                                .setNumSlots(numSlots)
                                .setCpuCores(cpuCores)
                                .build());
        assertThat(slotManagerConfiguration.getMinTotalCpu().getValue().doubleValue())
                .isEqualTo(cpuCores * minSlotNum / numSlots);
        assertThat(slotManagerConfiguration.getMaxTotalCpu().getValue().doubleValue())
                .isEqualTo(cpuCores * maxSlotNum / numSlots);
    }

    @Test
    void testComputeMinMaxSlotNumIsInvalid() {
        final Configuration configuration = new Configuration();
        final int minSlotNum = 10;
        final int maxSlotNum = 11;
        final int numSlots = 3;
        configuration.set(ResourceManagerOptions.MIN_SLOT_NUM, minSlotNum);
        configuration.set(ResourceManagerOptions.MAX_SLOT_NUM, maxSlotNum);

        assertThatIllegalStateException()
                .isThrownBy(
                        () ->
                                SlotManagerConfiguration.fromConfiguration(
                                        configuration,
                                        new WorkerResourceSpec.Builder()
                                                .setNumSlots(numSlots)
                                                .build()));
    }

    @Test
    void testComputeMinMaxCpuIsInvalid() {
        final Configuration configuration = new Configuration();
        final double minTotalCpu = 10.0;
        final double maxTotalCpu = 11.0;
        final int numSlots = 3;
        final double cpuCores = 3;
        configuration.set(ResourceManagerOptions.MIN_TOTAL_CPU, minTotalCpu);
        configuration.set(ResourceManagerOptions.MAX_TOTAL_CPU, maxTotalCpu);

        assertThatIllegalStateException()
                .isThrownBy(
                        () ->
                                SlotManagerConfiguration.fromConfiguration(
                                        configuration,
                                        new WorkerResourceSpec.Builder()
                                                .setNumSlots(numSlots)
                                                .setCpuCores(cpuCores)
                                                .build()));
    }

    @Test
    void testComputeMinMaxMemoryIsInvalid() {
        final Configuration configuration = new Configuration();
        final MemorySize minMemorySize = MemorySize.ofMebiBytes(500);
        final MemorySize maxMemorySize = MemorySize.ofMebiBytes(700);
        final int numSlots = 3;
        configuration.set(ResourceManagerOptions.MIN_TOTAL_MEM, minMemorySize);
        configuration.set(ResourceManagerOptions.MAX_TOTAL_MEM, maxMemorySize);

        assertThatIllegalStateException()
                .isThrownBy(
                        () ->
                                SlotManagerConfiguration.fromConfiguration(
                                        configuration,
                                        new WorkerResourceSpec.Builder()
                                                .setNumSlots(numSlots)
                                                .setTaskHeapMemoryMB(100)
                                                .setManagedMemoryMB(100)
                                                .setNetworkMemoryMB(100)
                                                .setTaskOffHeapMemoryMB(100)
                                                .build()));
    }
}
