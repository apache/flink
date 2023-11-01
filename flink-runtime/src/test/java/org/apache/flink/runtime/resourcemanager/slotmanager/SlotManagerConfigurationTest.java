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
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SlotManagerConfiguration}. */
class SlotManagerConfigurationTest {

    /**
     * Tests that {@link SlotManagerConfiguration#getSlotRequestTimeout()} returns the value
     * configured under key {@link JobManagerOptions#SLOT_REQUEST_TIMEOUT}.
     */
    @Test
    void testSetSlotRequestTimeout() throws Exception {
        final long slotIdleTimeout = 42;

        final Configuration configuration = new Configuration();
        configuration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, slotIdleTimeout);
        final SlotManagerConfiguration slotManagerConfiguration =
                SlotManagerConfiguration.fromConfiguration(configuration, WorkerResourceSpec.ZERO);

        assertThat(slotIdleTimeout)
                .isEqualTo(slotManagerConfiguration.getSlotRequestTimeout().toMilliseconds());
    }

    /**
     * Tests that {@link ResourceManagerOptions#SLOT_REQUEST_TIMEOUT} is preferred over {@link
     * JobManagerOptions#SLOT_REQUEST_TIMEOUT} if set.
     */
    @Test
    void testPreferLegacySlotRequestTimeout() throws Exception {
        final long legacySlotIdleTimeout = 42;

        final Configuration configuration = new Configuration();
        configuration.setLong(ResourceManagerOptions.SLOT_REQUEST_TIMEOUT, legacySlotIdleTimeout);
        configuration.setLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT, 300000L);
        final SlotManagerConfiguration slotManagerConfiguration =
                SlotManagerConfiguration.fromConfiguration(configuration, WorkerResourceSpec.ZERO);

        assertThat(legacySlotIdleTimeout)
                .isEqualTo(slotManagerConfiguration.getSlotRequestTimeout().toMilliseconds());
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
}
