/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.resources.ExternalResource;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceSpec;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link SlotManagerUtils}. */
public class SlotManagerUtilsTest extends TestLogger {
    private static final String EXTERNAL_RESOURCE_NAME = "gpu";

    @Test
    public void testGenerateDefaultSlotProfileFromWorkerResourceSpec() {
        final int numSlots = 5;
        final ResourceProfile resourceProfile =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(1)
                        .setTaskOffHeapMemoryMB(2)
                        .setNetworkMemoryMB(3)
                        .setManagedMemoryMB(4)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec workerResourceSpec =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0 * numSlots)
                        .setTaskHeapMemoryMB(1 * numSlots)
                        .setTaskOffHeapMemoryMB(2 * numSlots)
                        .setNetworkMemoryMB(3 * numSlots)
                        .setManagedMemoryMB(4 * numSlots)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, numSlots))
                        .build();

        assertThat(
                SlotManagerUtils.generateDefaultSlotResourceProfile(workerResourceSpec, numSlots),
                is(resourceProfile));
    }

    @Test
    public void testGenerateDefaultSlotProfileFromTotalResourceProfile() {
        final int numSlots = 5;
        final ResourceProfile resourceProfile =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(1)
                        .setTaskOffHeapMemoryMB(2)
                        .setNetworkMemoryMB(3)
                        .setManagedMemoryMB(4)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final ResourceProfile totalResourceProfile =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0 * numSlots)
                        .setTaskHeapMemoryMB(1 * numSlots)
                        .setTaskOffHeapMemoryMB(2 * numSlots)
                        .setNetworkMemoryMB(3 * numSlots)
                        .setManagedMemoryMB(4 * numSlots)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, numSlots))
                        .build();

        assertThat(
                SlotManagerUtils.generateDefaultSlotResourceProfile(totalResourceProfile, numSlots),
                is(resourceProfile));
    }

    @Test
    public void testGenerateDefaultSlotConsistentWithTaskExecutorResourceUtils() {
        final int numSlots = 5;
        final TaskExecutorResourceSpec taskExecutorResourceSpec =
                new TaskExecutorResourceSpec(
                        new CPUResource(1.0),
                        MemorySize.parse("1m"),
                        MemorySize.parse("2m"),
                        MemorySize.parse("3m"),
                        MemorySize.parse("4m"),
                        Collections.singleton(
                                new ExternalResource(EXTERNAL_RESOURCE_NAME, numSlots)));

        final ResourceProfile resourceProfileFromTaskExecutorResourceUtils =
                TaskExecutorResourceUtils.generateDefaultSlotResourceProfile(
                        taskExecutorResourceSpec, numSlots);

        final ResourceProfile totalResourceProfile =
                TaskExecutorResourceUtils.generateTotalAvailableResourceProfile(
                        taskExecutorResourceSpec);
        final WorkerResourceSpec workerResourceSpec =
                WorkerResourceSpec.fromTotalResourceProfile(totalResourceProfile, numSlots);

        assertThat(
                SlotManagerUtils.generateDefaultSlotResourceProfile(totalResourceProfile, numSlots),
                is(resourceProfileFromTaskExecutorResourceUtils));
        assertThat(
                SlotManagerUtils.generateDefaultSlotResourceProfile(workerResourceSpec, numSlots),
                is(resourceProfileFromTaskExecutorResourceUtils));
    }

    @Test
    public void testCalculateDefaultNumSlots() {
        final ResourceProfile defaultSlotResource =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(1)
                        .setTaskOffHeapMemoryMB(2)
                        .setNetworkMemoryMB(3)
                        .setManagedMemoryMB(4)
                        .build();
        final ResourceProfile totalResource1 = defaultSlotResource.multiply(5);
        final ResourceProfile totalResource2 =
                totalResource1.merge(ResourceProfile.newBuilder().setCpuCores(0.1).build());

        assertThat(
                SlotManagerUtils.calculateDefaultNumSlots(totalResource1, defaultSlotResource),
                is(5));
        assertThat(
                SlotManagerUtils.calculateDefaultNumSlots(totalResource2, defaultSlotResource),
                is(5));
        // For ResourceProfile.ANY in test case, return the maximum integer
        assertThat(
                SlotManagerUtils.calculateDefaultNumSlots(ResourceProfile.ANY, defaultSlotResource),
                is(Integer.MAX_VALUE));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCalculateDefaultNumSlotsFailZeroDefaultSlotProfile() {
        SlotManagerUtils.calculateDefaultNumSlots(
                ResourceProfile.fromResources(1.0, 1), ResourceProfile.ZERO);
    }

    @Test
    public void testGetEffectiveResourceProfile() {
        final ResourceProfile defaultProfile = ResourceProfile.fromResources(5, 10);
        final ResourceProfile concreteRequirement = ResourceProfile.fromResources(1, 20);

        assertThat(
                SlotManagerUtils.getEffectiveResourceProfile(
                        ResourceProfile.UNKNOWN, defaultProfile),
                is(defaultProfile));
        assertThat(
                SlotManagerUtils.getEffectiveResourceProfile(concreteRequirement, defaultProfile),
                is(concreteRequirement));
    }

    @Test
    public void testGenerateTaskManagerTotalResourceProfile() {
        final ResourceProfile resourceProfile =
                ResourceProfile.newBuilder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(1)
                        .setTaskOffHeapMemoryMB(2)
                        .setNetworkMemoryMB(3)
                        .setManagedMemoryMB(4)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();
        final WorkerResourceSpec workerResourceSpec =
                new WorkerResourceSpec.Builder()
                        .setCpuCores(1.0)
                        .setTaskHeapMemoryMB(1)
                        .setTaskOffHeapMemoryMB(2)
                        .setNetworkMemoryMB(3)
                        .setManagedMemoryMB(4)
                        .setExtendedResource(new ExternalResource(EXTERNAL_RESOURCE_NAME, 1))
                        .build();

        assertThat(
                SlotManagerUtils.generateTaskManagerTotalResourceProfile(workerResourceSpec),
                equalTo(resourceProfile));
    }
}
