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

package org.apache.flink.api.common;

import org.apache.flink.configuration.MemorySize;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SlotSharingGroup}. */
class SlotSharingGroupTest {
    @Test
    void testBuildSlotSharingGroupWithSpecificResource() {
        final String name = "ssg";
        final MemorySize heap = MemorySize.ofMebiBytes(100);
        final MemorySize offHeap = MemorySize.ofMebiBytes(200);
        final MemorySize managed = MemorySize.ofMebiBytes(300);
        final SlotSharingGroup slotSharingGroup =
                SlotSharingGroup.newBuilder(name)
                        .setCpuCores(1)
                        .setTaskHeapMemory(heap)
                        .setTaskOffHeapMemory(offHeap)
                        .setManagedMemory(managed)
                        .setExternalResource("gpu", 1)
                        .build();

        assertThat(slotSharingGroup.getName()).isEqualTo(name);
        assertThat(slotSharingGroup.getCpuCores()).isEqualTo(1.0);
        assertThat(slotSharingGroup.getTaskHeapMemory()).isEqualTo(heap);
        assertThat(slotSharingGroup.getTaskOffHeapMemory()).isEqualTo(offHeap);
        assertThat(slotSharingGroup.getManagedMemory()).isEqualTo(managed);
        assertThat(slotSharingGroup.getExternalResources())
                .isEqualTo(Collections.singletonMap("gpu", 1.0));
    }

    @Test
    void testBuildSlotSharingGroupWithUnknownResource() {
        final String name = "ssg";
        final SlotSharingGroup slotSharingGroup = SlotSharingGroup.newBuilder(name).build();

        assertThat(slotSharingGroup.getName()).isEqualTo(name);
        assertThat(slotSharingGroup.getCpuCores()).isNull();
        assertThat(slotSharingGroup.getTaskHeapMemory()).isNull();
        assertThat(slotSharingGroup.getManagedMemory()).isNull();
        assertThat(slotSharingGroup.getTaskOffHeapMemory()).isNull();
        assertThat(slotSharingGroup.getExternalResources()).isEmpty();
    }

    @Test
    void testBuildSlotSharingGroupWithIllegalConfig() {
        assertThatThrownBy(
                        () ->
                                SlotSharingGroup.newBuilder("ssg")
                                        .setCpuCores(1)
                                        .setTaskHeapMemory(MemorySize.ZERO)
                                        .setTaskOffHeapMemoryMB(10)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testBuildSlotSharingGroupWithoutAllRequiredConfig() {
        assertThatThrownBy(
                        () ->
                                SlotSharingGroup.newBuilder("ssg")
                                        .setCpuCores(1)
                                        .setTaskOffHeapMemoryMB(10)
                                        .build())
                .isInstanceOf(IllegalArgumentException.class);
    }
}
