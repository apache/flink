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

package org.apache.flink.api.common.operators;

import org.apache.flink.configuration.MemorySize;

import org.junit.Test;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for {@link SlotSharingGroup}. */
public class SlotSharingGroupTest {
    @Test
    public void testBuildSlotSharingGroupWithSpecificResource() {
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

        assertThat(slotSharingGroup.getName(), is(name));
        assertThat(slotSharingGroup.getCpuCores().get(), is(1.0));
        assertThat(slotSharingGroup.getTaskHeapMemory().get(), is(heap));
        assertThat(slotSharingGroup.getTaskOffHeapMemory().get(), is(offHeap));
        assertThat(slotSharingGroup.getManagedMemory().get(), is(managed));
        assertThat(
                slotSharingGroup.getExternalResources(), is(Collections.singletonMap("gpu", 1.0)));
    }

    @Test
    public void testBuildSlotSharingGroupWithUnknownResource() {
        final String name = "ssg";
        final SlotSharingGroup slotSharingGroup = SlotSharingGroup.newBuilder(name).build();

        assertThat(slotSharingGroup.getName(), is(name));
        assertFalse(slotSharingGroup.getCpuCores().isPresent());
        assertFalse(slotSharingGroup.getTaskHeapMemory().isPresent());
        assertFalse(slotSharingGroup.getManagedMemory().isPresent());
        assertFalse(slotSharingGroup.getTaskOffHeapMemory().isPresent());
        assertTrue(slotSharingGroup.getExternalResources().isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildSlotSharingGroupWithIllegalConfig() {
        SlotSharingGroup.newBuilder("ssg")
                .setCpuCores(1)
                .setTaskHeapMemory(MemorySize.ZERO)
                .setTaskOffHeapMemoryMB(10)
                .build();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildSlotSharingGroupWithoutAllRequiredConfig() {
        SlotSharingGroup.newBuilder("ssg").setCpuCores(1).setTaskOffHeapMemoryMB(10).build();
    }
}
