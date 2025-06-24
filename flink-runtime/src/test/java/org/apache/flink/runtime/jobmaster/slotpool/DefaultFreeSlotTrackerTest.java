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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultFreeSlotTracker}. */
class DefaultFreeSlotTrackerTest {

    @Test
    void testReserveSlot() {
        final ResourceID resourceId = ResourceID.generate();
        final PhysicalSlot slot1 = createAllocatedSlot(resourceId);
        final PhysicalSlot slot2 = createAllocatedSlot(resourceId);
        final Map<AllocationID, PhysicalSlot> slots = new HashMap<>();

        slots.put(slot1.getAllocationId(), slot1);
        slots.put(slot2.getAllocationId(), slot2);

        final FreeSlotTracker freeSlotTracker =
                FreeSlotTrackerTestUtils.createDefaultFreeSlotTracker(slots);
        for (AllocationID candidate : freeSlotTracker.getAvailableSlots()) {
            SlotInfo selectSlot = freeSlotTracker.getSlotInfo(candidate);
            assertThat(slots.get(selectSlot.getAllocationId())).isEqualTo(selectSlot);
            freeSlotTracker.reserveSlot(selectSlot.getAllocationId());
            break;
        }

        assertThat(freeSlotTracker.getAvailableSlots())
                .hasSize(1)
                .containsAnyOf(slot1.getAllocationId(), slot2.getAllocationId());
    }

    @Test
    void testCreatedFreeSlotTrackerWithoutBlockedSlots() {
        final ResourceID resourceId = ResourceID.generate();
        final PhysicalSlot slot1 = createAllocatedSlot(resourceId);
        final PhysicalSlot slot2 = createAllocatedSlot(resourceId);
        final Map<AllocationID, PhysicalSlot> slots = new HashMap<>();

        slots.put(slot1.getAllocationId(), slot1);
        slots.put(slot2.getAllocationId(), slot2);

        final FreeSlotTracker freeSlotTracker =
                FreeSlotTrackerTestUtils.createDefaultFreeSlotTracker(slots);
        assertThat(freeSlotTracker.getAvailableSlots()).hasSize(2);

        final FreeSlotTracker freeSlotTrackerWithoutBlockedSlots =
                freeSlotTracker.createNewFreeSlotTrackerWithoutBlockedSlots(
                        new HashSet<>(
                                Arrays.asList(slot1.getAllocationId(), slot2.getAllocationId())));
        assertThat(freeSlotTrackerWithoutBlockedSlots.getAvailableSlots()).isEmpty();
    }

    private AllocatedSlot createAllocatedSlot(ResourceID owner) {
        return new AllocatedSlot(
                new AllocationID(),
                new TaskManagerLocation(owner, InetAddress.getLoopbackAddress(), 41),
                0,
                ResourceProfile.UNKNOWN,
                new RpcTaskManagerGateway(
                        new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway(),
                        JobMasterId.generate()));
    }
}
