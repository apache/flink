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

/** Tests for {@link FreeSlotInfoTracker}. */
class DefaultFreeSlotInfoTrackerTest {

    @Test
    void testReserveSlot() {
        final ResourceID resourceId = ResourceID.generate();
        final SlotInfo slotInfo1 = createAllocatedSlot(resourceId);
        final SlotInfo slotInfo2 = createAllocatedSlot(resourceId);
        final Map<AllocationID, SlotInfo> slots = new HashMap<>();

        slots.put(slotInfo1.getAllocationId(), slotInfo1);
        slots.put(slotInfo2.getAllocationId(), slotInfo2);

        final FreeSlotInfoTracker freeSlotInfoTracker =
                FreeSlotInfoTrackerTestUtils.createDefaultFreeSlotInfoTracker(slots);
        for (AllocationID candidate : freeSlotInfoTracker.getAvailableSlots()) {
            SlotInfo selectSlot = freeSlotInfoTracker.getSlotInfo(candidate);
            assertThat(slots.get(selectSlot.getAllocationId())).isEqualTo(selectSlot);
            freeSlotInfoTracker.reserveSlot(selectSlot.getAllocationId());
            break;
        }

        assertThat(freeSlotInfoTracker.getAvailableSlots())
                .hasSize(1)
                .containsAnyOf(slotInfo1.getAllocationId(), slotInfo2.getAllocationId());
    }

    @Test
    void testCreatedFreeSlotInfoTrackerWithoutBlockedSlots() {
        final ResourceID resourceId = ResourceID.generate();
        final SlotInfo slotInfo1 = createAllocatedSlot(resourceId);
        final SlotInfo slotInfo2 = createAllocatedSlot(resourceId);
        final Map<AllocationID, SlotInfo> slots = new HashMap<>();

        slots.put(slotInfo1.getAllocationId(), slotInfo1);
        slots.put(slotInfo2.getAllocationId(), slotInfo2);

        final FreeSlotInfoTracker freeSlotInfoTracker =
                FreeSlotInfoTrackerTestUtils.createDefaultFreeSlotInfoTracker(slots);
        assertThat(freeSlotInfoTracker.getAvailableSlots()).hasSize(2);

        final FreeSlotInfoTracker freeSlotInfoTrackerWithoutBlockedSlots =
                freeSlotInfoTracker.createNewFreeSlotInfoTrackerWithoutBlockedSlots(
                        new HashSet<>(
                                Arrays.asList(
                                        slotInfo1.getAllocationId(), slotInfo2.getAllocationId())));
        assertThat(freeSlotInfoTrackerWithoutBlockedSlots.getAvailableSlots()).isEmpty();
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
