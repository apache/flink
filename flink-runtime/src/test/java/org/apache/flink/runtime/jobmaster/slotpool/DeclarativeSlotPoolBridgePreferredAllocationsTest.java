/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.clock.SystemClock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(TestLoggerExtension.class)
public class DeclarativeSlotPoolBridgePreferredAllocationsTest {

    @Test
    public void testDeclarativeSlotPoolTakesPreferredAllocationsIntoAccount() throws Exception {
        final DeclarativeSlotPoolBridge declarativeSlotPoolBridge =
                new DeclarativeSlotPoolBridge(
                        new JobID(),
                        new DefaultDeclarativeSlotPoolFactory(),
                        SystemClock.getInstance(),
                        TestingUtils.infiniteTime(),
                        TestingUtils.infiniteTime(),
                        TestingUtils.infiniteTime(),
                        PreferredAllocationRequestSlotMatchingStrategy.INSTANCE);

        declarativeSlotPoolBridge.start(
                JobMasterId.generate(),
                "localhost",
                ComponentMainThreadExecutorServiceAdapter.forMainThread());

        final LocalTaskManagerLocation localTaskManagerLocation = new LocalTaskManagerLocation();

        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();

        final CompletableFuture<PhysicalSlot> slotRequestWithPreferredAllocation1 =
                requestSlot(declarativeSlotPoolBridge, Collections.singleton(allocationId1));
        final CompletableFuture<PhysicalSlot> slotRequestWithEmptyPreferredAllocations =
                requestSlot(declarativeSlotPoolBridge, Collections.emptySet());
        final CompletableFuture<PhysicalSlot> slotRequestWithPreferredAllocation2 =
                requestSlot(declarativeSlotPoolBridge, Collections.singleton(allocationId2));

        final Collection<SlotOffer> slotOffers = new ArrayList<>();
        slotOffers.add(new SlotOffer(allocationId2, 0, ResourceProfile.ANY));
        final AllocationID otherAllocationId = new AllocationID();
        slotOffers.add(new SlotOffer(otherAllocationId, 1, ResourceProfile.ANY));
        slotOffers.add(new SlotOffer(allocationId1, 2, ResourceProfile.ANY));

        declarativeSlotPoolBridge.registerTaskManager(localTaskManagerLocation.getResourceID());
        declarativeSlotPoolBridge.offerSlots(
                localTaskManagerLocation, new SimpleAckingTaskManagerGateway(), slotOffers);

        assertThat(slotRequestWithPreferredAllocation1.join().getAllocationId())
                .isEqualTo(allocationId1);
        assertThat(slotRequestWithPreferredAllocation2.join().getAllocationId())
                .isEqualTo(allocationId2);
        assertThat(slotRequestWithEmptyPreferredAllocations.join().getAllocationId())
                .isEqualTo(otherAllocationId);
    }

    @Nonnull
    private CompletableFuture<PhysicalSlot> requestSlot(
            DeclarativeSlotPoolBridge declarativeSlotPoolBridge,
            Set<AllocationID> preferredAllocations) {
        return declarativeSlotPoolBridge.requestNewAllocatedSlot(
                new SlotRequestId(), ResourceProfile.UNKNOWN, preferredAllocations, null);
    }
}
