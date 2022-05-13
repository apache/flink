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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.scheduler.TestingPhysicalSlot;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link PreferredAllocationRequestSlotMatchingStrategy}. */
@ExtendWith(TestLoggerExtension.class)
public class PreferredAllocationRequestSlotMatchingStrategyTest {

    /**
     * This test ensures that new slots are matched against the preferred allocationIds of the
     * pending requests.
     */
    @Test
    public void testNewSlotsAreMatchedAgainstPreferredAllocationIDs() throws Exception {
        final PreferredAllocationRequestSlotMatchingStrategy strategy =
                PreferredAllocationRequestSlotMatchingStrategy.INSTANCE;

        final AllocationID allocationId1 = new AllocationID();
        final AllocationID allocationId2 = new AllocationID();

        final Collection<TestingPhysicalSlot> slots =
                Arrays.asList(
                        TestingPhysicalSlot.builder().withAllocationID(allocationId1).build(),
                        TestingPhysicalSlot.builder().withAllocationID(allocationId2).build());
        final Collection<PendingRequest> pendingRequests =
                Arrays.asList(
                        PendingRequest.createNormalRequest(
                                new SlotRequestId(),
                                ResourceProfile.UNKNOWN,
                                Collections.singleton(allocationId2)),
                        PendingRequest.createNormalRequest(
                                new SlotRequestId(),
                                ResourceProfile.UNKNOWN,
                                Collections.singleton(allocationId1)));

        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                strategy.matchRequestsAndSlots(slots, pendingRequests);

        assertThat(requestSlotMatches).hasSize(2);

        for (RequestSlotMatchingStrategy.RequestSlotMatch requestSlotMatch : requestSlotMatches) {
            assertThat(requestSlotMatch.getPendingRequest().getPreferredAllocations())
                    .contains(requestSlotMatch.getSlot().getAllocationId());
        }
    }
}
