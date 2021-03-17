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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/** Represents a bulk of physical slot requests. */
class PhysicalSlotRequestBulkImpl implements PhysicalSlotRequestBulk {

    private final Map<SlotRequestId, ResourceProfile> pendingRequests;

    private final Map<SlotRequestId, AllocationID> fulfilledRequests = new HashMap<>();

    private final BiConsumer<SlotRequestId, Throwable> canceller;

    PhysicalSlotRequestBulkImpl(
            Map<SlotRequestId, ResourceProfile> physicalSlotRequests,
            BiConsumer<SlotRequestId, Throwable> canceller) {
        this.pendingRequests = new HashMap<>(physicalSlotRequests);
        this.canceller = canceller;
    }

    void markRequestFulfilled(final SlotRequestId slotRequestId, final AllocationID allocationID) {
        pendingRequests.remove(slotRequestId);
        fulfilledRequests.put(slotRequestId, allocationID);
    }

    @Override
    public Collection<ResourceProfile> getPendingRequests() {
        return pendingRequests.values();
    }

    @Override
    public Set<AllocationID> getAllocationIdsOfFulfilledRequests() {
        return new HashSet<>(fulfilledRequests.values());
    }

    @Override
    public void cancel(Throwable cause) {
        // pending requests must be canceled first otherwise they might be fulfilled by
        // allocated slots released from this bulk
        for (SlotRequestId slotRequestId : pendingRequests.keySet()) {
            canceller.accept(slotRequestId, cause);
        }
        for (SlotRequestId slotRequestId : fulfilledRequests.keySet()) {
            canceller.accept(slotRequestId, cause);
        }
    }
}
