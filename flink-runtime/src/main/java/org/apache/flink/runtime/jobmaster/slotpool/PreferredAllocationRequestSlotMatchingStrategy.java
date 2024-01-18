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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * {@link RequestSlotMatchingStrategy} that takes the preferred allocations into account. The
 * strategy will try to fulfill the preferred allocations and if this is not possible, then it will
 * fall back to {@link SimpleRequestSlotMatchingStrategy}.
 */
public class PreferredAllocationRequestSlotMatchingStrategy implements RequestSlotMatchingStrategy {

    private RequestSlotMatchingStrategy rollback;

    private PreferredAllocationRequestSlotMatchingStrategy(
            @Nullable RequestSlotMatchingStrategy rollback) {
        this.rollback = rollback;
    }

    public static RequestSlotMatchingStrategy create(
            @Nullable RequestSlotMatchingStrategy rollback) {
        return new PreferredAllocationRequestSlotMatchingStrategy(rollback);
    }

    @Override
    public Collection<RequestSlotMatch> matchRequestsAndSlots(
            Collection<? extends PhysicalSlot> slots,
            Collection<PendingRequest> pendingRequests,
            Map<ResourceID, LoadingWeight> taskExecutorsLoadingWeight) {
        final Collection<RequestSlotMatch> requestSlotMatches = new ArrayList<>();

        final Map<AllocationID, PhysicalSlot> freeSlots =
                slots.stream()
                        .collect(
                                Collectors.toMap(
                                        PhysicalSlot::getAllocationId, Function.identity()));

        final Map<SlotRequestId, PendingRequest> pendingRequestsWithPreferredAllocations =
                new HashMap<>();
        final List<PendingRequest> unmatchedRequests = new ArrayList<>();

        // Split requests into those that have preferred allocations and those that don't have
        for (PendingRequest pendingRequest : pendingRequests) {
            if (pendingRequest.getPreferredAllocations().isEmpty()) {
                unmatchedRequests.add(pendingRequest);
            } else {
                pendingRequestsWithPreferredAllocations.put(
                        pendingRequest.getSlotRequestId(), pendingRequest);
            }
        }

        final Iterator<PhysicalSlot> freeSlotsIterator = freeSlots.values().iterator();
        // Match slots and pending requests based on preferred allocation
        while (freeSlotsIterator.hasNext() && !pendingRequestsWithPreferredAllocations.isEmpty()) {
            final PhysicalSlot freeSlot = freeSlotsIterator.next();

            final Iterator<PendingRequest> pendingRequestIterator =
                    pendingRequestsWithPreferredAllocations.values().iterator();

            while (pendingRequestIterator.hasNext()) {
                final PendingRequest pendingRequest = pendingRequestIterator.next();

                if (freeSlot.getResourceProfile().isMatching(pendingRequest.getResourceProfile())
                        && pendingRequest
                                .getPreferredAllocations()
                                .contains(freeSlot.getAllocationId())) {
                    requestSlotMatches.add(RequestSlotMatch.createFor(pendingRequest, freeSlot));
                    taskExecutorsLoadingWeight.compute(
                            freeSlot.getTaskManagerLocation().getResourceID(),
                            (resourceID, loadingWeight) ->
                                    loadingWeight == null
                                            ? pendingRequest.getLoading()
                                            : loadingWeight.merge(pendingRequest.getLoading()));
                    pendingRequestIterator.remove();
                    freeSlotsIterator.remove();
                    break;
                }
            }
        }

        unmatchedRequests.addAll(pendingRequestsWithPreferredAllocations.values());
        if (rollback != null && !freeSlots.isEmpty() && !unmatchedRequests.isEmpty()) {
            requestSlotMatches.addAll(
                    rollback.matchRequestsAndSlots(
                            freeSlots.values(), unmatchedRequests, taskExecutorsLoadingWeight));
        }

        return requestSlotMatches;
    }

    @Override
    public String toString() {
        return PreferredAllocationRequestSlotMatchingStrategy.class.getSimpleName();
    }
}
