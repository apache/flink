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
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The provider serves physical slot requests. */
public class PhysicalSlotProviderImpl implements PhysicalSlotProvider {
    private static final Logger LOG = LoggerFactory.getLogger(PhysicalSlotProviderImpl.class);

    private final SlotSelectionStrategy slotSelectionStrategy;

    private final SlotPool slotPool;

    public PhysicalSlotProviderImpl(
            SlotSelectionStrategy slotSelectionStrategy, SlotPool slotPool) {
        this.slotSelectionStrategy = checkNotNull(slotSelectionStrategy);
        this.slotPool = checkNotNull(slotPool);
    }

    @Override
    public void disableBatchSlotRequestTimeoutCheck() {
        slotPool.disableBatchSlotRequestTimeoutCheck();
    }

    @Override
    public Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>> allocatePhysicalSlots(
            Collection<PhysicalSlotRequest> physicalSlotRequests) {

        for (PhysicalSlotRequest physicalSlotRequest : physicalSlotRequests) {
            LOG.debug(
                    "Received slot request [{}] with resource requirements: {}",
                    physicalSlotRequest.getSlotRequestId(),
                    physicalSlotRequest.getSlotProfile().getPhysicalSlotResourceProfile());
        }

        Map<SlotRequestId, PhysicalSlotRequest> physicalSlotRequestsById =
                physicalSlotRequests.stream()
                        .collect(
                                Collectors.toMap(
                                        PhysicalSlotRequest::getSlotRequestId,
                                        Function.identity()));
        Map<SlotRequestId, Optional<PhysicalSlot>> availablePhysicalSlots =
                tryAllocateFromAvailable(physicalSlotRequestsById.values());

        return availablePhysicalSlots.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> {
                                    Optional<PhysicalSlot> availablePhysicalSlot = entry.getValue();
                                    SlotRequestId slotRequestId = entry.getKey();
                                    PhysicalSlotRequest physicalSlotRequest =
                                            physicalSlotRequestsById.get(slotRequestId);
                                    SlotProfile slotProfile = physicalSlotRequest.getSlotProfile();
                                    ResourceProfile resourceProfile =
                                            slotProfile.getPhysicalSlotResourceProfile();

                                    CompletableFuture<PhysicalSlot> slotFuture =
                                            availablePhysicalSlot
                                                    .map(CompletableFuture::completedFuture)
                                                    .orElseGet(
                                                            () ->
                                                                    requestNewSlot(
                                                                            slotRequestId,
                                                                            resourceProfile,
                                                                            slotProfile
                                                                                    .getPreferredAllocations(),
                                                                            physicalSlotRequest
                                                                                    .willSlotBeOccupiedIndefinitely()));

                                    return slotFuture.thenApply(
                                            physicalSlot ->
                                                    new PhysicalSlotRequest.Result(
                                                            slotRequestId, physicalSlot));
                                }));
    }

    private Map<SlotRequestId, Optional<PhysicalSlot>> tryAllocateFromAvailable(
            Collection<PhysicalSlotRequest> slotRequests) {
        FreeSlotInfoTracker freeSlotInfoTracker = slotPool.getFreeSlotInfoTracker();

        Map<SlotRequestId, Optional<PhysicalSlot>> allocateResult = new HashMap<>();
        for (PhysicalSlotRequest request : slotRequests) {
            Optional<SlotSelectionStrategy.SlotInfoAndLocality> slot =
                    slotSelectionStrategy.selectBestSlotForProfile(
                            freeSlotInfoTracker, request.getSlotProfile());
            allocateResult.put(
                    request.getSlotRequestId(),
                    slot.flatMap(
                            slotInfoAndLocality -> {
                                freeSlotInfoTracker.reserveSlot(
                                        slotInfoAndLocality.getSlotInfo().getAllocationId());
                                return slotPool.allocateAvailableSlot(
                                        request.getSlotRequestId(),
                                        slotInfoAndLocality.getSlotInfo().getAllocationId(),
                                        request.getSlotProfile().getPhysicalSlotResourceProfile());
                            }));
        }
        return allocateResult;
    }

    private CompletableFuture<PhysicalSlot> requestNewSlot(
            SlotRequestId slotRequestId,
            ResourceProfile resourceProfile,
            Collection<AllocationID> preferredAllocations,
            boolean willSlotBeOccupiedIndefinitely) {
        if (willSlotBeOccupiedIndefinitely) {
            return slotPool.requestNewAllocatedSlot(
                    slotRequestId, resourceProfile, preferredAllocations, null);
        } else {
            return slotPool.requestNewAllocatedBatchSlot(
                    slotRequestId, resourceProfile, preferredAllocations);
        }
    }

    @Override
    public void cancelSlotRequest(SlotRequestId slotRequestId, Throwable cause) {
        slotPool.releaseSlot(slotRequestId, cause);
    }
}
