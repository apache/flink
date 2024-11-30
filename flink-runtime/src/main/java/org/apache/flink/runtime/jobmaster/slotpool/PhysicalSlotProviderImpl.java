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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.LoadableResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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

    private final RequestSlotMatchingStrategy requestSlotMatchingStrategy;

    private final SlotPool slotPool;

    private final boolean requireAllAllocationAvailable;

    @VisibleForTesting
    public PhysicalSlotProviderImpl(
            SlotSelectionStrategy slotSelectionStrategy, SlotPool slotPool) {
        this(
                slotSelectionStrategy,
                RequestSlotMatchingStrategy.NoOpRequestSlotMatchingStrategy.INSTANCE,
                slotPool,
                false);
    }

    public PhysicalSlotProviderImpl(
            SlotSelectionStrategy slotSelectionStrategy,
            RequestSlotMatchingStrategy requestSlotMatchingStrategy,
            SlotPool slotPool,
            boolean requireAllAllocationAvailable) {
        this.slotSelectionStrategy = checkNotNull(slotSelectionStrategy);
        this.requestSlotMatchingStrategy = checkNotNull(requestSlotMatchingStrategy);
        this.slotPool = checkNotNull(slotPool);
        this.requireAllAllocationAvailable = requireAllAllocationAvailable;
    }

    @Override
    public void disableBatchSlotRequestTimeoutCheck() {
        slotPool.disableBatchSlotRequestTimeoutCheck();
    }

    @Override
    public Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>> allocatePhysicalSlots(
            Collection<PhysicalSlotRequest> physicalSlotRequests) {

        logRequestInfo(physicalSlotRequests);

        Map<SlotRequestId, PhysicalSlotRequest> physicalSlotRequestsById =
                physicalSlotRequests.stream()
                        .collect(
                                Collectors.toMap(
                                        PhysicalSlotRequest::getSlotRequestId,
                                        Function.identity()));
        Map<SlotRequestId, Optional<PhysicalSlot>> allocatedPhysicalSlots =
                tryAllocateFromAvailable(physicalSlotRequestsById.values());

        if (requireAllAllocationAvailable) {
            return tryAllocateAllRequestsWithAvailableSlots(
                    allocatedPhysicalSlots, physicalSlotRequestsById);
        } else {
            return allocateNewPhysicalSlots(allocatedPhysicalSlots, physicalSlotRequestsById);
        }
    }

    private Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>>
            tryAllocateAllRequestsWithAvailableSlots(
                    Map<SlotRequestId, Optional<PhysicalSlot>> allocatedPhysicalSlots,
                    Map<SlotRequestId, PhysicalSlotRequest> slotRequestsById) {
        Collection<PhysicalSlot> freeSlots =
                slotPool.getFreeSlotTracker().getFreeSlotsInformation();
        List<PendingRequest> remainingRequests =
                getRemainingPendingSlotRequests(slotRequestsById.values(), allocatedPhysicalSlots);
        if (freeSlots.size() < remainingRequests.size()) {
            // Allocate all request for new slots.
            return allocateNewPhysicalSlots(allocatedPhysicalSlots, slotRequestsById);
        } else {
            Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestMatches =
                    requestSlotMatchingStrategy.matchRequestsAndSlots(
                            freeSlots, remainingRequests, slotPool.getTaskExecutorsLoadingWeight());
            Preconditions.checkState(
                    requestMatches.size() <= remainingRequests.size(),
                    "Error allocate state in allocatePhysicalSlots.");
            if (requestMatches.size() == remainingRequests.size()) {
                // Allocated all slot requests here.
                return getAllRequestsAllocationResult(allocatedPhysicalSlots, requestMatches);
            } else {
                return allocateNewPhysicalSlots(allocatedPhysicalSlots, slotRequestsById);
            }
        }
    }

    private Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>>
            getAllRequestsAllocationResult(
                    Map<SlotRequestId, Optional<PhysicalSlot>> availableSlots,
                    Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestMatches) {
        Map<SlotRequestId, RequestSlotMatchingStrategy.RequestSlotMatch> matchesById =
                requestMatches.stream()
                        .collect(
                                Collectors.toMap(
                                        match -> match.getPendingRequest().getSlotRequestId(),
                                        Function.identity()));
        return availableSlots.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> {
                                    Optional<PhysicalSlot> availableSlotOpt = entry.getValue();
                                    SlotRequestId slotRequestId = entry.getKey();
                                    RequestSlotMatchingStrategy.RequestSlotMatch matchResult =
                                            matchesById.get(slotRequestId);

                                    CompletableFuture<PhysicalSlot> slotFuture =
                                            availableSlotOpt
                                                    .map(CompletableFuture::completedFuture)
                                                    .orElseGet(
                                                            () ->
                                                                    CompletableFuture
                                                                            .completedFuture(
                                                                                    slotPool.allocateAvailableSlot(
                                                                                                    slotRequestId,
                                                                                                    matchResult
                                                                                                            .getSlot()
                                                                                                            .getAllocationId(),
                                                                                                    matchResult
                                                                                                            .getPendingRequest()
                                                                                                            .getLoadableResourceProfile())
                                                                                            .get()));

                                    return slotFuture.thenApply(
                                            physicalSlot ->
                                                    new PhysicalSlotRequest.Result(
                                                            slotRequestId, physicalSlot));
                                }));
    }

    private List<PendingRequest> getRemainingPendingSlotRequests(
            Collection<PhysicalSlotRequest> physicalSlotRequests,
            Map<SlotRequestId, Optional<PhysicalSlot>> availablePhysicalSlots) {
        return physicalSlotRequests.stream()
                .filter(r -> availablePhysicalSlots.get(r.getSlotRequestId()).isEmpty())
                .map(
                        request ->
                                request.willSlotBeOccupiedIndefinitely()
                                        ? PendingRequest.createNormalRequest(
                                                request.getSlotRequestId(),
                                                request.getPhysicalSlotLoadableResourceProfile(),
                                                request.getSlotProfile().getPreferredAllocations())
                                        : PendingRequest.createBatchRequest(
                                                request.getSlotRequestId(),
                                                request.getPhysicalSlotLoadableResourceProfile(),
                                                request.getSlotProfile().getPreferredAllocations()))
                .collect(Collectors.toList());
    }

    private Map<SlotRequestId, CompletableFuture<PhysicalSlotRequest.Result>>
            allocateNewPhysicalSlots(
                    Map<SlotRequestId, Optional<PhysicalSlot>> availablePhysicalSlots,
                    Map<SlotRequestId, PhysicalSlotRequest> physicalSlotRequestsById) {
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

                                    CompletableFuture<PhysicalSlot> slotFuture =
                                            availablePhysicalSlot
                                                    .map(CompletableFuture::completedFuture)
                                                    .orElseGet(
                                                            () ->
                                                                    requestNewSlot(
                                                                            slotRequestId,
                                                                            physicalSlotRequest
                                                                                    .getPhysicalSlotLoadableResourceProfile(),
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

    private void logRequestInfo(Collection<PhysicalSlotRequest> physicalSlotRequests) {
        if (LOG.isDebugEnabled()) {
            for (PhysicalSlotRequest physicalSlotRequest : physicalSlotRequests) {
                LOG.debug(
                        "Received slot request [{}] with loadable resource requirements: {}",
                        physicalSlotRequest,
                        physicalSlotRequest.getPhysicalSlotLoadableResourceProfile());
            }
        }
    }

    private Map<SlotRequestId, Optional<PhysicalSlot>> tryAllocateFromAvailable(
            Collection<PhysicalSlotRequest> slotRequests) {
        FreeSlotTracker freeSlotTracker = slotPool.getFreeSlotTracker();

        Map<SlotRequestId, Optional<PhysicalSlot>> allocateResult = new HashMap<>();
        for (PhysicalSlotRequest request : slotRequests) {
            Optional<SlotSelectionStrategy.SlotInfoAndLocality> slot =
                    slotSelectionStrategy.selectBestSlotForProfile(
                            freeSlotTracker, request.getSlotProfile());
            allocateResult.put(
                    request.getSlotRequestId(),
                    slot.flatMap(
                            slotInfoAndLocality -> {
                                freeSlotTracker.reserveSlot(
                                        slotInfoAndLocality.getSlotInfo().getAllocationId());
                                return slotPool.allocateAvailableSlot(
                                        request.getSlotRequestId(),
                                        slotInfoAndLocality.getSlotInfo().getAllocationId(),
                                        request.getPhysicalSlotLoadableResourceProfile());
                            }));
        }
        return allocateResult;
    }

    private CompletableFuture<PhysicalSlot> requestNewSlot(
            SlotRequestId slotRequestId,
            LoadableResourceProfile loadableResourceProfile,
            Collection<AllocationID> preferredAllocations,
            boolean willSlotBeOccupiedIndefinitely) {
        if (willSlotBeOccupiedIndefinitely) {
            return slotPool.requestNewAllocatedSlot(
                    slotRequestId, loadableResourceProfile, preferredAllocations, null);
        } else {
            return slotPool.requestNewAllocatedBatchSlot(
                    slotRequestId, loadableResourceProfile, preferredAllocations);
        }
    }

    @Override
    public void cancelSlotRequest(SlotRequestId slotRequestId, Throwable cause) {
        slotPool.releaseSlot(slotRequestId, cause);
    }
}
