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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** {@link SlotPool} implementation which uses the {@link DeclarativeSlotPool} to allocate slots. */
public class DeclarativeSlotPoolBridge extends DeclarativeSlotPoolService implements SlotPool {

    /** Helper class to represent the fulfilled allocation infromation. */
    private static final class FulfilledAllocation {
        final AllocationID allocationID;
        final ResourceID taskExecutorID;
        final LoadingWeight loadingWeight;

        FulfilledAllocation(PhysicalSlot slot, LoadingWeight loadingWeight) {
            this.allocationID = Preconditions.checkNotNull(slot.getAllocationId());
            this.taskExecutorID =
                    Preconditions.checkNotNull(slot.getTaskManagerLocation().getResourceID());
            this.loadingWeight = Preconditions.checkNotNull(loadingWeight);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FulfilledAllocation that = (FulfilledAllocation) o;
            return Objects.equals(allocationID, that.allocationID)
                    && Objects.equals(taskExecutorID, that.taskExecutorID)
                    && Objects.equals(loadingWeight, that.loadingWeight);
        }
    }

    private final Map<SlotRequestId, PendingRequest> pendingRequests;
    private final Map<SlotRequestId, FulfilledAllocation> fulfilledRequests;
    private final Duration idleSlotTimeout;

    private final RequestSlotMatchingStrategy requestSlotMatchingStrategy;

    private final Duration batchSlotTimeout;
    private boolean isBatchSlotRequestTimeoutCheckDisabled;

    private boolean isJobRestarting = false;

    private final boolean deferSlotAllocation;

    public DeclarativeSlotPoolBridge(
            JobID jobId,
            DeclarativeSlotPoolFactory declarativeSlotPoolFactory,
            Clock clock,
            Duration rpcTimeout,
            Duration idleSlotTimeout,
            Duration batchSlotTimeout,
            RequestSlotMatchingStrategy requestSlotMatchingStrategy,
            Duration slotRequestMaxInterval,
            boolean deferSlotAllocation,
            @Nonnull ComponentMainThreadExecutor componentMainThreadExecutor) {
        super(
                jobId,
                declarativeSlotPoolFactory,
                clock,
                idleSlotTimeout,
                rpcTimeout,
                slotRequestMaxInterval,
                componentMainThreadExecutor);

        this.idleSlotTimeout = idleSlotTimeout;
        this.batchSlotTimeout = Preconditions.checkNotNull(batchSlotTimeout);

        log.debug(
                "Using the request slot matching strategy: {}",
                requestSlotMatchingStrategy.getClass().getSimpleName());
        this.requestSlotMatchingStrategy = requestSlotMatchingStrategy;
        this.deferSlotAllocation = deferSlotAllocation;

        this.isBatchSlotRequestTimeoutCheckDisabled = false;

        this.pendingRequests = new LinkedHashMap<>();
        this.fulfilledRequests = new HashMap<>();
    }

    @Override
    public <T> Optional<T> castInto(Class<T> clazz) {
        if (clazz.isAssignableFrom(getClass())) {
            return Optional.of(clazz.cast(this));
        }

        return Optional.empty();
    }

    @Override
    protected void onStart() {

        getDeclarativeSlotPool().registerNewSlotsListener(this::newSlotsAreAvailable);
        if (deferSlotAllocation) {
            getDeclarativeSlotPool()
                    .registerResourceRequestStableListener(
                            () -> {
                                if (!pendingRequests.isEmpty()) {
                                    componentMainThreadExecutor.schedule(
                                            this::newSlotsAvailableForDeferAllocation,
                                            0L,
                                            TimeUnit.MILLISECONDS);
                                }
                            });
        }

        componentMainThreadExecutor.schedule(
                this::checkIdleSlotTimeout, idleSlotTimeout.toMillis(), TimeUnit.MILLISECONDS);
        componentMainThreadExecutor.schedule(
                this::checkBatchSlotTimeout, batchSlotTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void onClose() {
        final FlinkException cause = new FlinkException("Closing slot pool");
        cancelPendingRequests(request -> true, cause);
    }

    /**
     * To set whether the underlying is currently restarting or not. In the former case the slot
     * pool bridge will accept all incoming slot offers.
     *
     * @param isJobRestarting whether this is restarting or not
     */
    @Override
    public void setIsJobRestarting(boolean isJobRestarting) {
        this.isJobRestarting = isJobRestarting;
    }

    @Override
    public Collection<SlotOffer> offerSlots(
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            Collection<SlotOffer> offers) {
        assertHasBeenStarted();

        if (!isTaskManagerRegistered(taskManagerLocation.getResourceID())) {
            log.debug(
                    "Ignoring offered slots from unknown task manager {}.",
                    taskManagerLocation.getResourceID());
            return Collections.emptyList();
        }

        if (isJobRestarting) {
            return getDeclarativeSlotPool()
                    .registerSlots(
                            offers,
                            taskManagerLocation,
                            taskManagerGateway,
                            getRelativeTimeMillis());

        } else {
            return getDeclarativeSlotPool()
                    .offerSlots(
                            offers,
                            taskManagerLocation,
                            taskManagerGateway,
                            getRelativeTimeMillis());
        }
    }

    private void cancelPendingRequests(
            Predicate<PendingRequest> requestPredicate, FlinkException cancelCause) {

        ResourceCounter decreasedResourceRequirements = ResourceCounter.empty();

        // need a copy since failing a request could trigger another request to be issued
        final Iterable<PendingRequest> pendingRequestsToFail =
                new ArrayList<>(pendingRequests.values());
        pendingRequests.clear();

        for (PendingRequest pendingRequest : pendingRequestsToFail) {
            if (requestPredicate.test(pendingRequest)) {
                pendingRequest.failRequest(cancelCause);
                decreasedResourceRequirements =
                        decreasedResourceRequirements.add(pendingRequest.getResourceProfile(), 1);
            } else {
                pendingRequests.put(pendingRequest.getSlotRequestId(), pendingRequest);
            }
        }

        getDeclarativeSlotPool().decreaseResourceRequirementsBy(decreasedResourceRequirements);
    }

    @Override
    protected void onReleaseTaskManager(ResourceCounter previouslyFulfilledRequirement) {
        getDeclarativeSlotPool().decreaseResourceRequirementsBy(previouslyFulfilledRequirement);
    }

    @VisibleForTesting
    void newSlotsAreAvailable(Collection<? extends PhysicalSlot> newSlots) {
        log.debug("Received new available slots: {}", newSlots);

        if (pendingRequests.isEmpty()) {
            return;
        }

        if (deferSlotAllocation) {
            if (getDeclarativeSlotPool().isResourceRequestStable()) {
                newSlotsAvailableForDeferAllocation();
            }
        } else {
            newSlotsAvailableForDirectlyAllocation(newSlots);
        }
    }

    private Map<ResourceID, LoadingWeight> getTaskExecutorsLoadingView() {
        final Map<ResourceID, LoadingWeight> result = new HashMap<>();
        Collection<FulfilledAllocation> fulfilledAllocations = fulfilledRequests.values();
        for (FulfilledAllocation allocation : fulfilledAllocations) {
            result.compute(
                    allocation.taskExecutorID,
                    (ignoredID, oldLoading) ->
                            Objects.isNull(oldLoading)
                                    ? allocation.loadingWeight
                                    : oldLoading.merge(allocation.loadingWeight));
        }
        return result;
    }

    private void newSlotsAvailableForDeferAllocation() {
        final Collection<PhysicalSlot> freeSlots =
                getDeclarativeSlotPool().getFreeSlotTracker().getFreeSlotsInformation();

        if (freeSlots.size() < pendingRequests.size()) {
            // Do nothing and waiting slots.
            log.debug(
                    "The number of available slots: {}, the required number of slots: {}, waiting for more available slots.",
                    freeSlots.size(),
                    pendingRequests.size());
            return;
        }

        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                requestSlotMatchingStrategy.matchRequestsAndSlots(
                        freeSlots, pendingRequests.values(), getTaskExecutorsLoadingView());
        if (requestSlotMatches.size() == pendingRequests.size()) {
            reserveAndFulfillMatchedFreeSlots(requestSlotMatches);
        } else if (requestSlotMatches.size() < pendingRequests.size()) {
            // Do nothing and waiting slots.
            log.debug(
                    "Ignored the matched results: {}, pendingRequests: {}, waiting for more available slots.",
                    requestSlotMatches,
                    pendingRequests);
        } else {
            // For requestSlotMatches.size() > pendingRequests.size()
            throw new IllegalStateException(
                    "The number of matched slots is not equals to the pendingRequests.");
        }
    }

    private void newSlotsAvailableForDirectlyAllocation(
            Collection<? extends PhysicalSlot> newSlots) {
        final Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches =
                requestSlotMatchingStrategy.matchRequestsAndSlots(
                        newSlots, pendingRequests.values(), new HashMap<>());
        reserveAndFulfillMatchedFreeSlots(requestSlotMatches);
    }

    private void reserveAndFulfillMatchedFreeSlots(
            Collection<RequestSlotMatchingStrategy.RequestSlotMatch> requestSlotMatches) {
        for (RequestSlotMatchingStrategy.RequestSlotMatch match : requestSlotMatches) {
            final PendingRequest pendingRequest = match.getPendingRequest();
            final PhysicalSlot slot = match.getSlot();

            log.debug("Matched pending request {} with slot {}.", pendingRequest, slot);

            Preconditions.checkNotNull(
                    pendingRequests.remove(pendingRequest.getSlotRequestId()),
                    "Cannot fulfill a non existing pending slot request.");

            reserveFreeSlot(slot.getAllocationId(), pendingRequest);
        }

        // we have to first reserve all matching slots before fulfilling the requests
        // otherwise it can happen that the scheduler reserves one of the new slots
        // for a request which has been triggered by fulfilling a pending request
        for (RequestSlotMatchingStrategy.RequestSlotMatch requestSlotMatch : requestSlotMatches) {
            final PendingRequest pendingRequest = requestSlotMatch.getPendingRequest();
            final PhysicalSlot slot = requestSlotMatch.getSlot();

            Preconditions.checkState(
                    pendingRequest.fulfill(slot), "Pending requests must be fulfillable.");
        }
    }

    @VisibleForTesting
    Collection<PhysicalSlot> getFreeSlotsInformation() {
        return getDeclarativeSlotPool().getFreeSlotTracker().getFreeSlotsInformation();
    }

    private PhysicalSlot reserveFreeSlot(AllocationID allocationId, PendingRequest pendingRequest) {
        SlotRequestId slotRequestId = pendingRequest.getSlotRequestId();
        log.debug("Reserve slot {} for slot request id {}", allocationId, slotRequestId);
        final PhysicalSlot slot =
                getDeclarativeSlotPool()
                        .reserveFreeSlot(allocationId, pendingRequest.getResourceProfile());
        fulfilledRequests.put(
                slotRequestId, new FulfilledAllocation(slot, pendingRequest.getLoading()));
        return slot;
    }

    @Override
    public Optional<PhysicalSlot> allocateAvailableSlot(
            AllocationID allocationID, PhysicalSlotRequest physicalSlotRequest) {
        assertRunningInMainThread();

        ResourceProfile requiredResourceProfile =
                physicalSlotRequest.getPhysicalSlotResourceProfile();
        Preconditions.checkNotNull(
                requiredResourceProfile, "The requiredResourceProfile must not be null.");

        SlotRequestId slotRequestId = physicalSlotRequest.getSlotRequestId();
        log.debug(
                "Reserving free slot {} for slot request id {} and profile {}.",
                allocationID,
                slotRequestId,
                requiredResourceProfile);

        return Optional.of(
                reserveFreeSlotForResource(allocationID, physicalSlotRequest.toPendingRequest()));
    }

    private PhysicalSlot reserveFreeSlotForResource(
            AllocationID allocationId, PendingRequest pendingRequest) {

        ResourceProfile requiredResourceProfile = pendingRequest.getResourceProfile();

        getDeclarativeSlotPool()
                .increaseResourceRequirementsBy(
                        ResourceCounter.withResource(requiredResourceProfile, 1));

        return reserveFreeSlot(allocationId, pendingRequest);
    }

    @Override
    public CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
            PhysicalSlotRequest physicalSlotRequest, @Nullable Duration timeout) {
        assertRunningInMainThread();

        log.debug(
                "Request new allocated slot with slot request id {} and resource profile {}",
                physicalSlotRequest.getSlotRequestId(),
                physicalSlotRequest.getPhysicalSlotResourceProfile());

        return internalRequestNewSlot(physicalSlotRequest.toPendingRequest(), timeout);
    }

    @Override
    public CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(
            PhysicalSlotRequest physicalSlotRequest) {
        assertRunningInMainThread();

        log.debug(
                "Request new allocated batch slot with slot request id {} and resource profile {}",
                physicalSlotRequest.getSlotRequestId(),
                physicalSlotRequest.getPhysicalSlotResourceProfile());

        return internalRequestNewSlot(physicalSlotRequest.toPendingRequest(), null);
    }

    private CompletableFuture<PhysicalSlot> internalRequestNewSlot(
            PendingRequest pendingRequest, @Nullable Duration timeout) {
        internalRequestNewAllocatedSlot(pendingRequest);

        if (timeout == null) {
            return pendingRequest.getSlotFuture();
        } else {
            return FutureUtils.orTimeout(
                            pendingRequest.getSlotFuture(),
                            timeout.toMillis(),
                            TimeUnit.MILLISECONDS,
                            componentMainThreadExecutor,
                            String.format(
                                    "Pending slot request %s timed out after %d ms.",
                                    pendingRequest.getSlotRequestId(), timeout.toMillis()))
                    .whenComplete(
                            (physicalSlot, throwable) -> {
                                if (throwable instanceof TimeoutException) {
                                    timeoutPendingSlotRequest(pendingRequest.getSlotRequestId());
                                }
                            });
        }
    }

    private void timeoutPendingSlotRequest(SlotRequestId slotRequestId) {
        releaseSlot(
                slotRequestId,
                new TimeoutException("Pending slot request timed out in slot pool."));
    }

    private void internalRequestNewAllocatedSlot(PendingRequest pendingRequest) {
        pendingRequests.put(pendingRequest.getSlotRequestId(), pendingRequest);

        getDeclarativeSlotPool()
                .increaseResourceRequirementsBy(
                        ResourceCounter.withResource(pendingRequest.getResourceProfile(), 1));
    }

    @Override
    protected void onFailAllocation(ResourceCounter previouslyFulfilledRequirements) {
        getDeclarativeSlotPool().decreaseResourceRequirementsBy(previouslyFulfilledRequirements);
    }

    @Override
    public void releaseSlot(@Nonnull SlotRequestId slotRequestId, @Nullable Throwable cause) {
        log.debug("Release slot with slot request id {}", slotRequestId);
        assertRunningInMainThread();

        final PendingRequest pendingRequest = pendingRequests.remove(slotRequestId);

        if (pendingRequest != null) {
            getDeclarativeSlotPool()
                    .decreaseResourceRequirementsBy(
                            ResourceCounter.withResource(pendingRequest.getResourceProfile(), 1));
            pendingRequest.failRequest(
                    new FlinkException(
                            String.format(
                                    "Pending slot request with %s has been released.",
                                    pendingRequest.getSlotRequestId()),
                            cause));
        } else {
            final FulfilledAllocation fulfilledAllocation = fulfilledRequests.remove(slotRequestId);

            if (fulfilledAllocation != null) {
                ResourceCounter previouslyFulfilledRequirement =
                        getDeclarativeSlotPool()
                                .freeReservedSlot(
                                        fulfilledAllocation.allocationID,
                                        cause,
                                        getRelativeTimeMillis());
                getDeclarativeSlotPool()
                        .decreaseResourceRequirementsBy(previouslyFulfilledRequirement);
            } else {
                log.debug(
                        "Could not find slot which has fulfilled slot request {}. Ignoring the release operation.",
                        slotRequestId);
            }
        }
    }

    @Override
    public void notifyNotEnoughResourcesAvailable(
            Collection<ResourceRequirement> acquiredResources) {
        assertRunningInMainThread();

        failPendingRequests(acquiredResources);
    }

    private void failPendingRequests(Collection<ResourceRequirement> acquiredResources) {
        // only fails streaming requests because batch jobs do not require all resources
        // requirements to be fullfilled at the same time
        Predicate<PendingRequest> predicate = request -> !request.isBatchRequest();
        if (pendingRequests.values().stream().anyMatch(predicate)) {
            log.warn(
                    "Could not acquire the minimum required resources, failing slot requests. Acquired: {}. Current slot pool status: {}",
                    acquiredResources,
                    getSlotServiceStatus());
            cancelPendingRequests(
                    predicate,
                    NoResourceAvailableException.withoutStackTrace(
                            "Could not acquire the minimum required resources."));
        }
    }

    @Override
    public Collection<SlotInfo> getAllocatedSlotsInformation() {
        assertRunningInMainThread();

        final Collection<? extends SlotInfo> allSlotsInformation =
                getDeclarativeSlotPool().getAllSlotsInformation();
        final Set<AllocationID> freeSlots =
                getDeclarativeSlotPool().getFreeSlotTracker().getAvailableSlots();

        return allSlotsInformation.stream()
                .filter(slotInfo -> !freeSlots.contains(slotInfo.getAllocationId()))
                .collect(Collectors.toList());
    }

    @Override
    public FreeSlotTracker getFreeSlotTracker() {
        assertRunningInMainThread();

        return getDeclarativeSlotPool().getFreeSlotTracker();
    }

    @Override
    public void disableBatchSlotRequestTimeoutCheck() {
        isBatchSlotRequestTimeoutCheckDisabled = true;
    }

    private void assertRunningInMainThread() {
        if (componentMainThreadExecutor != null) {
            componentMainThreadExecutor.assertRunningInMainThread();
        } else {
            throw new IllegalStateException("The FutureSlotPool has not been started yet.");
        }
    }

    private void checkIdleSlotTimeout() {
        getDeclarativeSlotPool().releaseIdleSlots(getRelativeTimeMillis());

        if (componentMainThreadExecutor != null) {
            componentMainThreadExecutor.schedule(
                    this::checkIdleSlotTimeout, idleSlotTimeout.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    void checkBatchSlotTimeout() {
        assertRunningInMainThread();

        if (isBatchSlotRequestTimeoutCheckDisabled) {
            return;
        }

        final Collection<PendingRequest> pendingBatchRequests = getPendingBatchRequests();

        if (!pendingBatchRequests.isEmpty()) {
            final Set<ResourceProfile> allResourceProfiles = getResourceProfilesFromAllSlots();

            final Map<Boolean, List<PendingRequest>> fulfillableAndUnfulfillableRequests =
                    pendingBatchRequests.stream()
                            .collect(
                                    Collectors.partitioningBy(
                                            canBeFulfilledWithAnySlot(allResourceProfiles)));

            final List<PendingRequest> fulfillableRequests =
                    fulfillableAndUnfulfillableRequests.get(true);
            final List<PendingRequest> unfulfillableRequests =
                    fulfillableAndUnfulfillableRequests.get(false);

            final long currentTimestamp = getRelativeTimeMillis();

            for (PendingRequest fulfillableRequest : fulfillableRequests) {
                fulfillableRequest.markFulfillable();
            }

            for (PendingRequest unfulfillableRequest : unfulfillableRequests) {
                unfulfillableRequest.markUnfulfillable(currentTimestamp);

                if (unfulfillableRequest.getUnfulfillableSince() + batchSlotTimeout.toMillis()
                        <= currentTimestamp) {
                    timeoutPendingSlotRequest(unfulfillableRequest.getSlotRequestId());
                }
            }
        }

        if (componentMainThreadExecutor != null) {
            componentMainThreadExecutor.schedule(
                    this::checkBatchSlotTimeout,
                    batchSlotTimeout.toMillis(),
                    TimeUnit.MILLISECONDS);
        }
    }

    private Set<ResourceProfile> getResourceProfilesFromAllSlots() {
        return Stream.concat(
                        getFreeSlotTracker().getFreeSlotsInformation().stream(),
                        getAllocatedSlotsInformation().stream())
                .map(SlotInfo::getResourceProfile)
                .collect(Collectors.toSet());
    }

    private Collection<PendingRequest> getPendingBatchRequests() {
        return pendingRequests.values().stream()
                .filter(PendingRequest::isBatchRequest)
                .collect(Collectors.toList());
    }

    private static Predicate<PendingRequest> canBeFulfilledWithAnySlot(
            Set<ResourceProfile> allocatedResourceProfiles) {
        return pendingRequest -> {
            for (ResourceProfile allocatedResourceProfile : allocatedResourceProfiles) {
                if (allocatedResourceProfile.isMatching(pendingRequest.getResourceProfile())) {
                    return true;
                }
            }

            return false;
        };
    }

    @VisibleForTesting
    public int getNumPendingRequests() {
        return pendingRequests.size();
    }

    @VisibleForTesting
    void increaseResourceRequirementsBy(ResourceCounter increment) {
        getDeclarativeSlotPool().increaseResourceRequirementsBy(increment);
    }

    @VisibleForTesting
    boolean isBatchSlotRequestTimeoutCheckEnabled() {
        return !isBatchSlotRequestTimeoutCheckDisabled;
    }
}
