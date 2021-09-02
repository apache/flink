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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.slots.DefaultRequirementMatcher;
import org.apache.flink.runtime.slots.RequirementMatcher;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Default {@link DeclarativeSlotPool} implementation.
 *
 * <p>The implementation collects the current resource requirements and declares them at the
 * ResourceManager. Whenever new slots are offered, the slot pool compares the offered slots to the
 * set of available and required resources and only accepts those slots which are required.
 *
 * <p>Slots which are released won't be returned directly to their owners. Instead, the slot pool
 * implementation will only return them after the idleSlotTimeout has been exceeded by a free slot.
 *
 * <p>The slot pool will call {@link #newSlotsListener} whenever newly offered slots are accepted or
 * if an allocated slot should become free after it is being {@link #freeReservedSlot freed}.
 *
 * <p>This class expects 1 of 2 access patterns for changing requirements, which should not be
 * mixed:
 *
 * <p>1) the legacy approach (used by the DefaultScheduler) tightly couples requirements to reserved
 * slots. When a slot is requested it increases the requirements, when the slot is freed they are
 * decreased again. In the general case what happens is that requirements are increased, a free slot
 * is reserved, (the slot is used for a bit,) the slot is freed, the requirements are reduced. To
 * this end {@link #freeReservedSlot}, {@link #releaseSlot} and {@link #releaseSlots} return a
 * {@link ResourceCounter} describing which requirement the slot(s) were fulfilling, with the
 * expectation that the scheduler will subsequently decrease the requirements by that amount.
 *
 * <p>2) The declarative approach (used by the AdaptiveScheduler) in contrast derives requirements
 * exclusively based on what a given job currently requires. It may repeatedly reserve/free slots
 * without any modifications to the requirements.
 */
public class DefaultDeclarativeSlotPool implements DeclarativeSlotPool {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultDeclarativeSlotPool.class);

    private final Consumer<? super Collection<ResourceRequirement>> notifyNewResourceRequirements;

    private final Time idleSlotTimeout;
    private final Time rpcTimeout;

    private final JobID jobId;
    private final AllocatedSlotPool slotPool;

    private final Map<AllocationID, ResourceProfile> slotToRequirementProfileMappings;

    private ResourceCounter totalResourceRequirements;

    private ResourceCounter fulfilledResourceRequirements;

    private NewSlotsListener newSlotsListener = NoOpNewSlotsListener.INSTANCE;

    private final RequirementMatcher requirementMatcher = new DefaultRequirementMatcher();

    public DefaultDeclarativeSlotPool(
            JobID jobId,
            AllocatedSlotPool slotPool,
            Consumer<? super Collection<ResourceRequirement>> notifyNewResourceRequirements,
            Time idleSlotTimeout,
            Time rpcTimeout) {

        this.jobId = jobId;
        this.slotPool = slotPool;
        this.notifyNewResourceRequirements = notifyNewResourceRequirements;
        this.idleSlotTimeout = idleSlotTimeout;
        this.rpcTimeout = rpcTimeout;
        this.totalResourceRequirements = ResourceCounter.empty();
        this.fulfilledResourceRequirements = ResourceCounter.empty();
        this.slotToRequirementProfileMappings = new HashMap<>();
    }

    @Override
    public void increaseResourceRequirementsBy(ResourceCounter increment) {
        if (increment.isEmpty()) {
            return;
        }
        totalResourceRequirements = totalResourceRequirements.add(increment);

        declareResourceRequirements();
    }

    @Override
    public void decreaseResourceRequirementsBy(ResourceCounter decrement) {
        if (decrement.isEmpty()) {
            return;
        }
        totalResourceRequirements = totalResourceRequirements.subtract(decrement);

        declareResourceRequirements();
    }

    @Override
    public void setResourceRequirements(ResourceCounter resourceRequirements) {
        totalResourceRequirements = resourceRequirements;

        declareResourceRequirements();
    }

    private void declareResourceRequirements() {
        final Collection<ResourceRequirement> resourceRequirements = getResourceRequirements();

        LOG.debug(
                "Declare new resource requirements for job {}.{}\trequired resources: {}{}\tacquired resources: {}",
                jobId,
                System.lineSeparator(),
                resourceRequirements,
                System.lineSeparator(),
                fulfilledResourceRequirements);
        notifyNewResourceRequirements.accept(resourceRequirements);
    }

    @Override
    public Collection<ResourceRequirement> getResourceRequirements() {
        final Collection<ResourceRequirement> currentResourceRequirements = new ArrayList<>();

        for (Map.Entry<ResourceProfile, Integer> resourceRequirement :
                totalResourceRequirements.getResourcesWithCount()) {
            currentResourceRequirements.add(
                    ResourceRequirement.create(
                            resourceRequirement.getKey(), resourceRequirement.getValue()));
        }

        return currentResourceRequirements;
    }

    @Override
    public Collection<SlotOffer> offerSlots(
            Collection<? extends SlotOffer> offers,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            long currentTime) {

        LOG.debug(
                "Received {} slot offers from TaskExecutor {}.",
                offers.size(),
                taskManagerLocation);
        final Collection<SlotOffer> acceptedSlotOffers = new ArrayList<>();
        final Collection<AllocatedSlot> acceptedSlots = new ArrayList<>();

        for (SlotOffer offer : offers) {
            if (slotPool.containsSlot(offer.getAllocationId())) {
                // we have already accepted this offer
                acceptedSlotOffers.add(offer);
            } else {
                Optional<AllocatedSlot> acceptedSlot =
                        matchOfferWithOutstandingRequirements(
                                offer, taskManagerLocation, taskManagerGateway);
                if (acceptedSlot.isPresent()) {
                    acceptedSlotOffers.add(offer);
                    acceptedSlots.add(acceptedSlot.get());
                } else {
                    LOG.debug(
                            "Could not match offer {} to any outstanding requirement.",
                            offer.getAllocationId());
                }
            }
        }

        slotPool.addSlots(acceptedSlots, currentTime);

        if (!acceptedSlots.isEmpty()) {
            LOG.debug(
                    "Acquired new resources; new total acquired resources: {}",
                    fulfilledResourceRequirements);
            newSlotsListener.notifyNewSlotsAreAvailable(acceptedSlots);
        }

        return acceptedSlotOffers;
    }

    private Optional<AllocatedSlot> matchOfferWithOutstandingRequirements(
            SlotOffer slotOffer,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway) {

        final Optional<ResourceProfile> match =
                requirementMatcher.match(
                        slotOffer.getResourceProfile(),
                        totalResourceRequirements,
                        fulfilledResourceRequirements::getResourceCount);

        if (match.isPresent()) {
            final ResourceProfile matchedRequirement = match.get();
            LOG.debug(
                    "Matched slot offer {} to requirement {}.",
                    slotOffer.getAllocationId(),
                    matchedRequirement);

            increaseAvailableResources(ResourceCounter.withResource(matchedRequirement, 1));

            final AllocatedSlot allocatedSlot =
                    createAllocatedSlot(slotOffer, taskManagerLocation, taskManagerGateway);

            // store the ResourceProfile against which the given slot has matched for future
            // book-keeping
            slotToRequirementProfileMappings.put(
                    allocatedSlot.getAllocationId(), matchedRequirement);

            return Optional.of(allocatedSlot);
        }
        return Optional.empty();
    }

    @VisibleForTesting
    ResourceCounter calculateUnfulfilledResources() {
        return totalResourceRequirements.subtract(fulfilledResourceRequirements);
    }

    private AllocatedSlot createAllocatedSlot(
            SlotOffer slotOffer,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway) {
        return new AllocatedSlot(
                slotOffer.getAllocationId(),
                taskManagerLocation,
                slotOffer.getSlotIndex(),
                slotOffer.getResourceProfile(),
                taskManagerGateway);
    }

    private void increaseAvailableResources(ResourceCounter acceptedResources) {
        fulfilledResourceRequirements = fulfilledResourceRequirements.add(acceptedResources);
    }

    @Nonnull
    private ResourceProfile getMatchingResourceProfile(AllocationID allocationId) {
        return Preconditions.checkNotNull(
                slotToRequirementProfileMappings.get(allocationId),
                "No matching resource profile found for %s",
                allocationId);
    }

    @Override
    public PhysicalSlot reserveFreeSlot(
            AllocationID allocationId, ResourceProfile requiredSlotProfile) {
        final AllocatedSlot allocatedSlot = slotPool.reserveFreeSlot(allocationId);

        Preconditions.checkState(
                allocatedSlot.getResourceProfile().isMatching(requiredSlotProfile),
                "Slot {} cannot fulfill the given requirement. SlotProfile={} Requirement={}",
                allocationId,
                allocatedSlot.getResourceProfile(),
                requiredSlotProfile);

        ResourceProfile previouslyMatchedResourceProfile =
                Preconditions.checkNotNull(slotToRequirementProfileMappings.get(allocationId));

        if (!previouslyMatchedResourceProfile.equals(requiredSlotProfile)) {
            // slots can be reserved for a requirement that is not in line with the mapping we
            // computed when the slot was
            // offered, so we have to update the mapping adjust the requirements accordingly to
            // ensure we still request enough slots to
            // be able to fulfill the total requirements
            LOG.debug(
                    "Adjusting requirements because a slot was reserved for a different requirement than initially assumed. Slot={} assumedRequirement={} actualRequirement={}",
                    allocationId,
                    previouslyMatchedResourceProfile,
                    requiredSlotProfile);
            updateSlotToRequirementProfileMapping(allocationId, requiredSlotProfile);
            adjustRequirements(previouslyMatchedResourceProfile, requiredSlotProfile);
        }

        return allocatedSlot;
    }

    @Override
    public ResourceCounter freeReservedSlot(
            AllocationID allocationId, @Nullable Throwable cause, long currentTime) {
        LOG.debug("Free reserved slot {}.", allocationId);

        final Optional<AllocatedSlot> freedSlot =
                slotPool.freeReservedSlot(allocationId, currentTime);

        Optional<ResourceCounter> previouslyFulfilledRequirement =
                freedSlot.map(Collections::singleton).map(this::getFulfilledRequirements);

        freedSlot.ifPresent(
                allocatedSlot -> {
                    releasePayload(Collections.singleton(allocatedSlot), cause);
                    newSlotsListener.notifyNewSlotsAreAvailable(
                            Collections.singletonList(allocatedSlot));
                });

        return previouslyFulfilledRequirement.orElseGet(ResourceCounter::empty);
    }

    private void updateSlotToRequirementProfileMapping(
            AllocationID allocationId, ResourceProfile matchedResourceProfile) {
        final ResourceProfile oldResourceProfile =
                Preconditions.checkNotNull(
                        slotToRequirementProfileMappings.put(allocationId, matchedResourceProfile),
                        "Expected slot profile matching to be non-empty.");

        fulfilledResourceRequirements =
                fulfilledResourceRequirements.add(matchedResourceProfile, 1);
        fulfilledResourceRequirements =
                fulfilledResourceRequirements.subtract(oldResourceProfile, 1);
    }

    private void adjustRequirements(
            ResourceProfile oldResourceProfile, ResourceProfile newResourceProfile) {
        // slots can be reserved for a requirement that is not in line with the mapping we computed
        // when the slot was
        // offered, so we have to adjust the requirements accordingly to ensure we still request
        // enough slots to
        // be able to fulfill the total requirements
        decreaseResourceRequirementsBy(ResourceCounter.withResource(newResourceProfile, 1));
        increaseResourceRequirementsBy(ResourceCounter.withResource(oldResourceProfile, 1));
    }

    @Override
    public void registerNewSlotsListener(NewSlotsListener newSlotsListener) {
        Preconditions.checkState(
                this.newSlotsListener == NoOpNewSlotsListener.INSTANCE,
                "DefaultDeclarativeSlotPool only supports a single slot listener.");
        this.newSlotsListener = newSlotsListener;
    }

    @Override
    public ResourceCounter releaseSlots(ResourceID owner, Exception cause) {
        final AllocatedSlotPool.AllocatedSlotsAndReservationStatus removedSlots =
                slotPool.removeSlots(owner);

        final Collection<AllocatedSlot> slotsToFree = new ArrayList<>();
        for (AllocatedSlot removedSlot : removedSlots.getAllocatedSlots()) {
            if (!removedSlots.wasFree(removedSlot.getAllocationId())) {
                slotsToFree.add(removedSlot);
            }
        }

        return freeAndReleaseSlots(slotsToFree, removedSlots.getAllocatedSlots(), cause);
    }

    @Override
    public ResourceCounter releaseSlot(AllocationID allocationId, Exception cause) {
        final boolean wasSlotFree = slotPool.containsFreeSlot(allocationId);
        final Optional<AllocatedSlot> removedSlot = slotPool.removeSlot(allocationId);

        if (removedSlot.isPresent()) {
            final AllocatedSlot slot = removedSlot.get();

            final Collection<AllocatedSlot> slotAsCollection = Collections.singleton(slot);
            return freeAndReleaseSlots(
                    wasSlotFree ? Collections.emptySet() : slotAsCollection,
                    slotAsCollection,
                    cause);
        } else {
            return ResourceCounter.empty();
        }
    }

    private ResourceCounter freeAndReleaseSlots(
            Collection<AllocatedSlot> currentlyReservedSlots,
            Collection<AllocatedSlot> slots,
            Exception cause) {

        ResourceCounter previouslyFulfilledRequirements =
                getFulfilledRequirements(currentlyReservedSlots);

        releasePayload(currentlyReservedSlots, cause);
        releaseSlots(slots, cause);

        return previouslyFulfilledRequirements;
    }

    private void releasePayload(Iterable<? extends AllocatedSlot> allocatedSlots, Throwable cause) {
        for (AllocatedSlot allocatedSlot : allocatedSlots) {
            allocatedSlot.releasePayload(cause);
        }
    }

    @Override
    public void releaseIdleSlots(long currentTimeMillis) {
        final Collection<AllocatedSlotPool.FreeSlotInfo> freeSlotsInformation =
                slotPool.getFreeSlotsInformation();

        ResourceCounter excessResources =
                fulfilledResourceRequirements.subtract(totalResourceRequirements);

        final Iterator<AllocatedSlotPool.FreeSlotInfo> freeSlotIterator =
                freeSlotsInformation.iterator();

        final Collection<AllocatedSlot> slotsToReturnToOwner = new ArrayList<>();

        while (!excessResources.isEmpty() && freeSlotIterator.hasNext()) {
            final AllocatedSlotPool.FreeSlotInfo idleSlot = freeSlotIterator.next();

            if (currentTimeMillis >= idleSlot.getFreeSince() + idleSlotTimeout.toMilliseconds()) {
                final ResourceProfile matchingProfile =
                        getMatchingResourceProfile(idleSlot.getAllocationId());

                if (excessResources.containsResource(matchingProfile)) {
                    excessResources = excessResources.subtract(matchingProfile, 1);
                    final Optional<AllocatedSlot> removedSlot =
                            slotPool.removeSlot(idleSlot.getAllocationId());

                    final AllocatedSlot allocatedSlot =
                            removedSlot.orElseThrow(
                                    () ->
                                            new IllegalStateException(
                                                    String.format(
                                                            "Could not find slot for allocation id %s.",
                                                            idleSlot.getAllocationId())));
                    slotsToReturnToOwner.add(allocatedSlot);
                }
            }
        }

        releaseSlots(
                slotsToReturnToOwner, new FlinkException("Returning idle slots to their owners."));
        LOG.debug(
                "Idle slots have been returned; new total acquired resources: {}",
                fulfilledResourceRequirements);
    }

    private void releaseSlots(Iterable<AllocatedSlot> slotsToReturnToOwner, Throwable cause) {
        for (AllocatedSlot slotToReturn : slotsToReturnToOwner) {
            Preconditions.checkState(!slotToReturn.isUsed(), "Free slot must not be used.");

            if (LOG.isDebugEnabled()) {
                LOG.info("Releasing slot [{}].", slotToReturn.getAllocationId(), cause);
            } else {
                LOG.info("Releasing slot [{}].", slotToReturn.getAllocationId());
            }

            final ResourceProfile matchingResourceProfile =
                    getMatchingResourceProfile(slotToReturn.getAllocationId());
            fulfilledResourceRequirements =
                    fulfilledResourceRequirements.subtract(matchingResourceProfile, 1);
            slotToRequirementProfileMappings.remove(slotToReturn.getAllocationId());

            final CompletableFuture<Acknowledge> freeSlotFuture =
                    slotToReturn
                            .getTaskManagerGateway()
                            .freeSlot(slotToReturn.getAllocationId(), cause, rpcTimeout);

            freeSlotFuture.whenComplete(
                    (Acknowledge ignored, Throwable throwable) -> {
                        if (throwable != null) {
                            // The slot status will be synced to task manager in next heartbeat.
                            LOG.debug(
                                    "Releasing slot [{}] of registered TaskExecutor {} failed. Discarding slot.",
                                    slotToReturn.getAllocationId(),
                                    slotToReturn.getTaskManagerId(),
                                    throwable);
                        }
                    });
        }
    }

    @Override
    public Collection<SlotInfoWithUtilization> getFreeSlotsInformation() {
        return slotPool.getFreeSlotsInformation().stream()
                .map(AllocatedSlotPool.FreeSlotInfo::asSlotInfo)
                .collect(Collectors.toList());
    }

    @Override
    public Collection<? extends SlotInfo> getAllSlotsInformation() {
        return slotPool.getAllSlotsInformation();
    }

    @Override
    public boolean containsFreeSlot(AllocationID allocationId) {
        return slotPool.containsFreeSlot(allocationId);
    }

    @Override
    public boolean containsSlots(ResourceID owner) {
        return slotPool.containsSlots(owner);
    }

    private ResourceCounter getFulfilledRequirements(
            Iterable<? extends AllocatedSlot> allocatedSlots) {
        ResourceCounter resourceDecrement = ResourceCounter.empty();

        for (AllocatedSlot allocatedSlot : allocatedSlots) {
            final ResourceProfile matchingResourceProfile =
                    getMatchingResourceProfile(allocatedSlot.getAllocationId());
            resourceDecrement = resourceDecrement.add(matchingResourceProfile, 1);
        }

        return resourceDecrement;
    }

    @VisibleForTesting
    ResourceCounter getFulfilledResourceRequirements() {
        return fulfilledResourceRequirements;
    }
}
