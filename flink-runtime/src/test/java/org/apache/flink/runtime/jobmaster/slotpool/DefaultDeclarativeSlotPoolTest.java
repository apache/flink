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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.QuadConsumer;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolTestUtils.createSlotOffersForResourceRequirements;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link DefaultDeclarativeSlotPool}. */
class DefaultDeclarativeSlotPoolTest {

    private static final ResourceProfile RESOURCE_PROFILE_1 =
            ResourceProfile.newBuilder().setCpuCores(1.7).build();
    private static final ResourceProfile RESOURCE_PROFILE_2 =
            ResourceProfile.newBuilder().setManagedMemoryMB(100).build();

    @Test
    void testIncreasingResourceRequirementsWillSendResourceRequirementNotification()
            throws InterruptedException {
        final NewResourceRequirementsService requirementsListener =
                new NewResourceRequirementsService();
        final DeclarativeSlotPool slotPool = createDefaultDeclarativeSlotPool(requirementsListener);

        final ResourceCounter increment1 = ResourceCounter.withResource(RESOURCE_PROFILE_1, 1);
        final ResourceCounter increment2 = createResourceRequirements();
        slotPool.increaseResourceRequirementsBy(increment1);
        slotPool.increaseResourceRequirementsBy(increment2);

        assertThat(requirementsListener.takeResourceRequirements())
                .isEqualTo(toResourceRequirements(increment1));

        final ResourceCounter totalResources = increment1.add(increment2);
        assertThat(requirementsListener.takeResourceRequirements())
                .isEqualTo(toResourceRequirements(totalResources));
        assertThat(requirementsListener.hasNextResourceRequirements()).isFalse();
    }

    @Test
    void testDecreasingResourceRequirementsWillSendResourceRequirementNotification()
            throws InterruptedException {
        final NewResourceRequirementsService requirementsListener =
                new NewResourceRequirementsService();
        final DefaultDeclarativeSlotPool slotPool =
                createDefaultDeclarativeSlotPool(requirementsListener);

        final ResourceCounter increment = ResourceCounter.withResource(RESOURCE_PROFILE_1, 3);
        slotPool.increaseResourceRequirementsBy(increment);

        requirementsListener.takeResourceRequirements();

        final ResourceCounter decrement = ResourceCounter.withResource(RESOURCE_PROFILE_1, 2);
        slotPool.decreaseResourceRequirementsBy(decrement);

        final ResourceCounter totalResources = increment.subtract(decrement);
        assertThat(requirementsListener.takeResourceRequirements())
                .isEqualTo(toResourceRequirements(totalResources));
        assertThat(requirementsListener.hasNextResourceRequirements()).isFalse();
    }

    @Test
    void testGetResourceRequirements() {
        final DefaultDeclarativeSlotPool slotPool =
                DefaultDeclarativeSlotPoolBuilder.builder().build();

        assertThat(slotPool.getResourceRequirements()).isEmpty();

        final ResourceCounter resourceRequirements = createResourceRequirements();

        slotPool.increaseResourceRequirementsBy(resourceRequirements);

        assertThat(slotPool.getResourceRequirements())
                .isEqualTo(toResourceRequirements(resourceRequirements));
    }

    @Test
    void testOfferSlots() throws InterruptedException {
        final NewSlotsService notifyNewSlots = new NewSlotsService();
        final DefaultDeclarativeSlotPool slotPool =
                createDefaultDeclarativeSlotPoolWithNewSlotsListener(notifyNewSlots);

        final ResourceCounter resourceRequirements = createResourceRequirements();

        slotPool.increaseResourceRequirementsBy(resourceRequirements);

        Collection<SlotOffer> slotOffers =
                createSlotOffersForResourceRequirements(resourceRequirements);

        final Collection<SlotOffer> acceptedSlots =
                SlotPoolTestUtils.offerSlots(slotPool, slotOffers);

        assertThat(acceptedSlots).containsExactlyInAnyOrderElementsOf(slotOffers);

        final Map<AllocationID, PhysicalSlot> newSlotsById =
                drainNewSlotService(notifyNewSlots).stream()
                        .collect(
                                Collectors.toMap(
                                        PhysicalSlot::getAllocationId, Function.identity()));

        assertThat(slotOffers)
                .hasSize(newSlotsById.size())
                .allSatisfy(
                        slotOffer -> {
                            PhysicalSlot slot = newSlotsById.get(slotOffer.getAllocationId());
                            assertThat(slot).isNotNull();
                            assertThat(slotOffer.getAllocationId())
                                    .isEqualTo(slot.getAllocationId());
                            assertThat(slotOffer.getSlotIndex())
                                    .isEqualTo(slot.getPhysicalSlotNumber());
                            assertThat(slotOffer.getResourceProfile())
                                    .isEqualTo(slot.getResourceProfile());
                        });
        assertThat(slotPool.getAllSlotsInformation())
                .hasSize(newSlotsById.size())
                .allSatisfy(
                        slotInfo -> {
                            PhysicalSlot slot = newSlotsById.get(slotInfo.getAllocationId());
                            assertThat(slot).isNotNull();
                            assertThat(slotInfo.getAllocationId())
                                    .isEqualTo(slot.getAllocationId());
                            assertThat(slotInfo.getPhysicalSlotNumber())
                                    .isEqualTo(slot.getPhysicalSlotNumber());
                            assertThat(slotInfo.getResourceProfile())
                                    .isEqualTo(slot.getResourceProfile());
                            assertThat(slotInfo.getTaskManagerLocation())
                                    .isEqualTo(slot.getTaskManagerLocation());
                        });
    }

    @Test
    void testDuplicateSlotOfferings() throws InterruptedException {
        final NewSlotsService notifyNewSlots = new NewSlotsService();
        final DefaultDeclarativeSlotPool slotPool =
                createDefaultDeclarativeSlotPoolWithNewSlotsListener(notifyNewSlots);

        final ResourceCounter resourceRequirements = createResourceRequirements();

        slotPool.increaseResourceRequirementsBy(resourceRequirements);

        final Collection<SlotOffer> slotOffers =
                createSlotOffersForResourceRequirements(resourceRequirements);

        SlotPoolTestUtils.offerSlots(slotPool, slotOffers);

        drainNewSlotService(notifyNewSlots);

        final Collection<SlotOffer> acceptedSlots =
                SlotPoolTestUtils.offerSlots(slotPool, slotOffers);

        assertThat(acceptedSlots).containsExactlyInAnyOrderElementsOf(slotOffers);
        // duplicate slots should not trigger notify new slots
        assertThat(notifyNewSlots.hasNextNewSlots()).isFalse();
    }

    @Test
    void testOfferingTooManySlotsWillRejectSuperfluousSlots() {
        final NewSlotsService notifyNewSlots = new NewSlotsService();
        final DefaultDeclarativeSlotPool slotPool =
                createDefaultDeclarativeSlotPoolWithNewSlotsListener(notifyNewSlots);

        final ResourceCounter resourceRequirements = createResourceRequirements();

        slotPool.increaseResourceRequirementsBy(resourceRequirements);

        final ResourceCounter increasedRequirements =
                resourceRequirements.add(RESOURCE_PROFILE_1, 2);

        final Collection<SlotOffer> slotOffers =
                createSlotOffersForResourceRequirements(increasedRequirements);

        final Collection<SlotOffer> acceptedSlots =
                SlotPoolTestUtils.offerSlots(slotPool, slotOffers);

        final Map<ResourceProfile, Long> resourceProfileCount =
                acceptedSlots.stream()
                        .map(SlotOffer::getResourceProfile)
                        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        assertThat(resourceRequirements.getResourcesWithCount())
                .allSatisfy(
                        resourceCount ->
                                assertThat(
                                                resourceProfileCount.getOrDefault(
                                                        resourceCount.getKey(), 0L))
                                        .isEqualTo((long) resourceCount.getValue()));
    }

    @Test
    void testReleaseSlotsRemovesSlots() throws InterruptedException {
        final NewResourceRequirementsService notifyNewResourceRequirements =
                new NewResourceRequirementsService();
        final DefaultDeclarativeSlotPool slotPool =
                createDefaultDeclarativeSlotPool(notifyNewResourceRequirements);

        final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        increaseRequirementsAndOfferSlotsToSlotPool(
                slotPool, createResourceRequirements(), taskManagerLocation);

        notifyNewResourceRequirements.takeResourceRequirements();

        slotPool.releaseSlots(
                taskManagerLocation.getResourceID(), new FlinkException("Test failure"));
        assertThat(slotPool.getAllSlotsInformation()).isEmpty();
    }

    @Test
    void testReleaseSlotsReturnsSlot() {
        final DefaultDeclarativeSlotPool slotPool =
                DefaultDeclarativeSlotPoolBuilder.builder().build();

        final ResourceCounter resourceRequirements = createResourceRequirements();

        final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        final FreeSlotConsumer freeSlotConsumer = new FreeSlotConsumer();
        final TestingTaskExecutorGateway testingTaskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setFreeSlotFunction(freeSlotConsumer)
                        .createTestingTaskExecutorGateway();

        final Collection<SlotOffer> slotOffers =
                increaseRequirementsAndOfferSlotsToSlotPool(
                        slotPool,
                        resourceRequirements,
                        taskManagerLocation,
                        testingTaskExecutorGateway);

        slotPool.releaseSlots(
                taskManagerLocation.getResourceID(), new FlinkException("Test failure"));

        final Collection<AllocationID> freedSlots = freeSlotConsumer.drainFreedSlots();

        assertThat(freedSlots)
                .containsExactlyInAnyOrderElementsOf(
                        slotOffers.stream()
                                .map(SlotOffer::getAllocationId)
                                .collect(Collectors.toList()));
    }

    @Test
    void testReleaseSlotsOnlyReturnsFulfilledRequirementsOfReservedSlots() {
        withSlotPoolContainingOneTaskManagerWithTwoSlotsWithUniqueResourceProfiles(
                (slotPool, freeSlot, slotToReserve, taskManagerLocation) -> {
                    slotPool.reserveFreeSlot(
                                    slotToReserve.getAllocationId(),
                                    slotToReserve.getResourceProfile())
                            .tryAssignPayload(new TestingPhysicalSlotPayload());

                    final ResourceCounter fulfilledRequirements =
                            slotPool.releaseSlots(
                                    taskManagerLocation.getResourceID(),
                                    new FlinkException("Test failure"));

                    assertThat(
                                    fulfilledRequirements.getResourceCount(
                                            freeSlot.getResourceProfile()))
                            .isEqualTo(0);
                    assertThat(
                                    fulfilledRequirements.getResourceCount(
                                            slotToReserve.getResourceProfile()))
                            .isEqualTo(1);
                });
    }

    @Test
    void testReleaseSlotOnlyReturnsFulfilledRequirementsOfReservedSlots() {
        withSlotPoolContainingOneTaskManagerWithTwoSlotsWithUniqueResourceProfiles(
                (slotPool, freeSlot, slotToReserve, ignored) -> {
                    slotPool.reserveFreeSlot(
                                    slotToReserve.getAllocationId(),
                                    slotToReserve.getResourceProfile())
                            .tryAssignPayload(new TestingPhysicalSlotPayload());

                    final ResourceCounter fulfilledRequirementsOfFreeSlot =
                            slotPool.releaseSlot(
                                    freeSlot.getAllocationId(), new FlinkException("Test failure"));
                    final ResourceCounter fulfilledRequirementsOfReservedSlot =
                            slotPool.releaseSlot(
                                    slotToReserve.getAllocationId(),
                                    new FlinkException("Test failure"));

                    assertThat(fulfilledRequirementsOfFreeSlot.getResources()).isEmpty();
                    assertThat(
                                    fulfilledRequirementsOfReservedSlot.getResourceCount(
                                            slotToReserve.getResourceProfile()))
                            .isEqualTo(1);
                });
    }

    private static void withSlotPoolContainingOneTaskManagerWithTwoSlotsWithUniqueResourceProfiles(
            QuadConsumer<DefaultDeclarativeSlotPool, SlotOffer, SlotOffer, TaskManagerLocation>
                    test) {
        final DefaultDeclarativeSlotPool slotPool =
                DefaultDeclarativeSlotPoolBuilder.builder().build();

        final ResourceCounter resourceRequirements =
                ResourceCounter.withResource(RESOURCE_PROFILE_1, 1).add(RESOURCE_PROFILE_2, 1);

        final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        final FreeSlotConsumer freeSlotConsumer = new FreeSlotConsumer();
        final TestingTaskExecutorGateway testingTaskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setFreeSlotFunction(freeSlotConsumer)
                        .createTestingTaskExecutorGateway();

        final Iterator<SlotOffer> slotOffers =
                increaseRequirementsAndOfferSlotsToSlotPool(
                                slotPool,
                                resourceRequirements,
                                taskManagerLocation,
                                testingTaskExecutorGateway)
                        .iterator();

        final SlotOffer slot1 = slotOffers.next();
        final SlotOffer slot2 = slotOffers.next();

        test.accept(slotPool, slot1, slot2, taskManagerLocation);
    }

    @Test
    void testReleaseSlotDecreasesFulfilledResourceRequirements() throws InterruptedException {
        final NewSlotsService notifyNewSlots = new NewSlotsService();
        final DefaultDeclarativeSlotPool slotPool =
                createDefaultDeclarativeSlotPoolWithNewSlotsListener(notifyNewSlots);

        final ResourceCounter resourceRequirements = createResourceRequirements();
        increaseRequirementsAndOfferSlotsToSlotPool(slotPool, resourceRequirements, null);

        final Collection<? extends PhysicalSlot> physicalSlots = notifyNewSlots.takeNewSlots();

        final PhysicalSlot physicalSlot = physicalSlots.iterator().next();

        slotPool.releaseSlot(physicalSlot.getAllocationId(), new FlinkException("Test failure"));

        final ResourceCounter finalResourceRequirements =
                resourceRequirements.subtract(physicalSlot.getResourceProfile(), 1);
        assertThat(slotPool.getFulfilledResourceRequirements())
                .isEqualTo(finalResourceRequirements);
    }

    @Test
    void testReleaseSlotReturnsSlot() throws InterruptedException {
        final NewSlotsService notifyNewSlots = new NewSlotsService();
        final DefaultDeclarativeSlotPool slotPool =
                createDefaultDeclarativeSlotPoolWithNewSlotsListener(notifyNewSlots);

        final ResourceCounter resourceRequirements = createResourceRequirements();
        final FreeSlotConsumer freeSlotConsumer = new FreeSlotConsumer();
        final TestingTaskExecutorGateway testingTaskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setFreeSlotFunction(freeSlotConsumer)
                        .createTestingTaskExecutorGateway();

        increaseRequirementsAndOfferSlotsToSlotPool(
                slotPool,
                resourceRequirements,
                new LocalTaskManagerLocation(),
                testingTaskExecutorGateway);

        final Collection<? extends PhysicalSlot> physicalSlots = notifyNewSlots.takeNewSlots();

        final PhysicalSlot physicalSlot = physicalSlots.iterator().next();

        slotPool.releaseSlot(physicalSlot.getAllocationId(), new FlinkException("Test failure"));

        final AllocationID freedSlot = Iterables.getOnlyElement(freeSlotConsumer.drainFreedSlots());

        assertThat(freedSlot).isEqualTo(physicalSlot.getAllocationId());
    }

    @Test
    void testReturnIdleSlotsAfterTimeout() {
        final Time idleSlotTimeout = Time.seconds(10);
        final long offerTime = 0;
        final DefaultDeclarativeSlotPool slotPool =
                DefaultDeclarativeSlotPoolBuilder.builder()
                        .setIdleSlotTimeout(idleSlotTimeout)
                        .build();

        final ResourceCounter resourceRequirements = createResourceRequirements();
        final FreeSlotConsumer freeSlotConsumer = new FreeSlotConsumer();
        final TestingTaskExecutorGateway testingTaskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setFreeSlotFunction(freeSlotConsumer)
                        .createTestingTaskExecutorGateway();

        final Collection<SlotOffer> acceptedSlots =
                increaseRequirementsAndOfferSlotsToSlotPool(
                        slotPool,
                        resourceRequirements,
                        new LocalTaskManagerLocation(),
                        testingTaskExecutorGateway);

        // decrease the resource requirements so that slots are no longer needed
        slotPool.decreaseResourceRequirementsBy(resourceRequirements);

        slotPool.releaseIdleSlots(offerTime + idleSlotTimeout.toMilliseconds());

        final Collection<AllocationID> freedSlots = freeSlotConsumer.drainFreedSlots();

        assertThat(freedSlots)
                .containsExactlyInAnyOrderElementsOf(
                        acceptedSlots.stream()
                                .map(SlotOffer::getAllocationId)
                                .collect(Collectors.toList()));
        assertNoAvailableAndRequiredResources(slotPool);
    }

    private void assertNoAvailableAndRequiredResources(DefaultDeclarativeSlotPool slotPool) {
        assertThat(slotPool.getFulfilledResourceRequirements().isEmpty()).isTrue();
        assertThat(slotPool.getResourceRequirements()).isEmpty();
        assertThat(slotPool.getAllSlotsInformation()).isEmpty();
    }

    @Test
    void testOnlyReturnExcessIdleSlots() {
        final Time idleSlotTimeout = Time.seconds(10);
        final long offerTime = 0;
        final DefaultDeclarativeSlotPool slotPool =
                DefaultDeclarativeSlotPoolBuilder.builder()
                        .setIdleSlotTimeout(idleSlotTimeout)
                        .build();

        final ResourceCounter resourceRequirements = createResourceRequirements();
        final Collection<SlotOffer> slotOffers =
                createSlotOffersForResourceRequirements(resourceRequirements);

        slotPool.increaseResourceRequirementsBy(resourceRequirements);
        final Collection<SlotOffer> acceptedSlots =
                SlotPoolTestUtils.offerSlots(slotPool, slotOffers);

        final ResourceCounter requiredResources =
                ResourceCounter.withResource(RESOURCE_PROFILE_1, 1);
        final ResourceCounter excessRequirements = resourceRequirements.subtract(requiredResources);
        slotPool.decreaseResourceRequirementsBy(excessRequirements);

        slotPool.releaseIdleSlots(offerTime + idleSlotTimeout.toMilliseconds());

        assertThat(acceptedSlots).isNotEmpty();
        assertThat(slotPool.getFulfilledResourceRequirements()).isEqualTo(requiredResources);
    }

    @Test
    void testFreedSlotWillBeUsedToFulfillOutstandingResourceRequirementsOfSameProfile()
            throws InterruptedException {
        final NewSlotsService notifyNewSlots = new NewSlotsService();
        final DefaultDeclarativeSlotPool slotPool =
                createDefaultDeclarativeSlotPoolWithNewSlotsListener(notifyNewSlots);

        final ResourceCounter initialRequirements =
                ResourceCounter.withResource(RESOURCE_PROFILE_1, 1);

        increaseRequirementsAndOfferSlotsToSlotPool(slotPool, initialRequirements, null);

        final Collection<PhysicalSlot> newSlots = drainNewSlotService(notifyNewSlots);
        final PhysicalSlot newSlot = Iterables.getOnlyElement(newSlots);

        slotPool.reserveFreeSlot(newSlot.getAllocationId(), RESOURCE_PROFILE_1);
        slotPool.freeReservedSlot(newSlot.getAllocationId(), null, 0);

        final Collection<PhysicalSlot> recycledSlots = drainNewSlotService(notifyNewSlots);

        assertThat(Iterables.getOnlyElement(recycledSlots)).isEqualTo(newSlot);

        final Collection<SlotOffer> newSlotOffers =
                createSlotOffersForResourceRequirements(initialRequirements);

        // the pending requirement should be fulfilled by the freed slot --> rejecting new slot
        // offers
        final Collection<SlotOffer> acceptedSlots =
                slotPool.offerSlots(
                        newSlotOffers,
                        new LocalTaskManagerLocation(),
                        SlotPoolTestUtils.createTaskManagerGateway(null),
                        0);

        assertThat(acceptedSlots).isEmpty();
        assertThat(slotPool.calculateUnfulfilledResources().isEmpty()).isTrue();
    }

    @Test
    void testRegisterSlotsAcceptsAllSlots() {
        final DefaultDeclarativeSlotPool declarativeSlotPool = createDefaultDeclarativeSlotPool();
        final int numberSlots = 10;
        final Collection<SlotOffer> slots =
                createSlotOffersForResourceRequirements(
                        ResourceCounter.withResource(RESOURCE_PROFILE_1, numberSlots));

        declarativeSlotPool.registerSlots(
                slots,
                new LocalTaskManagerLocation(),
                SlotPoolTestUtils.createTaskManagerGateway(null),
                0);

        final Collection<? extends SlotInfo> allSlotsInformation =
                declarativeSlotPool.getAllSlotsInformation();

        assertThat(allSlotsInformation).hasSize(numberSlots);

        assertThat(allSlotsInformation)
                .allSatisfy(
                        slotInfo ->
                                assertThat(slotInfo.getResourceProfile())
                                        .isEqualTo(RESOURCE_PROFILE_1));
    }

    @Test
    void testFreedSlotWillRemainAssignedToMatchedResourceProfile() {
        final DefaultDeclarativeSlotPool slotPool = new DefaultDeclarativeSlotPoolBuilder().build();

        final ResourceProfile largeResourceProfile =
                ResourceProfile.newBuilder().setManagedMemoryMB(1024).build();
        final ResourceProfile smallResourceProfile =
                ResourceProfile.newBuilder().setManagedMemoryMB(512).build();

        slotPool.increaseResourceRequirementsBy(
                ResourceCounter.withResource(largeResourceProfile, 1));
        SlotPoolTestUtils.offerSlots(
                slotPool,
                createSlotOffersForResourceRequirements(
                        ResourceCounter.withResource(ResourceProfile.ANY, 1)));

        final SlotInfo slot =
                slotPool.getFreeSlotInfoTracker().getFreeSlotsInformation().iterator().next();

        slotPool.reserveFreeSlot(slot.getAllocationId(), largeResourceProfile);
        assertThat(
                        slotPool.getFulfilledResourceRequirements()
                                .getResourceCount(largeResourceProfile))
                .isEqualTo(1);

        slotPool.increaseResourceRequirementsBy(
                ResourceCounter.withResource(smallResourceProfile, 1));
        slotPool.decreaseResourceRequirementsBy(
                ResourceCounter.withResource(largeResourceProfile, 1));

        // free the slot; this should not cause the slot to be automatically re-matched to the small
        // resource profile
        // this is currently the responsibility of the user, by reserving the slot for a different
        // profile
        slotPool.freeReservedSlot(slot.getAllocationId(), null, 1);
        assertThat(
                        slotPool.getFulfilledResourceRequirements()
                                .getResourceCount(largeResourceProfile))
                .isEqualTo(1);
        assertThat(
                        slotPool.getFulfilledResourceRequirements()
                                .getResourceCount(smallResourceProfile))
                .isEqualTo(0);
    }

    @Test
    void testReserveFreeSlotForResourceUpdatesAvailableResourcesAndRequirements() {
        final DefaultDeclarativeSlotPool slotPool = new DefaultDeclarativeSlotPoolBuilder().build();

        final ResourceProfile largeResourceProfile =
                ResourceProfile.newBuilder().setManagedMemoryMB(1024).build();
        final ResourceProfile smallResourceProfile = ResourceProfile.UNKNOWN;

        slotPool.increaseResourceRequirementsBy(
                ResourceCounter.withResource(largeResourceProfile, 1));
        SlotPoolTestUtils.offerSlots(
                slotPool,
                createSlotOffersForResourceRequirements(
                        ResourceCounter.withResource(largeResourceProfile, 1)));
        slotPool.increaseResourceRequirementsBy(
                ResourceCounter.withResource(smallResourceProfile, 1));

        final SlotInfo largeSlot =
                slotPool.getFreeSlotInfoTracker().getFreeSlotsInformation().stream()
                        .filter(slot -> slot.getResourceProfile().equals(largeResourceProfile))
                        .findFirst()
                        .get();

        slotPool.reserveFreeSlot(largeSlot.getAllocationId(), smallResourceProfile);

        ResourceCounter availableResources = slotPool.getFulfilledResourceRequirements();
        assertThat(availableResources.getResourceCount(smallResourceProfile)).isEqualTo(1);
        assertThat(availableResources.getResourceCount(largeResourceProfile)).isEqualTo(0);

        Collection<ResourceRequirement> currentResourceRequirements =
                slotPool.getResourceRequirements();
        // since we used one of the large slots for fulfilling another profile, we now need another
        // large slot for fulfill the original requirement
        // conversely we no longer need the small slot, because we are now using another slot for it
        assertThat(currentResourceRequirements)
                .containsExactly(ResourceRequirement.create(largeResourceProfile, 2));
    }

    @Test
    void testSetResourceRequirementsForInitialResourceRequirements() {
        final DefaultDeclarativeSlotPool slotPool = new DefaultDeclarativeSlotPoolBuilder().build();

        final ResourceCounter resourceRequirements =
                ResourceCounter.withResource(RESOURCE_PROFILE_1, 2);

        slotPool.setResourceRequirements(resourceRequirements);

        assertThat(slotPool.getResourceRequirements())
                .isEqualTo(toResourceRequirements(resourceRequirements));
    }

    @Test
    void testSetResourceRequirementsOverwritesPreviousValue() {
        final DefaultDeclarativeSlotPool slotPool = new DefaultDeclarativeSlotPoolBuilder().build();

        slotPool.setResourceRequirements(ResourceCounter.withResource(RESOURCE_PROFILE_1, 1));

        final ResourceCounter resourceRequirements =
                ResourceCounter.withResource(RESOURCE_PROFILE_2, 1);
        slotPool.setResourceRequirements(resourceRequirements);

        assertThat(slotPool.getResourceRequirements())
                .isEqualTo(toResourceRequirements(resourceRequirements));
    }

    @Test
    void testRegisterSlotsDoesNotAffectRequirements() {
        final DefaultDeclarativeSlotPool slotPool = new DefaultDeclarativeSlotPoolBuilder().build();

        final ResourceProfile slotProfile = RESOURCE_PROFILE_1;
        final ResourceProfile requestedProfile = ResourceProfile.UNKNOWN;

        slotPool.registerSlots(
                createSlotOffersForResourceRequirements(
                        ResourceCounter.withResource(slotProfile, 1)),
                new LocalTaskManagerLocation(),
                SlotPoolTestUtils.createTaskManagerGateway(null),
                0L);

        final AllocationID allocationId =
                slotPool.getFreeSlotInfoTracker().getAvailableSlots().iterator().next();

        assertThat(slotPool.getResourceRequirements()).isEmpty();

        slotPool.increaseResourceRequirementsBy(ResourceCounter.withResource(requestedProfile, 1));
        slotPool.reserveFreeSlot(allocationId, requestedProfile);
        slotPool.freeReservedSlot(allocationId, null, 1L);
        slotPool.decreaseResourceRequirementsBy(ResourceCounter.withResource(requestedProfile, 1));

        assertThat(slotPool.getResourceRequirements()).isEmpty();
    }

    @Nonnull
    static ResourceCounter createResourceRequirements() {
        final Map<ResourceProfile, Integer> requirements = new HashMap<>();
        requirements.put(RESOURCE_PROFILE_1, 2);
        requirements.put(RESOURCE_PROFILE_2, 1);

        return ResourceCounter.withResources(requirements);
    }

    @Nonnull
    private static Collection<ResourceRequirement> toResourceRequirements(
            ResourceCounter resourceCounter) {
        return resourceCounter.getResourcesWithCount().stream()
                .map(
                        resourceCount ->
                                ResourceRequirement.create(
                                        resourceCount.getKey(), resourceCount.getValue()))
                .collect(Collectors.toList());
    }

    @Nonnull
    private static DefaultDeclarativeSlotPool createDefaultDeclarativeSlotPool(
            NewResourceRequirementsService requirementsListener) {
        return DefaultDeclarativeSlotPoolBuilder.builder()
                .setNotifyNewResourceRequirements(requirementsListener)
                .build();
    }

    @Nonnull
    private static DefaultDeclarativeSlotPool createDefaultDeclarativeSlotPoolWithNewSlotsListener(
            DeclarativeSlotPool.NewSlotsListener newSlotsListener) {
        final DefaultDeclarativeSlotPool declarativeSlotPool = createDefaultDeclarativeSlotPool();

        declarativeSlotPool.registerNewSlotsListener(newSlotsListener);
        return declarativeSlotPool;
    }

    @Nonnull
    private static DefaultDeclarativeSlotPool createDefaultDeclarativeSlotPool() {
        return DefaultDeclarativeSlotPoolBuilder.builder().build();
    }

    @Nonnull
    private static Collection<SlotOffer> increaseRequirementsAndOfferSlotsToSlotPool(
            DefaultDeclarativeSlotPool slotPool,
            ResourceCounter resourceRequirements,
            @Nullable LocalTaskManagerLocation taskManagerLocation) {
        return increaseRequirementsAndOfferSlotsToSlotPool(
                slotPool, resourceRequirements, taskManagerLocation, null);
    }

    @Nonnull
    static Collection<SlotOffer> increaseRequirementsAndOfferSlotsToSlotPool(
            DefaultDeclarativeSlotPool slotPool,
            ResourceCounter resourceRequirements,
            @Nullable LocalTaskManagerLocation taskManagerLocation,
            @Nullable TaskExecutorGateway taskExecutorGateway) {
        final Collection<SlotOffer> slotOffers =
                createSlotOffersForResourceRequirements(resourceRequirements);

        slotPool.increaseResourceRequirementsBy(resourceRequirements);

        return slotPool.offerSlots(
                slotOffers,
                taskManagerLocation == null ? new LocalTaskManagerLocation() : taskManagerLocation,
                SlotPoolTestUtils.createTaskManagerGateway(taskExecutorGateway),
                0);
    }

    @Nonnull
    static Collection<PhysicalSlot> drainNewSlotService(NewSlotsService notifyNewSlots)
            throws InterruptedException {
        final Collection<PhysicalSlot> newSlots = new ArrayList<>();

        while (notifyNewSlots.hasNextNewSlots()) {
            newSlots.addAll(notifyNewSlots.takeNewSlots());
        }
        return newSlots;
    }

    private static final class NewResourceRequirementsService
            implements Consumer<Collection<ResourceRequirement>> {

        private final BlockingQueue<Collection<ResourceRequirement>> resourceRequirementsQueue =
                new ArrayBlockingQueue<>(2);

        @Override
        public void accept(Collection<ResourceRequirement> resourceRequirements) {
            resourceRequirementsQueue.offer(resourceRequirements);
        }

        private Collection<ResourceRequirement> takeResourceRequirements()
                throws InterruptedException {
            return resourceRequirementsQueue.take();
        }

        public boolean hasNextResourceRequirements() {
            return !resourceRequirementsQueue.isEmpty();
        }
    }

    static final class NewSlotsService implements DeclarativeSlotPool.NewSlotsListener {

        private final BlockingQueue<Collection<? extends PhysicalSlot>> physicalSlotsQueue =
                new ArrayBlockingQueue<>(2);

        private Collection<? extends PhysicalSlot> takeNewSlots() throws InterruptedException {
            return physicalSlotsQueue.take();
        }

        private boolean hasNextNewSlots() {
            return !physicalSlotsQueue.isEmpty();
        }

        @Override
        public void notifyNewSlotsAreAvailable(
                Collection<? extends PhysicalSlot> newlyAvailableSlots) {
            physicalSlotsQueue.offer(newlyAvailableSlots);
        }
    }

    static class FreeSlotConsumer
            implements BiFunction<AllocationID, Throwable, CompletableFuture<Acknowledge>> {

        final BlockingQueue<AllocationID> freedSlots = new ArrayBlockingQueue<>(10);

        @Override
        public CompletableFuture<Acknowledge> apply(
                AllocationID allocationID, Throwable throwable) {
            freedSlots.offer(allocationID);
            return CompletableFuture.completedFuture(Acknowledge.get());
        }

        Collection<AllocationID> drainFreedSlots() {
            final Collection<AllocationID> result = new ArrayList<>();

            freedSlots.drainTo(result);

            return result;
        }
    }
}
