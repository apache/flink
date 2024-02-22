/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPoolTest.FreeSlotConsumer;
import static org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPoolTest.NewSlotsService;
import static org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPoolTest.createResourceRequirements;
import static org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPoolTest.drainNewSlotService;
import static org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPoolTest.increaseRequirementsAndOfferSlotsToSlotPool;
import static org.apache.flink.runtime.jobmaster.slotpool.SlotPoolTestUtils.createSlotOffersForResourceRequirements;
import static org.apache.flink.shaded.guava31.com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link BlocklistDeclarativeSlotPool}. */
class BlocklistDeclarativeSlotPoolTest {

    private static final ResourceProfile RESOURCE_PROFILE =
            ResourceProfile.newBuilder().setCpuCores(1.7).build();

    @Test
    void testOfferSlotsFromBlockedTaskManager() throws Exception {
        testOfferSlots(true);
    }

    @Test
    void testOfferSlotsFromUnblockedTaskManager() throws Exception {
        testOfferSlots(false);
    }

    private void testOfferSlots(boolean isBlocked) throws Exception {
        final TaskManagerLocation taskManager = new LocalTaskManagerLocation();

        final NewSlotsService notifyNewSlots = new NewSlotsService();
        // mark task manager as blocked.
        final BlocklistDeclarativeSlotPool slotPool =
                BlocklistDeclarativeSlotPoolBuilder.builder()
                        .setBlockedTaskManagerChecker(
                                isBlocked ? taskManager.getResourceID()::equals : ignore -> false)
                        .build();
        slotPool.registerNewSlotsListener(notifyNewSlots);

        final ResourceCounter resourceRequirements = createResourceRequirements();
        slotPool.increaseResourceRequirementsBy(resourceRequirements);

        // offer slots on the blocked task manager
        Collection<SlotOffer> slotOffers =
                createSlotOffersForResourceRequirements(resourceRequirements);

        if (isBlocked) {
            assertThat(SlotPoolTestUtils.offerSlots(slotPool, slotOffers, taskManager)).isEmpty();
            assertThat(drainNewSlotService(notifyNewSlots)).isEmpty();
        } else {
            assertThat(SlotPoolTestUtils.offerSlots(slotPool, slotOffers, taskManager))
                    .containsExactlyInAnyOrderElementsOf(slotOffers);
            Map<AllocationID, SlotOffer> slotOfferMap =
                    slotOffers.stream()
                            .collect(
                                    Collectors.toMap(
                                            SlotOffer::getAllocationId, Function.identity()));
            assertThat(drainNewSlotService(notifyNewSlots))
                    .allMatch(
                            slot ->
                                    matchSlotToOffers(
                                            slot, slotOfferMap.remove(slot.getAllocationId())));
        }
    }

    @Test
    void testOfferDuplicateSlots() {
        final TaskManagerLocation taskManager = new LocalTaskManagerLocation();
        final List<ResourceID> blockedTaskManagers = new ArrayList<>();

        final BlocklistDeclarativeSlotPool slotPool =
                BlocklistDeclarativeSlotPoolBuilder.builder()
                        .setBlockedTaskManagerChecker(blockedTaskManagers::contains)
                        .build();

        final ResourceCounter resourceRequirements =
                ResourceCounter.withResource(RESOURCE_PROFILE, 2);
        slotPool.increaseResourceRequirementsBy(resourceRequirements);

        SlotOffer slot1 = new SlotOffer(new AllocationID(), 1, RESOURCE_PROFILE);
        SlotOffer slot2 = new SlotOffer(new AllocationID(), 1, RESOURCE_PROFILE);

        // offer and accept slot1
        assertThat(
                        SlotPoolTestUtils.offerSlots(
                                slotPool, Collections.singleton(slot1), taskManager))
                .containsExactly(slot1);

        // block the task manager.
        blockedTaskManagers.add(taskManager.getResourceID());

        // offer slot1 and slot2, accept slot1, reject slot2
        assertThat(SlotPoolTestUtils.offerSlots(slotPool, Arrays.asList(slot1, slot2), taskManager))
                .containsExactly(slot1);
    }

    @Test
    void testRegisterSlotsFromBlockedTaskManager() {
        testRegisterSlots(true);
    }

    @Test
    void testRegisterSlotsFromUnblockedTaskManager() {
        testRegisterSlots(false);
    }

    private void testRegisterSlots(boolean isBlocked) {
        TaskManagerLocation taskManager = new LocalTaskManagerLocation();
        final BlocklistDeclarativeSlotPool slotPool =
                BlocklistDeclarativeSlotPoolBuilder.builder()
                        .setBlockedTaskManagerChecker(
                                isBlocked ? taskManager.getResourceID()::equals : ignore -> false)
                        .build();

        final int numberSlots = 10;
        final Collection<SlotOffer> slotOffers =
                createSlotOffersForResourceRequirements(
                        ResourceCounter.withResource(RESOURCE_PROFILE, numberSlots));

        Collection<SlotOffer> acceptedOffers =
                slotPool.registerSlots(
                        slotOffers,
                        taskManager,
                        SlotPoolTestUtils.createTaskManagerGateway(null),
                        0);

        final Collection<? extends SlotInfo> allSlotsInformation =
                slotPool.getAllSlotsInformation();

        if (isBlocked) {
            assertThat(acceptedOffers).isEmpty();
            assertThat(allSlotsInformation).isEmpty();
        } else {
            assertThat(acceptedOffers).containsExactlyInAnyOrderElementsOf(slotOffers);
            assertThat(
                            allSlotsInformation.stream()
                                    .map(SlotInfo::getAllocationId)
                                    .collect(Collectors.toSet()))
                    .isEqualTo(
                            slotOffers.stream()
                                    .map(SlotOffer::getAllocationId)
                                    .collect(Collectors.toSet()));
        }
    }

    @Test
    void testRegisterDuplicateSlots() {
        final TaskManagerLocation taskManager = new LocalTaskManagerLocation();
        final List<ResourceID> blockedTaskManagers = new ArrayList<>();

        final BlocklistDeclarativeSlotPool slotPool =
                BlocklistDeclarativeSlotPoolBuilder.builder()
                        .setBlockedTaskManagerChecker(blockedTaskManagers::contains)
                        .build();

        SlotOffer slot1 = new SlotOffer(new AllocationID(), 1, RESOURCE_PROFILE);
        SlotOffer slot2 = new SlotOffer(new AllocationID(), 1, RESOURCE_PROFILE);

        // register and accept slot1
        Collection<SlotOffer> acceptedOffers =
                slotPool.registerSlots(
                        Collections.singleton(slot1),
                        taskManager,
                        SlotPoolTestUtils.createTaskManagerGateway(null),
                        0);
        assertThat(acceptedOffers).containsExactly(slot1);

        // block the task manager
        blockedTaskManagers.add(taskManager.getResourceID());

        // register slot1 and slot2, accept slot1, reject slot2
        acceptedOffers =
                slotPool.registerSlots(
                        Arrays.asList(slot1, slot2),
                        taskManager,
                        SlotPoolTestUtils.createTaskManagerGateway(null),
                        0);
        assertThat(acceptedOffers).containsExactly(slot1);
    }

    @Test
    void testFreeReservedSlotsOnBlockedTaskManager() throws Exception {
        testFreeReservedSlots(true);
    }

    @Test
    void testFreeReservedSlotsOnUnblockedTaskManager() throws Exception {
        testFreeReservedSlots(false);
    }

    private void testFreeReservedSlots(boolean isBlocked) throws Exception {
        final FreeSlotConsumer freeSlotConsumer = new FreeSlotConsumer();
        final TestingTaskExecutorGateway testingTaskExecutorGateway =
                new TestingTaskExecutorGatewayBuilder()
                        .setFreeSlotFunction(freeSlotConsumer)
                        .createTestingTaskExecutorGateway();

        final NewSlotsService notifyNewSlots = new NewSlotsService();
        Set<ResourceID> blockedTaskManagers = new HashSet<>();
        final BlocklistDeclarativeSlotPool slotPool =
                BlocklistDeclarativeSlotPoolBuilder.builder()
                        .setBlockedTaskManagerChecker(blockedTaskManagers::contains)
                        .build();
        slotPool.registerNewSlotsListener(notifyNewSlots);

        increaseRequirementsAndOfferSlotsToSlotPool(
                slotPool,
                ResourceCounter.withResource(RESOURCE_PROFILE, 1),
                null,
                testingTaskExecutorGateway);

        final Collection<PhysicalSlot> newSlots = drainNewSlotService(notifyNewSlots);
        final PhysicalSlot offeredSlot = getOnlyElement(newSlots);
        final AllocationID allocationID = offeredSlot.getAllocationId();

        slotPool.reserveFreeSlot(allocationID, RESOURCE_PROFILE);

        if (isBlocked) {
            // block TM
            blockedTaskManagers.add(offeredSlot.getTaskManagerLocation().getResourceID());
        }

        ResourceCounter previouslyFulfilledRequirement =
                slotPool.freeReservedSlot(allocationID, null, 0);

        final Collection<PhysicalSlot> recycledSlots = drainNewSlotService(notifyNewSlots);

        assertThat(previouslyFulfilledRequirement)
                .isEqualTo(ResourceCounter.withResource(RESOURCE_PROFILE, 1));

        if (isBlocked) {
            assertThat(recycledSlots).isEmpty();
            assertThat(getOnlyElement(freeSlotConsumer.drainFreedSlots())).isEqualTo(allocationID);
            assertThat(slotPool.getAllSlotsInformation()).isEmpty();
        } else {
            assertThat(getOnlyElement(recycledSlots).getAllocationId()).isEqualTo(allocationID);
            assertThat(freeSlotConsumer.drainFreedSlots()).isEmpty();
            assertThat(getOnlyElement(slotPool.getAllSlotsInformation()).getAllocationId())
                    .isEqualTo(allocationID);
        }
    }

    private boolean matchSlotToOffers(PhysicalSlot physicalSlot, SlotOffer slotOffer) {
        return physicalSlot.getAllocationId().equals(slotOffer.getAllocationId())
                && physicalSlot.getResourceProfile().equals(slotOffer.getResourceProfile())
                && physicalSlot.getPhysicalSlotNumber() == slotOffer.getSlotIndex();
    }

    private static class BlocklistDeclarativeSlotPoolBuilder {
        private BlockedTaskManagerChecker blockedTaskManagerChecker = resourceID -> false;

        public BlocklistDeclarativeSlotPoolBuilder setBlockedTaskManagerChecker(
                BlockedTaskManagerChecker blockedTaskManagerChecker) {
            this.blockedTaskManagerChecker = blockedTaskManagerChecker;
            return this;
        }

        public BlocklistDeclarativeSlotPool build() {
            return new BlocklistDeclarativeSlotPool(
                    new JobID(),
                    new DefaultAllocatedSlotPool(),
                    ignored -> {},
                    blockedTaskManagerChecker,
                    Time.seconds(20),
                    Time.seconds(20));
        }

        public static BlocklistDeclarativeSlotPoolBuilder builder() {
            return new BlocklistDeclarativeSlotPoolBuilder();
        }
    }
}
