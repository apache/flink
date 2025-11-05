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
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link DeclarativeSlotPoolBridge}. */
@ExtendWith(ParameterizedTestExtension.class)
class DeclarativeSlotPoolBridgeTest extends AbstractDeclarativeSlotPoolBridgeTest {
    private RequirementListener requirementListener;

    @BeforeEach
    void setup() {
        requirementListener =
                new RequirementListener(componentMainThreadExecutor, slotRequestMaxInterval);
    }

    @TestTemplate
    void testSlotOffer() throws Exception {
        final SlotRequestId slotRequestId = new SlotRequestId();
        final AllocationID expectedAllocationId = new AllocationID();
        final PhysicalSlot allocatedSlot = createAllocatedSlot(expectedAllocationId);

        final TestingDeclarativeSlotPoolFactory declarativeSlotPoolFactory =
                new TestingDeclarativeSlotPoolFactory(
                        TestingDeclarativeSlotPool.builder()
                                .setReserveFreeSlotFunction(
                                        (allocationID, resourceProfile) -> allocatedSlot)
                                .setGetFreeSlotTrackerSupplier(
                                        () ->
                                                TestingFreeSlotTracker.newBuilder()
                                                        .setGetFreeSlotsInformationSupplier(
                                                                () ->
                                                                        Collections.singleton(
                                                                                allocatedSlot))
                                                        .setGetAvailableSlotsSupplier(
                                                                () ->
                                                                        Collections.singleton(
                                                                                allocatedSlot
                                                                                        .getAllocationId()))
                                                        .build())
                                .setContainsFreeSlotFunction(ignoredId -> true)
                                .setIncreaseResourceRequirementsByConsumer(
                                        requirementListener::increaseRequirements)
                                .setDecreaseResourceRequirementsByConsumer(
                                        requirementListener::decreaseRequirements));
        try (DeclarativeSlotPoolBridge declarativeSlotPoolBridge =
                createDeclarativeSlotPoolBridge(
                        declarativeSlotPoolFactory, componentMainThreadExecutor)) {

            declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost");

            CompletableFuture<PhysicalSlot> slotAllocationFuture =
                    declarativeSlotPoolBridge.requestNewAllocatedSlot(
                            PhysicalSlotRequestUtils.normalRequest(
                                    slotRequestId, ResourceProfile.UNKNOWN),
                            null);

            requirementListener.tryWaitSlotRequestIsDone();

            declarativeSlotPoolBridge.newSlotsAreAvailable(Collections.singleton(allocatedSlot));

            slotAllocationFuture.join();
        }
    }

    @TestTemplate
    void testNotEnoughResourcesAvailableFailsPendingRequests() throws Exception {
        final SlotRequestId slotRequestId = new SlotRequestId();

        final TestingDeclarativeSlotPoolFactory declarativeSlotPoolFactory =
                new TestingDeclarativeSlotPoolFactory(TestingDeclarativeSlotPool.builder());
        try (DeclarativeSlotPoolBridge declarativeSlotPoolBridge =
                createDeclarativeSlotPoolBridge(declarativeSlotPoolFactory)) {

            declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost");

            CompletableFuture<PhysicalSlot> slotAllocationFuture =
                    CompletableFuture.supplyAsync(
                                    () ->
                                            declarativeSlotPoolBridge.requestNewAllocatedSlot(
                                                    PhysicalSlotRequestUtils.normalRequest(
                                                            slotRequestId, ResourceProfile.UNKNOWN),
                                                    Duration.ofMinutes(5)),
                                    componentMainThreadExecutor)
                            .get();

            tryWaitSlotRequestIsDone(declarativeSlotPoolBridge);

            componentMainThreadExecutor.execute(
                    () ->
                            declarativeSlotPoolBridge.notifyNotEnoughResourcesAvailable(
                                    Collections.emptyList()));

            assertThatFuture(slotAllocationFuture).failsWithin(Duration.ofSeconds(10));
        }
    }

    @TestTemplate
    void testReleasingAllocatedSlot() throws Exception {
        final CompletableFuture<AllocationID> releaseSlotFuture = new CompletableFuture<>();
        final AllocationID expectedAllocationId = new AllocationID();
        final PhysicalSlot allocatedSlot = createAllocatedSlot(expectedAllocationId);

        final TestingDeclarativeSlotPoolBuilder builder =
                TestingDeclarativeSlotPool.builder()
                        .setReserveFreeSlotFunction(
                                (allocationId, resourceProfile) -> {
                                    assertThat(allocationId).isSameAs(expectedAllocationId);
                                    return allocatedSlot;
                                })
                        .setFreeReservedSlotFunction(
                                (allocationID, throwable, aLong) -> {
                                    releaseSlotFuture.complete(allocationID);
                                    return ResourceCounter.empty();
                                });

        final TestingDeclarativeSlotPoolFactory declarativeSlotPoolFactory =
                new TestingDeclarativeSlotPoolFactory(builder);
        try (DeclarativeSlotPoolBridge declarativeSlotPoolBridge =
                createDeclarativeSlotPoolBridge(declarativeSlotPoolFactory)) {
            declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost");

            final SlotRequestId slotRequestId = new SlotRequestId();

            declarativeSlotPoolBridge.allocateAvailableSlot(
                    expectedAllocationId,
                    PhysicalSlotRequestUtils.normalRequest(
                            slotRequestId, allocatedSlot.getResourceProfile()));

            tryWaitSlotRequestIsDone(declarativeSlotPoolBridge);

            declarativeSlotPoolBridge.releaseSlot(slotRequestId, null);

            assertThat(releaseSlotFuture.join()).isSameAs(expectedAllocationId);
        }
    }

    @TestTemplate
    void testNoConcurrentModificationWhenSuspendingAndReleasingSlot() throws Exception {
        try (DeclarativeSlotPoolBridge declarativeSlotPoolBridge =
                createDeclarativeSlotPoolBridge(new DefaultDeclarativeSlotPoolFactory())) {

            declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost");

            final List<SlotRequestId> slotRequestIds =
                    Arrays.asList(new SlotRequestId(), new SlotRequestId());

            final List<CompletableFuture<PhysicalSlot>> slotFutures =
                    slotRequestIds.stream()
                            .map(
                                    slotRequestId -> {
                                        final CompletableFuture<PhysicalSlot> slotFuture =
                                                declarativeSlotPoolBridge.requestNewAllocatedSlot(
                                                        PhysicalSlotRequestUtils.normalRequest(
                                                                slotRequestId,
                                                                ResourceProfile.UNKNOWN),
                                                        RPC_TIMEOUT);
                                        slotFuture.whenComplete(
                                                (physicalSlot, throwable) -> {
                                                    if (throwable != null) {
                                                        declarativeSlotPoolBridge.releaseSlot(
                                                                slotRequestId, throwable);
                                                    }
                                                });
                                        return slotFuture;
                                    })
                            .collect(Collectors.toList());

            tryWaitSlotRequestIsDone(declarativeSlotPoolBridge);

            declarativeSlotPoolBridge.close();

            assertThatThrownBy(() -> FutureUtils.waitForAll(slotFutures).get())
                    .as("The slot futures should be completed exceptionally.")
                    .isInstanceOf(ExecutionException.class);
        }
    }

    @TestTemplate
    void testAcceptingOfferedSlotsWithoutResourceManagerConnected() throws Exception {
        try (DeclarativeSlotPoolBridge declarativeSlotPoolBridge =
                createDeclarativeSlotPoolBridge(new DefaultDeclarativeSlotPoolFactory())) {

            declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost");

            final CompletableFuture<PhysicalSlot> slotFuture =
                    declarativeSlotPoolBridge.requestNewAllocatedSlot(
                            PhysicalSlotRequestUtils.normalRequest(ResourceProfile.UNKNOWN),
                            RPC_TIMEOUT);

            tryWaitSlotRequestIsDone(declarativeSlotPoolBridge);

            final LocalTaskManagerLocation localTaskManagerLocation =
                    new LocalTaskManagerLocation();
            declarativeSlotPoolBridge.registerTaskManager(localTaskManagerLocation.getResourceID());

            final AllocationID allocationId = new AllocationID();
            declarativeSlotPoolBridge.offerSlots(
                    localTaskManagerLocation,
                    new SimpleAckingTaskManagerGateway(),
                    Collections.singleton(new SlotOffer(allocationId, 0, ResourceProfile.ANY)));

            assertThat(slotFuture.join().getAllocationId()).isSameAs(allocationId);
        }
    }

    @TestTemplate
    void testIfJobIsRestartingAllOfferedSlotsWillBeRegistered() throws Exception {
        final CompletableFuture<Void> registerSlotsCalledFuture = new CompletableFuture<>();
        final TestingDeclarativeSlotPoolFactory declarativeSlotPoolFactory =
                new TestingDeclarativeSlotPoolFactory(
                        TestingDeclarativeSlotPool.builder()
                                .setRegisterSlotsFunction(
                                        (slotOffers,
                                                taskManagerLocation,
                                                taskManagerGateway,
                                                aLong) -> {
                                            registerSlotsCalledFuture.complete(null);
                                            return new ArrayList<>(slotOffers);
                                        }));

        try (DeclarativeSlotPoolBridge declarativeSlotPoolBridge =
                createDeclarativeSlotPoolBridge(declarativeSlotPoolFactory)) {
            declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost");

            declarativeSlotPoolBridge.setIsJobRestarting(true);

            final LocalTaskManagerLocation localTaskManagerLocation =
                    new LocalTaskManagerLocation();
            declarativeSlotPoolBridge.registerTaskManager(localTaskManagerLocation.getResourceID());

            declarativeSlotPoolBridge.offerSlots(
                    localTaskManagerLocation,
                    new SimpleAckingTaskManagerGateway(),
                    Collections.singleton(
                            new SlotOffer(new AllocationID(), 0, ResourceProfile.ANY)));

            // make sure that the register slots method is called
            registerSlotsCalledFuture.join();
        }
    }

    @TestTemplate
    void testDeferSlotAllocationLogic() throws Exception {
        testDeferSlotAllocationLogic(1);
        testDeferSlotAllocationLogic(2);
        testDeferSlotAllocationLogic(4);
        testDeferSlotAllocationLogic(7);
        testDeferSlotAllocationLogic(10);
        testDeferSlotAllocationLogic(32);
    }

    private void testDeferSlotAllocationLogic(int requestSlotNum) throws Exception {

        final Set<AllocationID> availableSlotsIds = new HashSet<>();
        final Set<PhysicalSlot> freeSlotsInformation = new HashSet<>();

        try (DeclarativeSlotPoolBridge slotPoolBridge =
                createDeclarativeSlotPoolBridge(freeSlotsInformation, availableSlotsIds)) {

            slotPoolBridge.start(JOB_MASTER_ID, "localhost");

            final List<CompletableFuture<PhysicalSlot>> futures = new ArrayList<>(requestSlotNum);
            for (int i = 0; i < requestSlotNum; i++) {
                futures.add(
                        slotPoolBridge.requestNewAllocatedSlot(
                                PhysicalSlotRequestUtils.normalRequest(ResourceProfile.UNKNOWN),
                                null));
            }

            tryWaitSlotRequestIsDone(slotPoolBridge);

            for (int i = 0; i < requestSlotNum; i++) {
                final PhysicalSlot slot = createAllocatedSlot(new AllocationID());
                newSlotsAreAvailable(slotPoolBridge, freeSlotsInformation, availableSlotsIds, slot);
                if (deferSlotAllocation) {
                    checkForSlotBatchAllocating(requestSlotNum, i, futures);
                } else {
                    // Check for allocating slots directly.
                    futures.get(i).join();
                }
            }
        }
    }

    private void checkForSlotBatchAllocating(
            int requestSlotNum, int requestIndex, List<CompletableFuture<PhysicalSlot>> futures) {
        if (requestIndex < requestSlotNum - 1) {
            assertThat(FutureUtils.waitForAll(futures).getNumFuturesCompleted()).isZero();
        } else {
            FutureUtils.waitForAll(futures).join();
        }
    }

    private void newSlotsAreAvailable(
            DeclarativeSlotPoolBridge declarativeSlotPoolBridge,
            Set<PhysicalSlot> freeSlotsInformation,
            Set<AllocationID> availableSlotsIds,
            PhysicalSlot slot) {
        freeSlotsInformation.add(slot);
        availableSlotsIds.add(slot.getAllocationId());
        declarativeSlotPoolBridge.newSlotsAreAvailable(Collections.singleton(slot));
    }

    private void tryWaitSlotRequestIsDone(DeclarativeSlotPoolBridge declarativeSlotPoolBridge) {
        if (declarativeSlotPoolBridge.getDeclarativeSlotPool()
                instanceof DefaultDeclarativeSlotPool) {
            final DefaultDeclarativeSlotPool slotPool =
                    (DefaultDeclarativeSlotPool) declarativeSlotPoolBridge.getDeclarativeSlotPool();
            slotPool.tryWaitSlotRequestIsDone();
        }
    }

    private DeclarativeSlotPoolBridge createDeclarativeSlotPoolBridge(
            Set<PhysicalSlot> freeSlotsInformation, Set<AllocationID> availableSlotsIds) {
        final TestingDeclarativeSlotPoolFactory declarativeSlotPoolFactory =
                new TestingDeclarativeSlotPoolFactory(
                        TestingDeclarativeSlotPool.builder()
                                .setGetFreeSlotTrackerSupplier(
                                        () ->
                                                TestingFreeSlotTracker.newBuilder()
                                                        .setGetFreeSlotsInformationSupplier(
                                                                () -> freeSlotsInformation)
                                                        .setGetAvailableSlotsSupplier(
                                                                () -> availableSlotsIds)
                                                        .build())
                                .setReserveFreeSlotFunction(
                                        (allocationID, resourceProfile) ->
                                                freeSlotsInformation.stream()
                                                        .collect(
                                                                Collectors.toMap(
                                                                        SlotInfo::getAllocationId,
                                                                        Function.identity()))
                                                        .get(allocationID)));
        return createDeclarativeSlotPoolBridge(declarativeSlotPoolFactory);
    }
}
