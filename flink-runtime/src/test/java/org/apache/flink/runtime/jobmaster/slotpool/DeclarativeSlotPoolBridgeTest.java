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
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link DeclarativeSlotPoolBridge}. */
@ExtendWith(ParameterizedTestExtension.class)
class DeclarativeSlotPoolBridgeTest extends AbstractDeclarativeSlotPoolBridgeTest {

    @TestTemplate
    void testSlotOffer() throws Exception {
        final SlotRequestId slotRequestId = new SlotRequestId();
        final AllocationID expectedAllocationId = new AllocationID();
        final PhysicalSlot allocatedSlot = createAllocatedSlot(expectedAllocationId);

        final TestingDeclarativeSlotPoolFactory declarativeSlotPoolFactory =
                new TestingDeclarativeSlotPoolFactory(TestingDeclarativeSlotPool.builder());
        try (DeclarativeSlotPoolBridge declarativeSlotPoolBridge =
                createDeclarativeSlotPoolBridge(
                        declarativeSlotPoolFactory,
                        requestSlotMatchingStrategy,
                        slotRequestMaxInterval)) {

            declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost");

            CompletableFuture<PhysicalSlot> slotAllocationFuture =
                    declarativeSlotPoolBridge.requestNewAllocatedSlot(
                            slotRequestId, ResourceProfile.UNKNOWN, null);

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
                createDeclarativeSlotPoolBridge(
                        declarativeSlotPoolFactory,
                        requestSlotMatchingStrategy,
                        slotRequestMaxInterval)) {

            declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost");

            CompletableFuture<PhysicalSlot> slotAllocationFuture =
                    CompletableFuture.supplyAsync(
                                    () ->
                                            declarativeSlotPoolBridge.requestNewAllocatedSlot(
                                                    slotRequestId,
                                                    ResourceProfile.UNKNOWN,
                                                    Time.minutes(5)),
                                    componentMainThreadExecutor)
                            .get();

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
                createDeclarativeSlotPoolBridge(
                        declarativeSlotPoolFactory,
                        requestSlotMatchingStrategy,
                        slotRequestMaxInterval)) {
            declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost");

            final SlotRequestId slotRequestId = new SlotRequestId();

            declarativeSlotPoolBridge.allocateAvailableSlot(
                    slotRequestId, expectedAllocationId, allocatedSlot.getResourceProfile());
            declarativeSlotPoolBridge.releaseSlot(slotRequestId, null);

            assertThat(releaseSlotFuture.join()).isSameAs(expectedAllocationId);
        }
    }

    @TestTemplate
    void testNoConcurrentModificationWhenSuspendingAndReleasingSlot() throws Exception {
        try (DeclarativeSlotPoolBridge declarativeSlotPoolBridge =
                createDeclarativeSlotPoolBridge(
                        new DefaultDeclarativeSlotPoolFactory(),
                        requestSlotMatchingStrategy,
                        slotRequestMaxInterval)) {

            declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost");

            final List<SlotRequestId> slotRequestIds =
                    Arrays.asList(new SlotRequestId(), new SlotRequestId());

            final List<CompletableFuture<PhysicalSlot>> slotFutures =
                    slotRequestIds.stream()
                            .map(
                                    slotRequestId -> {
                                        final CompletableFuture<PhysicalSlot> slotFuture =
                                                declarativeSlotPoolBridge.requestNewAllocatedSlot(
                                                        slotRequestId,
                                                        ResourceProfile.UNKNOWN,
                                                        Time.fromDuration(RPC_TIMEOUT));
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

            declarativeSlotPoolBridge.close();

            assertThatThrownBy(() -> FutureUtils.waitForAll(slotFutures).get())
                    .as("The slot futures should be completed exceptionally.")
                    .isInstanceOf(ExecutionException.class);
        }
    }

    @TestTemplate
    void testAcceptingOfferedSlotsWithoutResourceManagerConnected() throws Exception {
        try (DeclarativeSlotPoolBridge declarativeSlotPoolBridge =
                createDeclarativeSlotPoolBridge(
                        new DefaultDeclarativeSlotPoolFactory(),
                        requestSlotMatchingStrategy,
                        slotRequestMaxInterval)) {

            declarativeSlotPoolBridge.start(JOB_MASTER_ID, "localhost");

            final CompletableFuture<PhysicalSlot> slotFuture =
                    declarativeSlotPoolBridge.requestNewAllocatedSlot(
                            new SlotRequestId(),
                            ResourceProfile.UNKNOWN,
                            Time.fromDuration(RPC_TIMEOUT));

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
                createDeclarativeSlotPoolBridge(
                        declarativeSlotPoolFactory,
                        requestSlotMatchingStrategy,
                        slotRequestMaxInterval)) {
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
}
