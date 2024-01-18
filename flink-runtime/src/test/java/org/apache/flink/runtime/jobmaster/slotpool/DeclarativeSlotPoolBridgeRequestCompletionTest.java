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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.CheckedSupplier;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests how the {@link DeclarativeSlotPoolBridge} completes slot requests. */
@ExtendWith(ParameterizedTestExtension.class)
class DeclarativeSlotPoolBridgeRequestCompletionTest {

    private static final Time TIMEOUT = SlotPoolUtils.TIMEOUT;

    private TestingResourceManagerGateway resourceManagerGateway;

    @Parameter private boolean slotBatchAllocatable;

    @Parameters(name = "slotBatchAllocatable: {0}")
    public static List<Boolean> getSlotBatchAllocatableParams() {
        return Lists.newArrayList(false, true);
    }

    @BeforeEach
    void setUp() {
        resourceManagerGateway = new TestingResourceManagerGateway();
    }

    /** Tests that the {@link DeclarativeSlotPoolBridge} completes slots in request order. */
    @TestTemplate
    void testRequestsAreCompletedInRequestOrder() {
        runSlotRequestCompletionTest(
                CheckedSupplier.unchecked(() -> createAndSetUpSlotPool(slotBatchAllocatable)),
                slotPool -> {});
    }

    /**
     * Tests that the {@link DeclarativeSlotPoolBridge} completes stashed slot requests in request
     * order.
     */
    @TestTemplate
    void testStashOrderMaintainsRequestOrder() {
        runSlotRequestCompletionTest(
                CheckedSupplier.unchecked(
                        () -> createAndSetUpSlotPoolWithoutResourceManager(slotBatchAllocatable)),
                this::connectToResourceManager);
    }

    private void runSlotRequestCompletionTest(
            Supplier<SlotPool> slotPoolSupplier, Consumer<SlotPool> actionAfterSlotRequest) {
        try (final SlotPool slotPool = slotPoolSupplier.get()) {

            final int requestNum = 10;

            final List<SlotRequestId> slotRequestIds =
                    IntStream.range(0, requestNum)
                            .mapToObj(ignored -> new SlotRequestId())
                            .collect(Collectors.toList());

            final List<CompletableFuture<PhysicalSlot>> slotRequests =
                    slotRequestIds.stream()
                            .map(
                                    slotRequestId ->
                                            slotPool.requestNewAllocatedSlot(
                                                    slotRequestId,
                                                    ResourceProfile.UNKNOWN,
                                                    LoadingWeight.EMPTY,
                                                    TIMEOUT))
                            .collect(Collectors.toList());

            actionAfterSlotRequest.accept(slotPool);

            final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
            slotPool.registerTaskManager(taskManagerLocation.getResourceID());

            // create a slot offer that is initiated by the last request
            final SlotOffer slotOffer = new SlotOffer(new AllocationID(), 0, ResourceProfile.ANY);
            final Collection<SlotOffer> acceptedSlots =
                    slotPool.offerSlots(
                            taskManagerLocation,
                            new SimpleAckingTaskManagerGateway(),
                            Collections.singleton(slotOffer));

            assertThat(acceptedSlots).contains(slotOffer);

            final FlinkException testingReleaseException =
                    new FlinkException("Testing release exception");

            DeclarativeSlotPoolBridge slotPoolBridge = (DeclarativeSlotPoolBridge) slotPool;

            // check that the slot requests get completed in sequential order
            for (int i = 0; i < slotRequestIds.size(); i++) {
                final CompletableFuture<PhysicalSlot> slotRequestFuture = slotRequests.get(i);
                if (slotBatchAllocatable) {
                    assertThat(slotRequestFuture.getNow(null)).isNull();
                    assertThat(slotPoolBridge.getReceivedSlots()).hasSize(1);
                } else {
                    assertThat(slotRequestFuture.getNow(null)).isNotNull();
                }
                slotPool.releaseSlot(slotRequestIds.get(i), testingReleaseException);
            }
        }
    }

    private SlotPool createAndSetUpSlotPool(boolean slotBatchAllocatable) throws Exception {
        return new DeclarativeSlotPoolBridgeBuilder()
                .setResourceManagerGateway(resourceManagerGateway)
                .buildAndStart(
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        slotBatchAllocatable);
    }

    private void connectToResourceManager(SlotPool slotPool) {
        slotPool.connectToResourceManager(resourceManagerGateway);
    }

    private SlotPool createAndSetUpSlotPoolWithoutResourceManager(boolean slotBatchAllocatable)
            throws Exception {
        return new DeclarativeSlotPoolBridgeBuilder()
                .setResourceManagerGateway(null)
                .buildAndStart(
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        slotBatchAllocatable);
    }
}
