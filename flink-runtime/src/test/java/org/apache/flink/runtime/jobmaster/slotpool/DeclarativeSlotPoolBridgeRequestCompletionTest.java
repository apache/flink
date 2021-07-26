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
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.CheckedSupplier;

import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

/** Tests how the {@link DeclarativeSlotPoolBridge} completes slot requests. */
public class DeclarativeSlotPoolBridgeRequestCompletionTest extends TestLogger {

    private static final Time TIMEOUT = SlotPoolUtils.TIMEOUT;

    private TestingResourceManagerGateway resourceManagerGateway;

    @Before
    public void setUp() throws Exception {
        resourceManagerGateway = new TestingResourceManagerGateway();
    }

    /** Tests that the {@link DeclarativeSlotPoolBridge} completes slots in request order. */
    @Test
    public void testRequestsAreCompletedInRequestOrder() {
        runSlotRequestCompletionTest(
                CheckedSupplier.unchecked(this::createAndSetUpSlotPool), slotPool -> {});
    }

    /**
     * Tests that the {@link DeclarativeSlotPoolBridge} completes stashed slot requests in request
     * order.
     */
    @Test
    public void testStashOrderMaintainsRequestOrder() {
        runSlotRequestCompletionTest(
                CheckedSupplier.unchecked(this::createAndSetUpSlotPoolWithoutResourceManager),
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

            assertThat(acceptedSlots, containsInAnyOrder(slotOffer));

            final FlinkException testingReleaseException =
                    new FlinkException("Testing release exception");

            // check that the slot requests get completed in sequential order
            for (int i = 0; i < slotRequestIds.size(); i++) {
                final CompletableFuture<PhysicalSlot> slotRequestFuture = slotRequests.get(i);
                assertThat(slotRequestFuture.getNow(null), is(not(nullValue())));
                slotPool.releaseSlot(slotRequestIds.get(i), testingReleaseException);
            }
        }
    }

    private SlotPool createAndSetUpSlotPool() throws Exception {
        return new DeclarativeSlotPoolBridgeBuilder()
                .setResourceManagerGateway(resourceManagerGateway)
                .buildAndStart(ComponentMainThreadExecutorServiceAdapter.forMainThread());
    }

    private void connectToResourceManager(SlotPool slotPool) {
        slotPool.connectToResourceManager(resourceManagerGateway);
    }

    private SlotPool createAndSetUpSlotPoolWithoutResourceManager() throws Exception {
        return new DeclarativeSlotPoolBridgeBuilder()
                .setResourceManagerGateway(null)
                .buildAndStart(ComponentMainThreadExecutorServiceAdapter.forMainThread());
    }
}
