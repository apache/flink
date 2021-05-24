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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.RpcTaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.clock.SystemClock;

import org.junit.Test;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Tests for the {@link DeclarativeSlotPoolBridge}. */
public class DeclarativeSlotPoolBridgeTest extends TestLogger {

    private static final Time rpcTimeout = Time.seconds(20);
    private static final JobID jobId = new JobID();
    private static final JobMasterId jobMasterId = JobMasterId.generate();
    private final ComponentMainThreadExecutor mainThreadExecutor =
            ComponentMainThreadExecutorServiceAdapter.forMainThread();

    @Test
    public void testSlotOffer() throws Exception {
        final SlotRequestId slotRequestId = new SlotRequestId();
        final AllocationID expectedAllocationId = new AllocationID();
        final PhysicalSlot allocatedSlot = createAllocatedSlot(expectedAllocationId);

        final TestingDeclarativeSlotPoolFactory declarativeSlotPoolFactory =
                new TestingDeclarativeSlotPoolFactory(TestingDeclarativeSlotPool.builder());
        try (DeclarativeSlotPoolBridge declarativeSlotPoolBridge =
                createDeclarativeSlotPoolBridge(declarativeSlotPoolFactory)) {

            declarativeSlotPoolBridge.start(jobMasterId, "localhost", mainThreadExecutor);

            CompletableFuture<PhysicalSlot> slotAllocationFuture =
                    declarativeSlotPoolBridge.requestNewAllocatedSlot(
                            slotRequestId, ResourceProfile.UNKNOWN, null);

            declarativeSlotPoolBridge.newSlotsAreAvailable(Collections.singleton(allocatedSlot));

            slotAllocationFuture.join();
        }
    }

    @Test
    public void testNotEnoughResourcesAvailableFailsPendingRequests() throws Exception {
        final SlotRequestId slotRequestId = new SlotRequestId();

        final TestingDeclarativeSlotPoolFactory declarativeSlotPoolFactory =
                new TestingDeclarativeSlotPoolFactory(TestingDeclarativeSlotPool.builder());
        try (DeclarativeSlotPoolBridge declarativeSlotPoolBridge =
                createDeclarativeSlotPoolBridge(declarativeSlotPoolFactory)) {

            declarativeSlotPoolBridge.start(jobMasterId, "localhost", mainThreadExecutor);

            CompletableFuture<PhysicalSlot> slotAllocationFuture =
                    CompletableFuture.supplyAsync(
                                    () ->
                                            declarativeSlotPoolBridge.requestNewAllocatedSlot(
                                                    slotRequestId,
                                                    ResourceProfile.UNKNOWN,
                                                    Time.minutes(5)),
                                    mainThreadExecutor)
                            .get();

            mainThreadExecutor.execute(
                    () ->
                            declarativeSlotPoolBridge.notifyNotEnoughResourcesAvailable(
                                    Collections.emptyList()));

            assertThat(
                    slotAllocationFuture,
                    FlinkMatchers.futureWillCompleteExceptionally(
                            NoResourceAvailableException.class, Duration.ofSeconds(10)));
        }
    }

    @Test
    public void testReleasingAllocatedSlot() throws Exception {
        final CompletableFuture<AllocationID> releaseSlotFuture = new CompletableFuture<>();
        final AllocationID expectedAllocationId = new AllocationID();
        final PhysicalSlot allocatedSlot = createAllocatedSlot(expectedAllocationId);

        final TestingDeclarativeSlotPoolBuilder builder =
                TestingDeclarativeSlotPool.builder()
                        .setReserveFreeSlotFunction(
                                (allocationId, resourceProfile) -> {
                                    assertThat(allocationId, is(expectedAllocationId));
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
            declarativeSlotPoolBridge.start(jobMasterId, "localhost", mainThreadExecutor);

            final SlotRequestId slotRequestId = new SlotRequestId();

            declarativeSlotPoolBridge.allocateAvailableSlot(
                    slotRequestId, expectedAllocationId, allocatedSlot.getResourceProfile());
            declarativeSlotPoolBridge.releaseSlot(slotRequestId, null);

            assertThat(releaseSlotFuture.join(), is(expectedAllocationId));
        }
    }

    @Test
    public void testNoConcurrentModificationWhenSuspendingAndReleasingSlot() throws Exception {
        try (DeclarativeSlotPoolBridge declarativeSlotPoolBridge =
                createDeclarativeSlotPoolBridge(new DefaultDeclarativeSlotPoolFactory())) {

            declarativeSlotPoolBridge.start(jobMasterId, "localhost", mainThreadExecutor);

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
                                                        rpcTimeout);
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

            try {
                FutureUtils.waitForAll(slotFutures).get();
                fail("The slot futures should be completed exceptionally.");
            } catch (ExecutionException expected) {
                // expected
            }
        }
    }

    @Nonnull
    static DeclarativeSlotPoolBridge createDeclarativeSlotPoolBridge(
            DeclarativeSlotPoolFactory declarativeSlotPoolFactory) {
        return new DeclarativeSlotPoolBridge(
                jobId,
                declarativeSlotPoolFactory,
                SystemClock.getInstance(),
                rpcTimeout,
                Time.seconds(20),
                Time.seconds(20));
    }

    static PhysicalSlot createAllocatedSlot(AllocationID allocationID) {
        return new AllocatedSlot(
                allocationID,
                new LocalTaskManagerLocation(),
                0,
                ResourceProfile.ANY,
                new RpcTaskManagerGateway(
                        new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway(),
                        JobMasterId.generate()));
    }
}
