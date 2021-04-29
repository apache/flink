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

import com.google.common.collect.Lists;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.TestingComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the SlotPoolImpl interactions. */
public class SlotPoolInteractionsTest extends TestLogger {

    private static final Time fastTimeout = Time.milliseconds(1L);

    @ClassRule
    public static final TestingComponentMainThreadExecutor.Resource EXECUTOR_RESOURCE =
            new TestingComponentMainThreadExecutor.Resource(10L);

    private final TestingComponentMainThreadExecutor testMainThreadExecutor =
            EXECUTOR_RESOURCE.getComponentMainThreadTestExecutor();

    // ------------------------------------------------------------------------
    //  tests
    // ------------------------------------------------------------------------

    @Test
    public void testSlotAllocationNoResourceManager() throws Exception {

        try (SlotPool pool = createAndSetUpSlotPoolWithoutResourceManager()) {

            final CompletableFuture<PhysicalSlot> future =
                    testMainThreadExecutor.execute(
                            () ->
                                    pool.requestNewAllocatedSlot(
                                            new SlotRequestId(),
                                            ResourceProfile.UNKNOWN,
                                            fastTimeout));

            try {
                future.get();
                fail("We expected an ExecutionException.");
            } catch (ExecutionException e) {
                assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
            }
        }
    }

    @Test
    public void testCancelSlotAllocationWithoutResourceManager() throws Exception {

        try (SlotPool pool = createAndSetUpSlotPoolWithoutResourceManager()) {

            final CompletableFuture<SlotRequestId> timeoutFuture = new CompletableFuture<>();

            final CompletableFuture<PhysicalSlot> future =
                    testMainThreadExecutor.execute(
                            () ->
                                    pool.requestNewAllocatedSlot(
                                            new SlotRequestId(),
                                            ResourceProfile.UNKNOWN,
                                            fastTimeout));

            try {
                future.get();
                fail("We expected a TimeoutException.");
            } catch (ExecutionException e) {
                assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
            }

            // wait for the timeout of the pending slot request
            timeoutFuture.get();

            assertEquals(0L, pool.getAllocatedSlotsInformation().size());
        }
    }

    /** Tests that a slot allocation times out wrt to the specified time out. */
    @Test
    public void testSlotAllocationTimeout() throws Exception {

        try (SlotPool pool = createAndSetUpSlotPool()) {

            final CompletableFuture<SlotRequestId> slotRequestTimeoutFuture =
                    new CompletableFuture<>();

            final CompletableFuture<PhysicalSlot> future =
                    testMainThreadExecutor.execute(
                            () ->
                                    pool.requestNewAllocatedSlot(
                                            new SlotRequestId(),
                                            ResourceProfile.UNKNOWN,
                                            fastTimeout));

            try {
                future.get();
                fail("We expected a TimeoutException.");
            } catch (ExecutionException e) {
                assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
            }

            // wait until we have timed out the slot request
            slotRequestTimeoutFuture.get();

            assertEquals(0L, pool.getAllocatedSlotsInformation().size());
        }
    }

    /** Tests that extra slots are kept by the {@link SlotPool}. */
    @Test
    public void testExtraSlotsAreKept() throws Exception {
        final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();

        TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
        resourceManagerGateway.setRequestSlotConsumer(
                (SlotRequest slotRequest) ->
                        allocationIdFuture.complete(slotRequest.getAllocationId()));

        try (SlotPool pool =
                new SlotPoolBuilder(testMainThreadExecutor.getMainThreadExecutor())
                        .setResourceManagerGateway(resourceManagerGateway)
                        .build()) {

            final CompletableFuture<SlotRequestId> slotRequestTimeoutFuture =
                    new CompletableFuture<>();

            final CompletableFuture<PhysicalSlot> future =
                    testMainThreadExecutor.execute(
                            () ->
                                    pool.requestNewAllocatedSlot(
                                            new SlotRequestId(),
                                            ResourceProfile.UNKNOWN,
                                            fastTimeout));

            try {
                future.get();
                fail("We expected a TimeoutException.");
            } catch (ExecutionException e) {
                assertTrue(ExceptionUtils.stripExecutionException(e) instanceof TimeoutException);
            }

            // wait until we have timed out the slot request
            slotRequestTimeoutFuture.get();

            assertEquals(0L, pool.getAllocatedSlotsInformation().size());

            AllocationID allocationId = allocationIdFuture.get();
            final SlotOffer slotOffer = new SlotOffer(allocationId, 0, ResourceProfile.ANY);
            final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
            final TaskManagerGateway taskManagerGateway = new SimpleAckingTaskManagerGateway();

            testMainThreadExecutor.execute(
                    () -> pool.registerTaskManager(taskManagerLocation.getResourceID()));
                    ;
            assertTrue(
                    testMainThreadExecutor.execute(() ->
                            pool.offerSlots(taskManagerLocation, taskManagerGateway,
                                    Lists.newArrayList(slotOffer)) != null));

            assertTrue(pool.getAllocatedSlotsInformation().contains(slotOffer));
        }
    }

    private SlotPool createAndSetUpSlotPool() throws Exception {
        return new SlotPoolBuilder(testMainThreadExecutor.getMainThreadExecutor()).build();
    }

    private SlotPool createAndSetUpSlotPoolWithoutResourceManager() throws Exception {
        return new SlotPoolBuilder(testMainThreadExecutor.getMainThreadExecutor())
                .setResourceManagerGateway(null)
                .build();
    }
}
