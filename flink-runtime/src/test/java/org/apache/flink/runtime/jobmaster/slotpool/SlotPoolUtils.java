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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** Testing utility functions for the {@link SlotPool}. */
public class SlotPoolUtils {

    public static final Time TIMEOUT = Time.seconds(10L);

    private SlotPoolUtils() {
        throw new UnsupportedOperationException("Cannot instantiate this class.");
    }

    static TestingSlotPoolImpl createAndSetUpSlotPool(
            @Nullable final ResourceManagerGateway resourceManagerGateway) throws Exception {

        return new SlotPoolBuilder(ComponentMainThreadExecutorServiceAdapter.forMainThread())
                .setResourceManagerGateway(resourceManagerGateway)
                .build();
    }

    static CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
            final SlotPool slotPool, final SlotRequestId slotRequestId) {

        return requestNewAllocatedSlot(slotPool, slotRequestId, TIMEOUT);
    }

    static CompletableFuture<PhysicalSlot> requestNewAllocatedSlot(
            final SlotPool slotPool, final SlotRequestId slotRequestId, final Time timeout) {

        return slotPool.requestNewAllocatedSlot(slotRequestId, ResourceProfile.UNKNOWN, timeout);
    }

    static void requestNewAllocatedSlots(
            final SlotPool slotPool, final SlotRequestId... slotRequestIds) {
        for (SlotRequestId slotRequestId : slotRequestIds) {
            requestNewAllocatedSlot(slotPool, slotRequestId);
        }
    }

    public static CompletableFuture<PhysicalSlot> requestNewAllocatedBatchSlot(
            SlotPool slotPool,
            ComponentMainThreadExecutor mainThreadExecutor,
            ResourceProfile resourceProfile) {

        return CompletableFuture.supplyAsync(
                        () ->
                                slotPool.requestNewAllocatedBatchSlot(
                                        new SlotRequestId(), resourceProfile),
                        mainThreadExecutor)
                .thenCompose(Function.identity());
    }

    public static ResourceID offerSlots(
            SlotPoolImpl slotPool,
            ComponentMainThreadExecutor mainThreadExecutor,
            List<ResourceProfile> resourceProfiles) {
        return offerSlots(
                slotPool,
                mainThreadExecutor,
                resourceProfiles,
                new SimpleAckingTaskManagerGateway());
    }

    public static ResourceID offerSlots(
            SlotPoolImpl slotPool,
            ComponentMainThreadExecutor mainThreadExecutor,
            List<ResourceProfile> resourceProfiles,
            TaskManagerGateway taskManagerGateway) {
        final TaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
        CompletableFuture.runAsync(
                        () -> {
                            slotPool.registerTaskManager(taskManagerLocation.getResourceID());

                            final Collection<SlotOffer> slotOffers =
                                    IntStream.range(0, resourceProfiles.size())
                                            .mapToObj(
                                                    i ->
                                                            new SlotOffer(
                                                                    new AllocationID(),
                                                                    i,
                                                                    resourceProfiles.get(i)))
                                            .collect(Collectors.toList());

                            final Collection<SlotOffer> acceptedOffers =
                                    slotPool.offerSlots(
                                            taskManagerLocation, taskManagerGateway, slotOffers);

                            assertThat(acceptedOffers, is(slotOffers));
                        },
                        mainThreadExecutor)
                .join();

        return taskManagerLocation.getResourceID();
    }

    public static void failAllocation(
            SlotPoolImpl slotPool,
            ComponentMainThreadExecutor mainThreadExecutor,
            AllocationID allocationId,
            Exception exception) {
        CompletableFuture.runAsync(
                        () -> slotPool.failAllocation(allocationId, exception), mainThreadExecutor)
                .join();
    }

    public static void releaseTaskManager(
            SlotPoolImpl slotPool,
            ComponentMainThreadExecutor mainThreadExecutor,
            ResourceID taskManagerResourceId) {
        CompletableFuture.runAsync(
                        () ->
                                slotPool.releaseTaskManager(
                                        taskManagerResourceId,
                                        new FlinkException("Let's get rid of the offered slot.")),
                        mainThreadExecutor)
                .join();
    }
}
