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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequest;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/** A {@link PhysicalSlotProvider} implementation that can be used in tests. */
public class TestingPhysicalSlotProvider implements PhysicalSlotProvider {

    private final Map<SlotRequestId, PhysicalSlotRequest> requests;
    private final Map<SlotRequestId, CompletableFuture<TestingPhysicalSlot>> responses;
    private final Map<SlotRequestId, Throwable> cancellations;

    private final Function<ResourceProfile, CompletableFuture<TestingPhysicalSlot>>
            physicalSlotCreator;

    public static TestingPhysicalSlotProvider create(
            Function<ResourceProfile, CompletableFuture<TestingPhysicalSlot>> physicalSlotCreator) {
        return new TestingPhysicalSlotProvider(physicalSlotCreator);
    }

    public static TestingPhysicalSlotProvider createWithInfiniteSlotCreation() {
        return create(
                (resourceProfile) ->
                        CompletableFuture.completedFuture(
                                new TestingPhysicalSlot(resourceProfile, new AllocationID())));
    }

    public static TestingPhysicalSlotProvider createWithoutImmediatePhysicalSlotCreation() {
        return create((ignored) -> new CompletableFuture<>());
    }

    public static TestingPhysicalSlotProvider createWithFailingPhysicalSlotCreation(Throwable t) {
        return create((ignored) -> FutureUtils.completedExceptionally(t));
    }

    public static TestingPhysicalSlotProvider createWithLimitedAmountOfPhysicalSlots(
            int slotCount) {
        return createWithLimitedAmountOfPhysicalSlots(
                slotCount, new SimpleAckingTaskManagerGateway());
    }

    public static TestingPhysicalSlotProvider createWithLimitedAmountOfPhysicalSlots(
            int slotCount, TaskManagerGateway taskManagerGateway) {
        AtomicInteger availableSlotCount = new AtomicInteger(slotCount);
        return create(
                (resourceProfile) -> {
                    int count = availableSlotCount.getAndDecrement();
                    if (count > 0) {
                        return CompletableFuture.completedFuture(
                                TestingPhysicalSlot.builder()
                                        .withResourceProfile(resourceProfile)
                                        .withTaskManagerGateway(taskManagerGateway)
                                        .build());
                    } else {
                        return FutureUtils.completedExceptionally(
                                new NoResourceAvailableException(
                                        String.format(
                                                "The limit of %d provided slots was reached. No available slots can be provided.",
                                                slotCount)));
                    }
                });
    }

    private TestingPhysicalSlotProvider(
            Function<ResourceProfile, CompletableFuture<TestingPhysicalSlot>> physicalSlotCreator) {
        this.physicalSlotCreator = physicalSlotCreator;

        this.requests = new HashMap<>();
        this.responses = new HashMap<>();
        this.cancellations = new HashMap<>();
    }

    @Override
    public CompletableFuture<PhysicalSlotRequest.Result> allocatePhysicalSlot(
            PhysicalSlotRequest physicalSlotRequest) {
        SlotRequestId slotRequestId = physicalSlotRequest.getSlotRequestId();
        requests.put(slotRequestId, physicalSlotRequest);

        final CompletableFuture<TestingPhysicalSlot> resultFuture =
                physicalSlotCreator.apply(
                        physicalSlotRequest.getSlotProfile().getPhysicalSlotResourceProfile());

        responses.put(slotRequestId, resultFuture);

        return resultFuture.thenApply(
                physicalSlot -> new PhysicalSlotRequest.Result(slotRequestId, physicalSlot));
    }

    @Override
    public void cancelSlotRequest(SlotRequestId slotRequestId, Throwable cause) {
        cancellations.put(slotRequestId, cause);
    }

    public CompletableFuture<TestingPhysicalSlot> getResultForRequestId(
            SlotRequestId slotRequestId) {
        return getResponses().get(slotRequestId);
    }

    PhysicalSlotRequest getFirstRequestOrFail() {
        return getFirstElementOrFail(requests.values());
    }

    public void awaitAllSlotRequests() {
        getResponses().values().forEach(CompletableFuture::join);
    }

    public Map<SlotRequestId, PhysicalSlotRequest> getRequests() {
        return Collections.unmodifiableMap(requests);
    }

    public CompletableFuture<TestingPhysicalSlot> getFirstResponseOrFail() {
        return getFirstElementOrFail(responses.values());
    }

    public Map<SlotRequestId, CompletableFuture<TestingPhysicalSlot>> getResponses() {
        return Collections.unmodifiableMap(responses);
    }

    public Map<SlotRequestId, Throwable> getCancellations() {
        return Collections.unmodifiableMap(cancellations);
    }

    private static <T> T getFirstElementOrFail(Collection<T> collection) {
        Optional<T> element = collection.stream().findFirst();
        Preconditions.checkState(element.isPresent());
        return element.get();
    }
}
