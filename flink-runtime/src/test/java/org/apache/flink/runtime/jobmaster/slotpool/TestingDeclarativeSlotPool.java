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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.slots.ResourceRequirement;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.function.QuadFunction;
import org.apache.flink.util.function.TriFunction;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/** Testing {@link DeclarativeSlotPool} implementation. */
final class TestingDeclarativeSlotPool implements DeclarativeSlotPool {

    private final Consumer<ResourceCounter> increaseResourceRequirementsByConsumer;

    private final Consumer<ResourceCounter> decreaseResourceRequirementsByConsumer;

    private final Supplier<Collection<ResourceRequirement>> getResourceRequirementsSupplier;

    private final QuadFunction<
                    Collection<? extends SlotOffer>,
                    TaskManagerLocation,
                    TaskManagerGateway,
                    Long,
                    Collection<SlotOffer>>
            offerSlotsFunction;

    private final Supplier<Collection<SlotInfoWithUtilization>> getFreeSlotsInformationSupplier;

    private final Supplier<Collection<? extends SlotInfo>> getAllSlotsInformationSupplier;

    private final BiFunction<ResourceID, Exception, ResourceCounter> releaseSlotsFunction;

    private final BiFunction<AllocationID, Exception, ResourceCounter> releaseSlotFunction;

    private final BiFunction<AllocationID, ResourceProfile, PhysicalSlot> reserveFreeSlotFunction;

    private final TriFunction<AllocationID, Throwable, Long, ResourceCounter>
            freeReservedSlotFunction;

    private final Function<ResourceID, Boolean> containsSlotsFunction;

    private final Function<AllocationID, Boolean> containsFreeSlotFunction;

    private final LongConsumer releaseIdleSlotsConsumer;

    private final Consumer<ResourceCounter> setResourceRequirementsConsumer;

    TestingDeclarativeSlotPool(
            Consumer<ResourceCounter> increaseResourceRequirementsByConsumer,
            Consumer<ResourceCounter> decreaseResourceRequirementsByConsumer,
            Supplier<Collection<ResourceRequirement>> getResourceRequirementsSupplier,
            QuadFunction<
                            Collection<? extends SlotOffer>,
                            TaskManagerLocation,
                            TaskManagerGateway,
                            Long,
                            Collection<SlotOffer>>
                    offerSlotsFunction,
            Supplier<Collection<SlotInfoWithUtilization>> getFreeSlotsInformationSupplier,
            Supplier<Collection<? extends SlotInfo>> getAllSlotsInformationSupplier,
            BiFunction<ResourceID, Exception, ResourceCounter> releaseSlotsFunction,
            BiFunction<AllocationID, Exception, ResourceCounter> releaseSlotFunction,
            BiFunction<AllocationID, ResourceProfile, PhysicalSlot> reserveFreeSlotFunction,
            TriFunction<AllocationID, Throwable, Long, ResourceCounter> freeReservedSlotFunction,
            Function<ResourceID, Boolean> containsSlotsFunction,
            Function<AllocationID, Boolean> containsFreeSlotFunction,
            LongConsumer releaseIdleSlotsConsumer,
            Consumer<ResourceCounter> setResourceRequirementsConsumer) {
        this.increaseResourceRequirementsByConsumer = increaseResourceRequirementsByConsumer;
        this.decreaseResourceRequirementsByConsumer = decreaseResourceRequirementsByConsumer;
        this.getResourceRequirementsSupplier = getResourceRequirementsSupplier;
        this.offerSlotsFunction = offerSlotsFunction;
        this.getFreeSlotsInformationSupplier = getFreeSlotsInformationSupplier;
        this.getAllSlotsInformationSupplier = getAllSlotsInformationSupplier;
        this.releaseSlotsFunction = releaseSlotsFunction;
        this.releaseSlotFunction = releaseSlotFunction;
        this.reserveFreeSlotFunction = reserveFreeSlotFunction;
        this.freeReservedSlotFunction = freeReservedSlotFunction;
        this.containsSlotsFunction = containsSlotsFunction;
        this.containsFreeSlotFunction = containsFreeSlotFunction;
        this.releaseIdleSlotsConsumer = releaseIdleSlotsConsumer;
        this.setResourceRequirementsConsumer = setResourceRequirementsConsumer;
    }

    @Override
    public void increaseResourceRequirementsBy(ResourceCounter increment) {
        increaseResourceRequirementsByConsumer.accept(increment);
    }

    @Override
    public void decreaseResourceRequirementsBy(ResourceCounter decrement) {
        decreaseResourceRequirementsByConsumer.accept(decrement);
    }

    @Override
    public void setResourceRequirements(ResourceCounter resourceRequirements) {
        setResourceRequirementsConsumer.accept(resourceRequirements);
    }

    @Override
    public Collection<ResourceRequirement> getResourceRequirements() {
        return getResourceRequirementsSupplier.get();
    }

    @Override
    public Collection<SlotOffer> offerSlots(
            Collection<? extends SlotOffer> offers,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            long currentTime) {
        return offerSlotsFunction.apply(
                offers, taskManagerLocation, taskManagerGateway, currentTime);
    }

    @Override
    public Collection<SlotInfoWithUtilization> getFreeSlotsInformation() {
        return getFreeSlotsInformationSupplier.get();
    }

    @Override
    public Collection<? extends SlotInfo> getAllSlotsInformation() {
        return getAllSlotsInformationSupplier.get();
    }

    @Override
    public boolean containsFreeSlot(AllocationID allocationId) {
        return containsFreeSlotFunction.apply(allocationId);
    }

    @Override
    public ResourceCounter releaseSlots(ResourceID owner, Exception cause) {
        return releaseSlotsFunction.apply(owner, cause);
    }

    @Override
    public ResourceCounter releaseSlot(AllocationID allocationId, Exception cause) {
        return releaseSlotFunction.apply(allocationId, cause);
    }

    @Override
    public PhysicalSlot reserveFreeSlot(
            AllocationID allocationId, ResourceProfile requiredSlotProfile) {
        return reserveFreeSlotFunction.apply(allocationId, requiredSlotProfile);
    }

    @Override
    public ResourceCounter freeReservedSlot(
            AllocationID allocationId, @Nullable Throwable cause, long currentTime) {
        return freeReservedSlotFunction.apply(allocationId, cause, currentTime);
    }

    @Override
    public boolean containsSlots(ResourceID owner) {
        return containsSlotsFunction.apply(owner);
    }

    @Override
    public void releaseIdleSlots(long currentTimeMillis) {
        releaseIdleSlotsConsumer.accept(currentTimeMillis);
    }

    @Override
    public void registerNewSlotsListener(NewSlotsListener listener) {
        // noop
    }

    public static TestingDeclarativeSlotPoolBuilder builder() {
        return new TestingDeclarativeSlotPoolBuilder();
    }
}
