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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/** Builder for {@link TestingDeclarativeSlotPool}. */
public class TestingDeclarativeSlotPoolBuilder {

    private Consumer<ResourceCounter> increaseResourceRequirementsByConsumer = ignored -> {};
    private Consumer<ResourceCounter> decreaseResourceRequirementsByConsumer = ignored -> {};
    private Supplier<Collection<ResourceRequirement>> getResourceRequirementsSupplier =
            Collections::emptyList;
    private QuadFunction<
                    Collection<? extends SlotOffer>,
                    TaskManagerLocation,
                    TaskManagerGateway,
                    Long,
                    Collection<SlotOffer>>
            offerSlotsFunction =
                    (ignoredA, ignoredB, ignoredC, ignoredD) -> Collections.emptyList();
    private Supplier<Collection<SlotInfo>> getFreeSlotsInformationSupplier = Collections::emptyList;
    private Supplier<Collection<? extends SlotInfo>> getAllSlotsInformationSupplier =
            Collections::emptyList;
    private Supplier<FreeSlotInfoTracker> getFreeSlotInfoTrackerSupplier =
            () -> TestingFreeSlotInfoTracker.newBuilder().build();
    private BiFunction<ResourceID, Exception, ResourceCounter> releaseSlotsFunction =
            (ignoredA, ignoredB) -> ResourceCounter.empty();
    private BiFunction<AllocationID, Exception, ResourceCounter> releaseSlotFunction =
            (ignoredA, ignoredB) -> ResourceCounter.empty();
    private BiFunction<AllocationID, ResourceProfile, PhysicalSlot> reserveFreeSlotFunction =
            (ignoredA, ignoredB) -> null;
    private TriFunction<AllocationID, Throwable, Long, ResourceCounter> freeReservedSlotFunction =
            (ignoredA, ignoredB, ignoredC) -> ResourceCounter.empty();
    private Function<ResourceID, Boolean> containsSlotsFunction = ignored -> false;
    private LongConsumer returnIdleSlotsConsumer = ignored -> {};
    private Consumer<ResourceCounter> setResourceRequirementsConsumer = ignored -> {};
    private Function<AllocationID, Boolean> containsFreeSlotFunction = ignored -> false;
    private QuadFunction<
                    Collection<? extends SlotOffer>,
                    TaskManagerLocation,
                    TaskManagerGateway,
                    Long,
                    Collection<SlotOffer>>
            registerSlotsFunction =
                    (slotOffers, ignoredB, ignoredC, ignoredD) -> new ArrayList<>(slotOffers);

    public TestingDeclarativeSlotPoolBuilder setIncreaseResourceRequirementsByConsumer(
            Consumer<ResourceCounter> increaseResourceRequirementsByConsumer) {
        this.increaseResourceRequirementsByConsumer = increaseResourceRequirementsByConsumer;
        return this;
    }

    public TestingDeclarativeSlotPoolBuilder setDecreaseResourceRequirementsByConsumer(
            Consumer<ResourceCounter> decreaseResourceRequirementsByConsumer) {
        this.decreaseResourceRequirementsByConsumer = decreaseResourceRequirementsByConsumer;
        return this;
    }

    public TestingDeclarativeSlotPoolBuilder setSetResourceRequirementsConsumer(
            Consumer<ResourceCounter> setResourceRequirementsConsumer) {
        this.setResourceRequirementsConsumer = setResourceRequirementsConsumer;
        return this;
    }

    public TestingDeclarativeSlotPoolBuilder setGetResourceRequirementsSupplier(
            Supplier<Collection<ResourceRequirement>> getResourceRequirementsSupplier) {
        this.getResourceRequirementsSupplier = getResourceRequirementsSupplier;
        return this;
    }

    public TestingDeclarativeSlotPoolBuilder setOfferSlotsFunction(
            QuadFunction<
                            Collection<? extends SlotOffer>,
                            TaskManagerLocation,
                            TaskManagerGateway,
                            Long,
                            Collection<SlotOffer>>
                    offerSlotsFunction) {
        this.offerSlotsFunction = offerSlotsFunction;
        return this;
    }

    public TestingDeclarativeSlotPoolBuilder setRegisterSlotsFunction(
            QuadFunction<
                            Collection<? extends SlotOffer>,
                            TaskManagerLocation,
                            TaskManagerGateway,
                            Long,
                            Collection<SlotOffer>>
                    registerSlotsFunction) {
        this.registerSlotsFunction = registerSlotsFunction;
        return this;
    }

    public TestingDeclarativeSlotPoolBuilder setGetFreeSlotsInformationSupplier(
            Supplier<Collection<SlotInfo>> getFreeSlotsInformationSupplier) {
        this.getFreeSlotsInformationSupplier = getFreeSlotsInformationSupplier;
        return this;
    }

    public TestingDeclarativeSlotPoolBuilder setGetFreeSlotInfoTrackerSupplier(
            Supplier<FreeSlotInfoTracker> getFreeSlotInfoTrackerSupplier) {
        this.getFreeSlotInfoTrackerSupplier = getFreeSlotInfoTrackerSupplier;
        return this;
    }

    public TestingDeclarativeSlotPoolBuilder setGetAllSlotsInformationSupplier(
            Supplier<Collection<? extends SlotInfo>> getAllSlotsInformationSupplier) {
        this.getAllSlotsInformationSupplier = getAllSlotsInformationSupplier;
        return this;
    }

    public TestingDeclarativeSlotPoolBuilder setReleaseSlotsFunction(
            BiFunction<ResourceID, Exception, ResourceCounter> failSlotsConsumer) {
        this.releaseSlotsFunction = failSlotsConsumer;
        return this;
    }

    public TestingDeclarativeSlotPoolBuilder setReleaseSlotFunction(
            BiFunction<AllocationID, Exception, ResourceCounter> failSlotConsumer) {
        this.releaseSlotFunction = failSlotConsumer;
        return this;
    }

    public TestingDeclarativeSlotPoolBuilder setReserveFreeSlotFunction(
            BiFunction<AllocationID, ResourceProfile, PhysicalSlot>
                    allocateFreeSlotForResourceFunction) {
        this.reserveFreeSlotFunction = allocateFreeSlotForResourceFunction;
        return this;
    }

    public TestingDeclarativeSlotPoolBuilder setFreeReservedSlotFunction(
            TriFunction<AllocationID, Throwable, Long, ResourceCounter> freeReservedSlotFunction) {
        this.freeReservedSlotFunction = freeReservedSlotFunction;
        return this;
    }

    public TestingDeclarativeSlotPoolBuilder setContainsSlotsFunction(
            Function<ResourceID, Boolean> containsSlotsFunction) {
        this.containsSlotsFunction = containsSlotsFunction;
        return this;
    }

    public TestingDeclarativeSlotPoolBuilder setContainsFreeSlotFunction(
            Function<AllocationID, Boolean> containsFreeSlotFunction) {
        this.containsFreeSlotFunction = containsFreeSlotFunction;
        return this;
    }

    public TestingDeclarativeSlotPoolBuilder setReturnIdleSlotsConsumer(
            LongConsumer returnIdleSlotsConsumer) {
        this.returnIdleSlotsConsumer = returnIdleSlotsConsumer;
        return this;
    }

    public TestingDeclarativeSlotPool build() {
        return new TestingDeclarativeSlotPool(
                increaseResourceRequirementsByConsumer,
                decreaseResourceRequirementsByConsumer,
                getResourceRequirementsSupplier,
                offerSlotsFunction,
                registerSlotsFunction,
                getFreeSlotsInformationSupplier,
                getFreeSlotInfoTrackerSupplier,
                getAllSlotsInformationSupplier,
                releaseSlotsFunction,
                releaseSlotFunction,
                reserveFreeSlotFunction,
                freeReservedSlotFunction,
                containsSlotsFunction,
                containsFreeSlotFunction,
                returnIdleSlotsConsumer,
                setResourceRequirementsConsumer);
    }
}
