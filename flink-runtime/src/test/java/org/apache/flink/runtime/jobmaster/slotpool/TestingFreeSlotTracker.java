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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobmaster.SlotInfo;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** Testing implements of {@link FreeSlotTracker}. */
public class TestingFreeSlotTracker implements FreeSlotTracker {
    private final Supplier<Set<AllocationID>> getAvailableSlotsSupplier;
    private final Function<AllocationID, SlotInfo> getSlotInfoFunction;
    private final Supplier<Collection<AllocatedSlotPool.FreeSlotInfo>>
            getFreeSlotsWithIdleSinceInformationSupplier;
    private final Supplier<Collection<PhysicalSlot>> getFreeSlotsInformationSupplier;
    private final Function<SlotInfo, Double> getTaskExecutorUtilizationFunction;
    private final Consumer<AllocationID> reserveSlotConsumer;
    private final Function<Set<AllocationID>, FreeSlotTracker>
            createNewFreeSlotTrackerWithoutBlockedSlotsFunction;

    public TestingFreeSlotTracker(
            Supplier<Set<AllocationID>> getAvailableSlotsSupplier,
            Function<AllocationID, SlotInfo> getSlotInfoFunction,
            Supplier<Collection<AllocatedSlotPool.FreeSlotInfo>>
                    getFreeSlotsWithIdleSinceInformationSupplier,
            Supplier<Collection<PhysicalSlot>> getFreeSlotsInformationSupplier,
            Function<SlotInfo, Double> getTaskExecutorUtilizationFunction,
            Consumer<AllocationID> reserveSlotConsumer,
            Function<Set<AllocationID>, FreeSlotTracker>
                    createNewFreeSlotTrackerWithoutBlockedSlotsFunction) {
        this.getAvailableSlotsSupplier = getAvailableSlotsSupplier;
        this.getSlotInfoFunction = getSlotInfoFunction;
        this.getFreeSlotsWithIdleSinceInformationSupplier =
                getFreeSlotsWithIdleSinceInformationSupplier;
        this.getFreeSlotsInformationSupplier = getFreeSlotsInformationSupplier;
        this.getTaskExecutorUtilizationFunction = getTaskExecutorUtilizationFunction;
        this.reserveSlotConsumer = reserveSlotConsumer;
        this.createNewFreeSlotTrackerWithoutBlockedSlotsFunction =
                createNewFreeSlotTrackerWithoutBlockedSlotsFunction;
    }

    @Override
    public Set<AllocationID> getAvailableSlots() {
        return getAvailableSlotsSupplier.get();
    }

    @Override
    public SlotInfo getSlotInfo(AllocationID allocationId) {
        return getSlotInfoFunction.apply(allocationId);
    }

    @Override
    public Collection<AllocatedSlotPool.FreeSlotInfo> getFreeSlotsWithIdleSinceInformation() {
        return getFreeSlotsWithIdleSinceInformationSupplier.get();
    }

    @Override
    public Collection<PhysicalSlot> getFreeSlotsInformation() {
        return getFreeSlotsInformationSupplier.get();
    }

    @Override
    public double getTaskExecutorUtilization(SlotInfo slotInfo) {
        return getTaskExecutorUtilizationFunction.apply(slotInfo);
    }

    @Override
    public void reserveSlot(AllocationID allocationId) {
        reserveSlotConsumer.accept(allocationId);
    }

    @Override
    public FreeSlotTracker createNewFreeSlotTrackerWithoutBlockedSlots(
            Set<AllocationID> blockedSlots) {
        return createNewFreeSlotTrackerWithoutBlockedSlotsFunction.apply(blockedSlots);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /** Builder of {@link TestingFreeSlotTracker}. * */
    public static class Builder {
        private Supplier<Set<AllocationID>> getAvailableSlotsSupplier = Collections::emptySet;
        private Function<AllocationID, SlotInfo> getSlotInfoFunction = ignored -> null;
        private Supplier<Collection<AllocatedSlotPool.FreeSlotInfo>>
                getFreeSlotsWithIdleSinceInformationSupplier = Collections::emptyList;
        private Supplier<Collection<PhysicalSlot>> getFreeSlotsInformationSupplier =
                Collections::emptyList;
        private Function<SlotInfo, Double> getTaskExecutorUtilizationFunction = ignored -> 0d;
        private Consumer<AllocationID> reserveSlotConsumer = ignore -> {};
        private Function<Set<AllocationID>, FreeSlotTracker>
                createNewFreeSlotTrackerWithoutBlockedSlotsFunction = ignored -> null;

        public Builder setGetAvailableSlotsSupplier(
                Supplier<Set<AllocationID>> getAvailableSlotsSupplier) {
            this.getAvailableSlotsSupplier = getAvailableSlotsSupplier;
            return this;
        }

        public Builder setGetSlotInfoFunction(
                Function<AllocationID, SlotInfo> getSlotInfoFunction) {
            this.getSlotInfoFunction = getSlotInfoFunction;
            return this;
        }

        public Builder setGetFreeSlotsWithIdleSinceInformationSupplier(
                Supplier<Collection<AllocatedSlotPool.FreeSlotInfo>>
                        getFreeSlotsWithIdleSinceInformationSupplier) {
            this.getFreeSlotsWithIdleSinceInformationSupplier =
                    getFreeSlotsWithIdleSinceInformationSupplier;
            return this;
        }

        public Builder setGetFreeSlotsInformationSupplier(
                Supplier<Collection<PhysicalSlot>> getFreeSlotsInformationSupplier) {
            this.getFreeSlotsInformationSupplier = getFreeSlotsInformationSupplier;
            return this;
        }

        public Builder setGetTaskExecutorUtilizationFunction(
                Function<SlotInfo, Double> getTaskExecutorUtilizationFunction) {
            this.getTaskExecutorUtilizationFunction = getTaskExecutorUtilizationFunction;
            return this;
        }

        public Builder setReserveSlotConsumer(Consumer<AllocationID> reserveSlotConsumer) {
            this.reserveSlotConsumer = reserveSlotConsumer;
            return this;
        }

        public TestingFreeSlotTracker build() {
            return new TestingFreeSlotTracker(
                    getAvailableSlotsSupplier,
                    getSlotInfoFunction,
                    getFreeSlotsWithIdleSinceInformationSupplier,
                    getFreeSlotsInformationSupplier,
                    getTaskExecutorUtilizationFunction,
                    reserveSlotConsumer,
                    createNewFreeSlotTrackerWithoutBlockedSlotsFunction);
        }
    }

    /** Testing {@link AllocatedSlotPool.FreeSlotInfo}. */
    public static class TestingFreeSlotInfo implements AllocatedSlotPool.FreeSlotInfo {
        private final SlotInfo slotInfo;

        public TestingFreeSlotInfo(SlotInfo slotInfo) {
            this.slotInfo = slotInfo;
        }

        @Override
        public SlotInfo asSlotInfo() {
            return slotInfo;
        }

        @Override
        public long getFreeSince() {
            return 0;
        }
    }
}
