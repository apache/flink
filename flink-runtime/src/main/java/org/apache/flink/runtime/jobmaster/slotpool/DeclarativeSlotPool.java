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

import javax.annotation.Nullable;

import java.util.Collection;

/**
 * Slot pool interface which uses Flink's declarative resource management protocol to acquire
 * resources.
 *
 * <p>In order to acquire new resources, users need to increase the required resources. Once they no
 * longer need the resources, users need to decrease the required resources so that superfluous
 * resources can be returned.
 */
public interface DeclarativeSlotPool {

    /**
     * Increases the resource requirements by increment.
     *
     * @param increment increment by which to increase the resource requirements
     */
    void increaseResourceRequirementsBy(ResourceCounter increment);

    /**
     * Decreases the resource requirements by decrement.
     *
     * @param decrement decrement by which to decrease the resource requirements
     */
    void decreaseResourceRequirementsBy(ResourceCounter decrement);

    /**
     * Sets the resource requirements to the given resourceRequirements.
     *
     * @param resourceRequirements new resource requirements
     */
    void setResourceRequirements(ResourceCounter resourceRequirements);

    /**
     * Returns the current resource requirements.
     *
     * @return current resource requirements
     */
    Collection<ResourceRequirement> getResourceRequirements();

    /**
     * Offers slots to this slot pool. The slot pool is free to accept as many slots as it needs.
     *
     * @param offers offers containing the list of slots offered to this slot pool
     * @param taskManagerLocation taskManagerLocation is the location of the offering TaskExecutor
     * @param taskManagerGateway taskManagerGateway is the gateway to talk to the offering
     *     TaskExecutor
     * @param currentTime currentTime is the time the slots are being offered
     * @return collection of accepted slots; the other slot offers are implicitly rejected
     */
    Collection<SlotOffer> offerSlots(
            Collection<? extends SlotOffer> offers,
            TaskManagerLocation taskManagerLocation,
            TaskManagerGateway taskManagerGateway,
            long currentTime);

    /**
     * Returns the slot information for all free slots (slots which can be allocated from the slot
     * pool).
     *
     * @return collection of free slot information
     */
    Collection<SlotInfoWithUtilization> getFreeSlotsInformation();

    /**
     * Returns the slot information for all slots (free and allocated slots).
     *
     * @return collection of slot information
     */
    Collection<? extends SlotInfo> getAllSlotsInformation();

    /**
     * Checks whether the slot pool contains a slot with the given {@link AllocationID} and if it is
     * free.
     *
     * @param allocationId allocationId specifies the slot to check for
     * @return {@code true} if the slot pool contains a free slot registered under the given
     *     allocation id; otherwise {@code false}
     */
    boolean containsFreeSlot(AllocationID allocationId);

    /**
     * Reserves the free slot identified by the given allocationId and maps it to the given
     * requiredSlotProfile.
     *
     * @param allocationId allocationId identifies the free slot to allocate
     * @param requiredSlotProfile requiredSlotProfile specifying the resource requirement
     * @return a PhysicalSlot representing the allocated slot
     * @throws IllegalStateException if no free slot with the given allocationId exists or if the
     *     specified slot cannot fulfill the requiredSlotProfile
     */
    PhysicalSlot reserveFreeSlot(AllocationID allocationId, ResourceProfile requiredSlotProfile);

    /**
     * Frees the reserved slot identified by the given allocationId. If no slot with allocationId
     * exists, then the call is ignored.
     *
     * <p>Whether the freed slot is returned to the owning TaskExecutor is implementation dependent.
     *
     * @param allocationId allocationId identifying the slot to release
     * @param cause cause for releasing the slot; can be {@code null}
     * @param currentTime currentTime when the slot was released
     * @return the resource requirements that the slot was fulfilling
     */
    ResourceCounter freeReservedSlot(
            AllocationID allocationId, @Nullable Throwable cause, long currentTime);

    /**
     * Releases all slots belonging to the owning TaskExecutor if it has been registered.
     *
     * @param owner owner identifying the owning TaskExecutor
     * @param cause cause for failing the slots
     * @return the resource requirements that all slots were fulfilling; empty if all slots were
     *     currently free
     */
    ResourceCounter releaseSlots(ResourceID owner, Exception cause);

    /**
     * Releases the slot specified by allocationId if one exists.
     *
     * @param allocationId allocationId identifying the slot to fail
     * @param cause cause for failing the slot
     * @return the resource requirements that the slot was fulfilling; empty if the slot was
     *     currently free
     */
    ResourceCounter releaseSlot(AllocationID allocationId, Exception cause);

    /**
     * Returns whether the slot pool has a slot registered which is owned by the given TaskExecutor.
     *
     * @param owner owner identifying the TaskExecutor for which to check whether the slot pool has
     *     some slots registered
     * @return true if the given TaskExecutor has a slot registered at the slot pool
     */
    boolean containsSlots(ResourceID owner);

    /**
     * Releases slots which have exceeded the idle slot timeout and are no longer needed to fulfill
     * the resource requirements.
     *
     * @param currentTimeMillis current time
     */
    void releaseIdleSlots(long currentTimeMillis);

    /**
     * Registers a listener which is called whenever new slots become available.
     *
     * @param listener which is called whenever new slots become available
     */
    void registerNewSlotsListener(NewSlotsListener listener);

    /**
     * Listener interface for newly available slots.
     *
     * <p>Implementations of the {@link DeclarativeSlotPool} will call {@link
     * #notifyNewSlotsAreAvailable} whenever newly offered slots are accepted or if an allocated
     * slot should become free after it is being {@link #freeReservedSlot freed}.
     */
    interface NewSlotsListener {

        /**
         * Notifies the listener about newly available slots.
         *
         * <p>This method will be called whenever newly offered slots are accepted or if an
         * allocated slot should become free after it is being {@link #freeReservedSlot freed}.
         *
         * @param newlyAvailableSlots are the newly available slots
         */
        void notifyNewSlotsAreAvailable(Collection<? extends PhysicalSlot> newlyAvailableSlots);
    }

    /** No-op {@link NewSlotsListener} implementation. */
    enum NoOpNewSlotsListener implements NewSlotsListener {
        INSTANCE;

        @Override
        public void notifyNewSlotsAreAvailable(
                Collection<? extends PhysicalSlot> newlyAvailableSlots) {}
    }
}
