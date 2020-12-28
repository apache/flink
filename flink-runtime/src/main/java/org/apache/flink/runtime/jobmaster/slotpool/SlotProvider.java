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
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/**
 * The slot provider is responsible for preparing slots for ready-to-run tasks.
 *
 * <p>It supports two allocating modes:
 *
 * <ul>
 *   <li>Immediate allocating: A request for a task slot immediately gets satisfied, we can call
 *       {@link CompletableFuture#getNow(Object)} to get the allocated slot.
 *   <li>Queued allocating: A request for a task slot is queued and returns a future that will be
 *       fulfilled as soon as a slot becomes available.
 * </ul>
 */
public interface SlotProvider {

    /**
     * Allocating slot with specific requirement.
     *
     * @param slotRequestId identifying the slot request
     * @param scheduledUnit The task to allocate the slot for
     * @param slotProfile profile of the requested slot
     * @param allocationTimeout after which the allocation fails with a timeout exception
     * @return The future of the allocation
     */
    CompletableFuture<LogicalSlot> allocateSlot(
            SlotRequestId slotRequestId,
            ScheduledUnit scheduledUnit,
            SlotProfile slotProfile,
            Time allocationTimeout);

    /**
     * Allocating batch slot with specific requirement.
     *
     * @param slotRequestId identifying the slot request
     * @param scheduledUnit The task to allocate the slot for
     * @param slotProfile profile of the requested slot
     * @return The future of the allocation
     */
    default CompletableFuture<LogicalSlot> allocateBatchSlot(
            SlotRequestId slotRequestId, ScheduledUnit scheduledUnit, SlotProfile slotProfile) {
        throw new UnsupportedOperationException("Not properly implemented.");
    }

    /**
     * Allocating slot with specific requirement.
     *
     * @param scheduledUnit The task to allocate the slot for
     * @param slotProfile profile of the requested slot
     * @param allocationTimeout after which the allocation fails with a timeout exception
     * @return The future of the allocation
     */
    default CompletableFuture<LogicalSlot> allocateSlot(
            ScheduledUnit scheduledUnit, SlotProfile slotProfile, Time allocationTimeout) {
        return allocateSlot(new SlotRequestId(), scheduledUnit, slotProfile, allocationTimeout);
    }

    /**
     * Cancels the slot request with the given {@link SlotRequestId} and {@link SlotSharingGroupId}.
     *
     * @param slotRequestId identifying the slot request to cancel
     * @param slotSharingGroupId identifying the slot request to cancel
     * @param cause of the cancellation
     */
    void cancelSlotRequest(
            SlotRequestId slotRequestId,
            @Nullable SlotSharingGroupId slotSharingGroupId,
            Throwable cause);
}
