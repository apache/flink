/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.TaskExecutorsLoadInformation;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** The interface to define the methods for request slot matching strategy. */
public interface RequestSlotMatchingStrategy {

    Supplier<FlinkRuntimeException> NO_SLOTS_EXCEPTION_GETTER =
            () -> new FlinkRuntimeException("No suitable slots enough.");

    /**
     * Match slots from the free slots with the given collection of requests execution groups.
     *
     * @param requestGroups the requested execution slot sharing groups.
     * @param freeSlots the free slots.
     * @param taskExecutorsLoadInformation the task executors loading information.
     * @return The assignment result.
     */
    Collection<SlotAssignment> matchRequestsWithSlots(
            Collection<ExecutionSlotSharingGroup> requestGroups,
            Collection<PhysicalSlot> freeSlots,
            TaskExecutorsLoadInformation taskExecutorsLoadInformation);

    /**
     * Helper class to represent the slot and the loading or slots utilization weight info of the
     * task executor where the slot is located at.
     */
    class SlotTaskExecutorWeight<T> {
        final @Nonnull T taskExecutorWeight;
        final @Nonnull PhysicalSlot physicalSlot;

        SlotTaskExecutorWeight(@Nonnull T taskExecutorWeight, @Nonnull PhysicalSlot physicalSlot) {
            this.taskExecutorWeight = taskExecutorWeight;
            this.physicalSlot = physicalSlot;
        }

        ResourceID getResourceID() {
            return physicalSlot.getTaskManagerLocation().getResourceID();
        }
    }

    static Map<ResourceID, Set<PhysicalSlot>> getSlotsPerTaskExecutor(
            Collection<PhysicalSlot> freeSlots) {
        return freeSlots.stream()
                .collect(
                        Collectors.groupingBy(
                                slot -> slot.getTaskManagerLocation().getResourceID(),
                                Collectors.toSet()));
    }
}
