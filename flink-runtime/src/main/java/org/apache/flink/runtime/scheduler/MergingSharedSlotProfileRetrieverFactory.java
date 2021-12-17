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
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/** Factory for {@link MergingSharedSlotProfileRetriever}. */
class MergingSharedSlotProfileRetrieverFactory
        implements SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory {
    private final SyncPreferredLocationsRetriever preferredLocationsRetriever;

    private final Function<ExecutionVertexID, AllocationID> priorAllocationIdRetriever;

    private final Supplier<Set<AllocationID>> reservedAllocationIdsRetriever;

    MergingSharedSlotProfileRetrieverFactory(
            SyncPreferredLocationsRetriever preferredLocationsRetriever,
            Function<ExecutionVertexID, AllocationID> priorAllocationIdRetriever,
            Supplier<Set<AllocationID>> reservedAllocationIdsRetriever) {
        this.preferredLocationsRetriever = Preconditions.checkNotNull(preferredLocationsRetriever);
        this.priorAllocationIdRetriever = Preconditions.checkNotNull(priorAllocationIdRetriever);
        this.reservedAllocationIdsRetriever =
                Preconditions.checkNotNull(reservedAllocationIdsRetriever);
    }

    @Override
    public SharedSlotProfileRetriever createFromBulk(Set<ExecutionVertexID> bulk) {
        return new MergingSharedSlotProfileRetriever(reservedAllocationIdsRetriever.get(), bulk);
    }

    /**
     * Computes a merged {@link SlotProfile} of an execution slot sharing group within a bulk to
     * schedule.
     */
    private class MergingSharedSlotProfileRetriever implements SharedSlotProfileRetriever {
        /** All reserved {@link AllocationID}s of the job. */
        private final Set<AllocationID> reservedAllocationIds;

        /** All {@link ExecutionVertexID}s of the bulk. */
        private final Set<ExecutionVertexID> producersToIgnore;

        private MergingSharedSlotProfileRetriever(
                Set<AllocationID> reservedAllocationIds, Set<ExecutionVertexID> producersToIgnore) {
            this.reservedAllocationIds = Preconditions.checkNotNull(reservedAllocationIds);
            this.producersToIgnore = Preconditions.checkNotNull(producersToIgnore);
        }

        /**
         * Computes a {@link SlotProfile} of an execution slot sharing group.
         *
         * <p>The preferred locations of the {@link SlotProfile} is a union of the preferred
         * locations of all executions sharing the slot. The input locations within the bulk are
         * ignored to avoid cyclic dependencies within the region, e.g. in case of all-to-all
         * pipelined connections, so that the allocations do not block each other.
         *
         * <p>The preferred {@link AllocationID}s of the {@link SlotProfile} are all previous {@link
         * AllocationID}s of all executions sharing the slot.
         *
         * <p>The {@link SlotProfile} also refers to all reserved {@link AllocationID}s of the job.
         *
         * @param executionSlotSharingGroup executions sharing the slot.
         * @param physicalSlotResourceProfile {@link ResourceProfile} of the slot.
         * @return {@link SlotProfile} to allocate for the {@code executionSlotSharingGroup}.
         */
        @Override
        public SlotProfile getSlotProfile(
                ExecutionSlotSharingGroup executionSlotSharingGroup,
                ResourceProfile physicalSlotResourceProfile) {
            Collection<AllocationID> priorAllocations = new HashSet<>();
            Collection<TaskManagerLocation> preferredLocations = new ArrayList<>();
            for (ExecutionVertexID execution : executionSlotSharingGroup.getExecutionVertexIds()) {
                priorAllocations.add(priorAllocationIdRetriever.apply(execution));
                preferredLocations.addAll(
                        preferredLocationsRetriever.getPreferredLocations(
                                execution, producersToIgnore));
            }
            return SlotProfile.priorAllocation(
                    physicalSlotResourceProfile,
                    physicalSlotResourceProfile,
                    preferredLocations,
                    priorAllocations,
                    reservedAllocationIds);
        }
    }
}
