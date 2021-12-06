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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;
import org.apache.flink.runtime.scheduler.SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory;

/** Factory for {@link SlotSharingExecutionSlotAllocator}. */
class SlotSharingExecutionSlotAllocatorFactory implements ExecutionSlotAllocatorFactory {
    private final PhysicalSlotProvider slotProvider;

    private final boolean slotWillBeOccupiedIndefinitely;

    private final PhysicalSlotRequestBulkChecker bulkChecker;

    private final Time allocationTimeout;

    private final SlotSharingStrategy.Factory slotSharingStrategyFactory;

    SlotSharingExecutionSlotAllocatorFactory(
            PhysicalSlotProvider slotProvider,
            boolean slotWillBeOccupiedIndefinitely,
            PhysicalSlotRequestBulkChecker bulkChecker,
            Time allocationTimeout) {
        this(
                slotProvider,
                slotWillBeOccupiedIndefinitely,
                bulkChecker,
                allocationTimeout,
                new LocalInputPreferredSlotSharingStrategy.Factory());
    }

    SlotSharingExecutionSlotAllocatorFactory(
            PhysicalSlotProvider slotProvider,
            boolean slotWillBeOccupiedIndefinitely,
            PhysicalSlotRequestBulkChecker bulkChecker,
            Time allocationTimeout,
            SlotSharingStrategy.Factory slotSharingStrategyFactory) {
        this.slotProvider = slotProvider;
        this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
        this.bulkChecker = bulkChecker;
        this.slotSharingStrategyFactory = slotSharingStrategyFactory;
        this.allocationTimeout = allocationTimeout;
    }

    @Override
    public ExecutionSlotAllocator createInstance(final ExecutionSlotAllocationContext context) {
        SlotSharingStrategy slotSharingStrategy =
                slotSharingStrategyFactory.create(
                        context.getSchedulingTopology(),
                        context.getLogicalSlotSharingGroups(),
                        context.getCoLocationGroups());
        SyncPreferredLocationsRetriever preferredLocationsRetriever =
                new DefaultSyncPreferredLocationsRetriever(context, context);
        SharedSlotProfileRetrieverFactory sharedSlotProfileRetrieverFactory =
                new MergingSharedSlotProfileRetrieverFactory(
                        preferredLocationsRetriever,
                        context::getPriorAllocationId,
                        context::getReservedAllocations);
        return new SlotSharingExecutionSlotAllocator(
                slotProvider,
                slotWillBeOccupiedIndefinitely,
                slotSharingStrategy,
                sharedSlotProfileRetrieverFactory,
                bulkChecker,
                allocationTimeout,
                context::getResourceProfile);
    }
}
