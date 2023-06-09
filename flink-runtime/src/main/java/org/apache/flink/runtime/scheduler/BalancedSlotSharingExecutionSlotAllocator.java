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
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotRequestBulkChecker;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Allocates {@link LogicalSlot}s from physical shared slots.
 *
 * <p>The allocator are similar to {@link SlotSharingExecutionSlotAllocator}. Unlike {@link
 * SlotSharingExecutionSlotAllocator} allocating randomly for tasks in {@link
 * ExecutionSlotSharingGroup}, this allocator will allocate slots in the current specified order
 * defined in {@link #groupByExecutionSlotSharingGroup(List)}.
 */
public class BalancedSlotSharingExecutionSlotAllocator extends SlotSharingExecutionSlotAllocator
        implements ExecutionSlotAllocator {

    BalancedSlotSharingExecutionSlotAllocator(
            PhysicalSlotProvider slotProvider,
            boolean slotWillBeOccupiedIndefinitely,
            SlotSharingStrategy slotSharingStrategy,
            SharedSlotProfileRetriever.SharedSlotProfileRetrieverFactory
                    sharedSlotProfileRetrieverFactory,
            PhysicalSlotRequestBulkChecker bulkChecker,
            Time allocationTimeout,
            Function<ExecutionVertexID, ResourceProfile> resourceProfileRetriever) {
        super(
                slotProvider,
                slotWillBeOccupiedIndefinitely,
                slotSharingStrategy,
                sharedSlotProfileRetrieverFactory,
                bulkChecker,
                allocationTimeout,
                resourceProfileRetriever);
    }

    @Override
    public List<ExecutionSlotAssignment> allocateSlotsFor(
            List<ExecutionAttemptID> executionAttemptIds) {
        return super.allocateSlotsFor(executionAttemptIds);
    }

    @Override
    protected Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>>
            groupByExecutionSlotSharingGroup(List<ExecutionVertexID> executionVertexIds) {
        final Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> eSsgToExecutionIDs =
                super.groupByExecutionSlotSharingGroup(executionVertexIds);

        final Map<
                        SlotSharingGroup,
                        Deque<Map.Entry<ExecutionSlotSharingGroup, List<ExecutionVertexID>>>>
                groupBySsg = new HashMap<>();

        Map<SlotSharingGroup, List<Map.Entry<ExecutionSlotSharingGroup, List<ExecutionVertexID>>>>
                collect =
                        eSsgToExecutionIDs.entrySet().stream()
                                .collect(
                                        Collectors.groupingBy(
                                                entry ->
                                                        slotSharingStrategy.getSlotSharingGroup(
                                                                entry.getKey())));

        collect.forEach(
                (slotSharingGroup, entries) ->
                        groupBySsg.put(
                                slotSharingGroup,
                                entries.stream()
                                        .sorted(
                                                (left, right) ->
                                                        right.getValue().size()
                                                                - left.getValue().size())
                                        .collect(ArrayDeque::new, Deque::add, Collection::addAll)));

        final Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> result =
                new LinkedHashMap<>();

        groupBySsg.forEach((ssg, set) -> sortInSlotSharingGroup(result, set));

        return result;
    }

    private void sortInSlotSharingGroup(
            Map<ExecutionSlotSharingGroup, List<ExecutionVertexID>> eSsgToExecutionIDs,
            Deque<Map.Entry<ExecutionSlotSharingGroup, List<ExecutionVertexID>>> deque) {
        boolean nextAtHead = true;
        while (!deque.isEmpty()) {
            Map.Entry<ExecutionSlotSharingGroup, List<ExecutionVertexID>> next =
                    nextAtHead ? deque.pollFirst() : deque.pollLast();
            eSsgToExecutionIDs.put(next.getKey(), next.getValue());
            nextAtHead = !nextAtHead;
        }
    }
}
