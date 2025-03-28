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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.TaskExecutorsLoadInformation;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan.SlotAssignment;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Simple {@link SlotAssigner} that treats all slots and slot sharing groups equally. */
public class DefaultSlotAssigner implements SlotAssigner {

    private final RequestSlotMatchingStrategy requestSlotMatchingStrategy;

    public DefaultSlotAssigner(RequestSlotMatchingStrategy requestSlotMatchingStrategy) {
        this.requestSlotMatchingStrategy = Preconditions.checkNotNull(requestSlotMatchingStrategy);
    }

    @Override
    public Collection<SlotAssignment> assignSlots(
            JobInformation jobInformation,
            Collection<PhysicalSlot> freeSlots,
            VertexParallelism vertexParallelism,
            TaskExecutorsLoadInformation taskExecutorsLoadInformation,
            JobAllocationsInformation previousAllocations) {
        List<ExecutionSlotSharingGroup> allGroups = new ArrayList<>();
        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            allGroups.addAll(createExecutionSlotSharingGroups(vertexParallelism, slotSharingGroup));
        }

        return requestSlotMatchingStrategy.matchRequestsWithSlots(
                allGroups, freeSlots, taskExecutorsLoadInformation);
    }

    static List<ExecutionSlotSharingGroup> createExecutionSlotSharingGroups(
            VertexParallelism vertexParallelism, SlotSharingGroup slotSharingGroup) {
        final Map<Integer, Set<ExecutionVertexID>> sharedSlotToVertexAssignment = new HashMap<>();
        slotSharingGroup
                .getJobVertexIds()
                .forEach(
                        jobVertexId -> {
                            int parallelism = vertexParallelism.getParallelism(jobVertexId);
                            for (int subtaskIdx = 0; subtaskIdx < parallelism; subtaskIdx++) {
                                sharedSlotToVertexAssignment
                                        .computeIfAbsent(subtaskIdx, ignored -> new HashSet<>())
                                        .add(new ExecutionVertexID(jobVertexId, subtaskIdx));
                            }
                        });
        return sharedSlotToVertexAssignment.values().stream()
                .map(ExecutionSlotSharingGroup::new)
                .collect(Collectors.toList());
    }
}
