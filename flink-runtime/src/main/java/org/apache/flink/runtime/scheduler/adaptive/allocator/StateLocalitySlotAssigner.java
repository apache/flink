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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.base.MoreObjects;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/** A {@link SlotAssigner} that assigns slots based on the number of local key groups. */
public class StateLocalitySlotAssigner implements SlotAssigner {

    private static class AllocationScore<T extends Comparable<? super T>>
            implements Comparable<AllocationScore<T>> {

        private final T id;
        private final AllocationID allocationId;

        public AllocationScore(T id, AllocationID allocationId, int score) {
            this.id = id;
            this.allocationId = allocationId;
            this.score = score;
        }

        private final int score;

        public T getId() {
            return id;
        }

        public AllocationID getAllocationId() {
            return allocationId;
        }

        public int getScore() {
            return score;
        }

        @Override
        public int compareTo(StateLocalitySlotAssigner.AllocationScore<T> other) {
            int result = Integer.compare(score, other.score);
            if (result != 0) {
                return result;
            }
            result = other.allocationId.compareTo(allocationId);
            if (result != 0) {
                return result;
            }
            return other.id.compareTo(id);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("id", id)
                    .add("allocationId", allocationId)
                    .add("score", score)
                    .toString();
        }
    }

    private final Map<AllocationID, Map<JobVertexID, KeyGroupRange>> locality;
    private final Map<JobVertexID, Integer> maxParallelism;

    public StateLocalitySlotAssigner(ArchivedExecutionGraph archivedExecutionGraph) {
        this.locality = calculateLocalKeyGroups(archivedExecutionGraph);
        this.maxParallelism =
                StreamSupport.stream(
                                archivedExecutionGraph.getVerticesTopologically().spliterator(),
                                false)
                        .collect(
                                Collectors.toMap(
                                        ArchivedExecutionJobVertex::getJobVertexId,
                                        ArchivedExecutionJobVertex::getMaxParallelism));
    }

    @Override
    public List<SlotSharingSlotAllocator.ExecutionSlotSharingGroupAndSlot> assignSlots(
            Collection<? extends SlotInfo> slots,
            Collection<SlotSharingSlotAllocator.ExecutionSlotSharingGroup> groups) {
        final PriorityQueue<AllocationScore<String>> groupScore =
                new PriorityQueue<>(Comparator.reverseOrder());

        final Map<JobVertexID, Integer> parallelism = new HashMap<>();
        groups.forEach(
                group ->
                        group.getContainedExecutionVertices()
                                .forEach(
                                        evi ->
                                                parallelism.merge(
                                                        evi.getJobVertexId(), 1, Integer::sum)));

        for (SlotSharingSlotAllocator.ExecutionSlotSharingGroup group : groups) {
            final Map<AllocationID, Integer> allocationScore = calculateScore(group, parallelism);
            allocationScore.forEach(
                    (allocationId, score) ->
                            groupScore.add(
                                    new AllocationScore<>(group.getId(), allocationId, score)));
        }
        final List<SlotSharingSlotAllocator.ExecutionSlotSharingGroupAndSlot> result =
                new ArrayList<>();
        final Map<String, SlotSharingSlotAllocator.ExecutionSlotSharingGroup> groupsById =
                groups.stream()
                        .collect(
                                Collectors.toMap(
                                        SlotSharingSlotAllocator.ExecutionSlotSharingGroup::getId,
                                        Function.identity()));
        final Map<AllocationID, ? extends SlotInfo> slotsById =
                slots.stream()
                        .collect(Collectors.toMap(SlotInfo::getAllocationId, Function.identity()));
        AllocationScore<String> item;
        while ((item = groupScore.poll()) != null) {
            System.out.println("======= " + item);
            @Nullable
            final SlotSharingSlotAllocator.ExecutionSlotSharingGroup group =
                    groupsById.get(item.getId());
            if (group != null) {
                @Nullable final SlotInfo slot = slotsById.remove(item.getAllocationId());
                if (slot != null) {
                    result.add(
                            new SlotSharingSlotAllocator.ExecutionSlotSharingGroupAndSlot(
                                    group, slot));
                    Objects.requireNonNull(groupsById.remove(item.getId()));
                }
            }
        }

        // Let's distribute remaining slots with no score...
        final Iterator<? extends SlotInfo> remainingSlots = slotsById.values().iterator();
        for (SlotSharingSlotAllocator.ExecutionSlotSharingGroup group : groupsById.values()) {
            result.add(
                    new SlotSharingSlotAllocator.ExecutionSlotSharingGroupAndSlot(
                            group, remainingSlots.next()));
            remainingSlots.remove();
        }

        return result;
    }

    public Map<AllocationID, Integer> calculateScore(
            SlotSharingSlotAllocator.ExecutionSlotSharingGroup group,
            Map<JobVertexID, Integer> parallelism) {
        final Map<AllocationID, Integer> score = new HashMap<>();
        for (ExecutionVertexID evi : group.getContainedExecutionVertices()) {
            if (maxParallelism.containsKey(evi.getJobVertexId())) {
                final KeyGroupRange kgr =
                        KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                                maxParallelism.get(evi.getJobVertexId()),
                                parallelism.get(evi.getJobVertexId()),
                                evi.getSubtaskIndex());
                locality.forEach(
                        (allocationId, potentials) -> {
                            @Nullable
                            final KeyGroupRange prev = potentials.get(evi.getJobVertexId());
                            if (prev != null) {
                                final int intersection =
                                        prev.getIntersection(kgr).getNumberOfKeyGroups();
                                if (intersection > 0) {
                                    score.merge(allocationId, intersection, Integer::sum);
                                }
                            }
                        });
            }
        }
        return score;
    }

    @Override
    public Map<SlotSharingGroupId, Set<? extends SlotInfo>> splitSlotsBetweenSlotSharingGroups(
            Collection<? extends SlotInfo> freeSlots, Collection<SlotSharingGroup> groups) {
        final int slotsPerSlotSharingGroup = freeSlots.size() / groups.size();
        if (slotsPerSlotSharingGroup <= 0) {
            return Collections.emptyMap();
        }

        final Map<SlotSharingGroupId, Set<SlotInfo>> groupSlots = new HashMap<>();
        final Map<AllocationID, ? extends SlotInfo> slotsByAllocationId =
                freeSlots.stream()
                        .collect(Collectors.toMap(SlotInfo::getAllocationId, Function.identity()));
        final PriorityQueue<AllocationScore<SlotSharingGroupId>> groupScore =
                new PriorityQueue<>(Comparator.reverseOrder());
        // Order slots by score & allocationId & groupId...
        for (SlotSharingGroup group : groups) {
            for (SlotInfo slot : freeSlots) {
                final Map<JobVertexID, KeyGroupRange> slotLocality =
                        locality.getOrDefault(slot.getAllocationId(), Collections.emptyMap());
                int numLocalKeyGroups = 0;
                for (JobVertexID groupVertex : group.getJobVertexIds()) {
                    @Nullable final KeyGroupRange range = slotLocality.get(groupVertex);
                    if (range != null) {
                        numLocalKeyGroups += range.getNumberOfKeyGroups();
                    }
                }
                groupScore.add(
                        new StateLocalitySlotAssigner.AllocationScore<>(
                                group.getSlotSharingGroupId(),
                                slot.getAllocationId(),
                                numLocalKeyGroups));
            }
        }

        StateLocalitySlotAssigner.AllocationScore<SlotSharingGroupId> item;
        final Set<AllocationID> alreadyAllocated = new HashSet<>();
        while ((item = groupScore.poll()) != null) {
            System.out.println("=== " + item);
            if (!alreadyAllocated.contains(item.getAllocationId())) {
                final Set<SlotInfo> s =
                        groupSlots.computeIfAbsent(item.getId(), ignored -> new HashSet<>());
                if (s.size() < slotsPerSlotSharingGroup) {
                    s.add(slotsByAllocationId.get(item.getAllocationId()));
                    alreadyAllocated.add(item.getAllocationId());
                }
            }
        }
        @SuppressWarnings({"unchecked", "rawtypes"})
        final Map<SlotSharingGroupId, Set<? extends SlotInfo>> cast = (Map) groupSlots;
        return cast;
    }

    private Map<AllocationID, Map<JobVertexID, KeyGroupRange>> calculateLocalKeyGroups(
            ArchivedExecutionGraph archivedExecutionGraph) {
        final Map<AllocationID, Map<JobVertexID, KeyGroupRange>> localKeyGroups = new HashMap<>();
        if (archivedExecutionGraph != null) {
            for (ArchivedExecutionJobVertex executionJobVertex :
                    archivedExecutionGraph.getVerticesTopologically()) {
                for (ArchivedExecutionVertex executionVertex :
                        executionJobVertex.getTaskVertices()) {
                    final AllocationID allocationId =
                            executionVertex.getCurrentExecutionAttempt().getAssignedAllocationID();
                    final KeyGroupRange kgr =
                            KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                                    executionJobVertex.getMaxParallelism(),
                                    executionJobVertex.getParallelism(),
                                    executionVertex.getParallelSubtaskIndex());
                    @Nullable
                    final KeyGroupRange previous =
                            localKeyGroups
                                    .computeIfAbsent(allocationId, ignored -> new HashMap<>())
                                    .put(executionJobVertex.getJobVertexId(), kgr);
                    Preconditions.checkState(
                            previous == null,
                            "Can only have a single key group range of a vertex per slot");
                    System.out.printf(
                            "Previous Slot: %s, ExecutionVertex: %s, KGR: %s%n",
                            allocationId,
                            new ExecutionVertexID(
                                    executionJobVertex.getJobVertexId(),
                                    executionVertex.getParallelSubtaskIndex()),
                            kgr);
                }
            }
        }
        return localKeyGroups;
    }
}
