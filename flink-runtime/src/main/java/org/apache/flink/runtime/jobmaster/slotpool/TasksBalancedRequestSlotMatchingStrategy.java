/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.scheduler.loading.DefaultLoadingWeight;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.runtime.state.heap.AbstractHeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueue;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.scheduler.loading.WeightLoadable.sortByLoadingDescend;

/**
 * The tasks balanced based implementation of {@link RequestSlotMatchingStrategy} that matches the
 * pending requests for tasks balance at task-manager level.
 */
public enum TasksBalancedRequestSlotMatchingStrategy implements RequestSlotMatchingStrategy {
    INSTANCE;

    public static final Logger LOG =
            LoggerFactory.getLogger(TasksBalancedRequestSlotMatchingStrategy.class);

    /** The {@link PhysicalSlotElement} comparator to compare loading. */
    static final class PhysicalSlotElementComparator implements Comparator<PhysicalSlotElement> {

        private final Map<ResourceID, LoadingWeight> taskExecutorsLoading;

        PhysicalSlotElementComparator(Map<ResourceID, LoadingWeight> taskExecutorsLoading) {
            this.taskExecutorsLoading = Preconditions.checkNotNull(taskExecutorsLoading);
        }

        @Override
        public int compare(PhysicalSlotElement left, PhysicalSlotElement right) {
            final LoadingWeight leftLoad =
                    taskExecutorsLoading.getOrDefault(
                            left.physicalSlot.getTaskManagerLocation().getResourceID(),
                            DefaultLoadingWeight.EMPTY);
            final LoadingWeight rightLoad =
                    taskExecutorsLoading.getOrDefault(
                            right.physicalSlot.getTaskManagerLocation().getResourceID(),
                            DefaultLoadingWeight.EMPTY);
            return leftLoad.compareTo(rightLoad);
        }
    }

    /** The {@link PhysicalSlot} element wrapper for {@link HeapPriorityQueue}. */
    static final class PhysicalSlotElement extends AbstractHeapPriorityQueueElement {

        private final PhysicalSlot physicalSlot;

        public PhysicalSlotElement(PhysicalSlot physicalSlot) {
            this.physicalSlot = physicalSlot;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof PhysicalSlotElement) {
                return physicalSlot.equals(((PhysicalSlotElement) o).physicalSlot);
            }
            return false;
        }

        @Override
        public int hashCode() {
            return physicalSlot.hashCode();
        }
    }

    /** The {@link PhysicalSlotElement} comparator. */
    static final class PhysicalSlotElementPriorityComparator
            implements PriorityComparator<PhysicalSlotElement> {

        private final PhysicalSlotElementComparator physicalSlotElementComparator;

        PhysicalSlotElementPriorityComparator(Map<ResourceID, LoadingWeight> taskExecutorsLoading) {
            this.physicalSlotElementComparator =
                    new PhysicalSlotElementComparator(taskExecutorsLoading);
        }

        @Override
        public int comparePriority(PhysicalSlotElement left, PhysicalSlotElement right) {
            return physicalSlotElementComparator.compare(left, right);
        }
    }

    @Override
    public Collection<RequestSlotMatch> matchRequestsAndSlots(
            Collection<? extends PhysicalSlot> slots,
            Collection<PendingRequest> pendingRequests,
            Map<ResourceID, LoadingWeight> taskExecutorsLoad) {
        if (pendingRequests.isEmpty()) {
            return Collections.emptyList();
        }

        final Collection<RequestSlotMatch> resultingMatches = new ArrayList<>();
        final List<PendingRequest> sortedRequests = sortByLoadingDescend(pendingRequests);
        LOG.debug(
                "Available slots: {}, sortedRequests: {}, taskExecutorsLoad: {}",
                slots,
                sortedRequests,
                taskExecutorsLoad);
        Collection<PhysicalSlotElement> slotElements =
                slots.stream().map(PhysicalSlotElement::new).collect(Collectors.toList());
        final Map<ResourceProfile, HeapPriorityQueue<PhysicalSlotElement>> profileSlots =
                getSlotCandidatesByProfile(slotElements, taskExecutorsLoad);
        final Map<ResourceID, Set<PhysicalSlotElement>> taskExecutorSlots =
                groupSlotsByTaskExecutor(slotElements);
        for (PendingRequest request : sortedRequests) {
            Optional<PhysicalSlotElement> bestSlotEle =
                    tryMatchPhysicalSlot(request, profileSlots, taskExecutorsLoad);
            if (bestSlotEle.isPresent()) {
                PhysicalSlotElement slotElement = bestSlotEle.get();
                updateReferenceAfterMatching(
                        profileSlots,
                        taskExecutorsLoad,
                        taskExecutorSlots,
                        slotElement,
                        request.getLoading());
                resultingMatches.add(RequestSlotMatch.createFor(request, slotElement.physicalSlot));
            }
        }
        return resultingMatches;
    }

    private Map<ResourceID, Set<PhysicalSlotElement>> groupSlotsByTaskExecutor(
            Collection<PhysicalSlotElement> slotElements) {
        return slotElements.stream()
                .collect(
                        Collectors.groupingBy(
                                physicalSlot ->
                                        physicalSlot
                                                .physicalSlot
                                                .getTaskManagerLocation()
                                                .getResourceID(),
                                Collectors.toSet()));
    }

    private Map<ResourceProfile, HeapPriorityQueue<PhysicalSlotElement>> getSlotCandidatesByProfile(
            Collection<PhysicalSlotElement> slotElements,
            Map<ResourceID, LoadingWeight> taskExecutorsLoad) {
        final Map<ResourceProfile, HeapPriorityQueue<PhysicalSlotElement>> result = new HashMap<>();
        final PhysicalSlotElementPriorityComparator physicalSlotElementPriorityComparator =
                new PhysicalSlotElementPriorityComparator(taskExecutorsLoad);
        for (PhysicalSlotElement slotEle : slotElements) {
            result.compute(
                    slotEle.physicalSlot.getResourceProfile(),
                    (resourceProfile, oldSlots) -> {
                        HeapPriorityQueue<PhysicalSlotElement> values =
                                Objects.isNull(oldSlots)
                                        ? new HeapPriorityQueue<>(
                                                physicalSlotElementPriorityComparator, 8)
                                        : oldSlots;
                        values.add(slotEle);
                        return values;
                    });
        }
        return result;
    }

    private Optional<PhysicalSlotElement> tryMatchPhysicalSlot(
            PendingRequest request,
            Map<ResourceProfile, HeapPriorityQueue<PhysicalSlotElement>> profileToSlotMap,
            Map<ResourceID, LoadingWeight> taskExecutorsLoad) {
        final ResourceProfile requestProfile = request.getResourceProfile();

        final Set<ResourceProfile> candidateProfiles =
                profileToSlotMap.keySet().stream()
                        .filter(slotProfile -> slotProfile.isMatching(requestProfile))
                        .collect(Collectors.toSet());

        return candidateProfiles.stream()
                .map(
                        candidateProfile -> {
                            HeapPriorityQueue<PhysicalSlotElement> slots =
                                    profileToSlotMap.get(candidateProfile);
                            return Objects.isNull(slots) ? null : slots.peek();
                        })
                .filter(Objects::nonNull)
                .min(new PhysicalSlotElementComparator(taskExecutorsLoad));
    }

    private void updateReferenceAfterMatching(
            Map<ResourceProfile, HeapPriorityQueue<PhysicalSlotElement>> profileSlots,
            Map<ResourceID, LoadingWeight> taskExecutorsLoad,
            Map<ResourceID, Set<PhysicalSlotElement>> taskExecutorSlots,
            PhysicalSlotElement targetSlotElement,
            LoadingWeight loading) {
        final ResourceID tmID =
                targetSlotElement.physicalSlot.getTaskManagerLocation().getResourceID();
        // Update the loading for the target task executor.
        taskExecutorsLoad.compute(
                tmID,
                (ignoredId, oldLoading) ->
                        Objects.isNull(oldLoading) ? loading : oldLoading.merge(loading));
        // Update the sorted set for slots that is located on the same task executor as targetSlot.
        // Use Map#remove to avoid the ConcurrentModifyException.
        final Set<PhysicalSlotElement> slotToReSort = taskExecutorSlots.remove(tmID);
        for (PhysicalSlotElement slotEle : slotToReSort) {
            HeapPriorityQueue<PhysicalSlotElement> slotsOfProfile =
                    profileSlots.get(slotEle.physicalSlot.getResourceProfile());
            // Re-add for the latest order.
            slotsOfProfile.remove(slotEle);
            if (!slotEle.equals(targetSlotElement)) {
                slotsOfProfile.add(slotEle);
            }
        }
        slotToReSort.remove(targetSlotElement);
        taskExecutorSlots.put(tmID, slotToReSort);
    }

    @Override
    public String toString() {
        return TasksBalancedRequestSlotMatchingStrategy.class.getSimpleName();
    }
}
