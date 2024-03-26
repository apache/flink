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
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.util.CollectionUtil.isNullOrEmpty;

/**
 * The tasks balanced based implementation of {@link RequestSlotMatchingStrategy} that matches the
 * pending requests for tasks balance at task-manager level.
 */
public enum TasksBalancedRequestSlotMatchingStrategy implements RequestSlotMatchingStrategy {
    INSTANCE;

    @Override
    public Collection<RequestSlotMatch> matchRequestsAndSlots(
            Collection<? extends PhysicalSlot> slots,
            Collection<PendingRequest> pendingRequests,
            Map<ResourceID, LoadingWeight> taskExecutorsLoadingWeight) {
        if (pendingRequests.isEmpty()) {
            return Collections.emptyList();
        }

        final Collection<RequestSlotMatch> resultingMatches = new ArrayList<>();
        List<PendingRequest> sortedRequests = sortByLoadingDescend(pendingRequests);
        Map<ResourceID, ? extends List<? extends PhysicalSlot>> availableSlots =
                getSlotsPerTaskExecutor(slots);

        for (PendingRequest request : sortedRequests) {
            ResourceID candidateTaskExecutor = getCandidateTaskExecutor(taskExecutorsLoadingWeight);

            List<? extends PhysicalSlot> slotCandidates = availableSlots.get(candidateTaskExecutor);
            Preconditions.checkState(!isNullOrEmpty(slotCandidates));
            for (PhysicalSlot slot : slotCandidates) {
                if (slot.getResourceProfile().isMatching(request.getResourceProfile())) {
                    resultingMatches.add(RequestSlotMatch.createFor(request, slot));
                    slotCandidates.remove(slot);
                    taskExecutorsLoadingWeight.compute(
                            candidateTaskExecutor,
                            (ignored, loadingWeight) -> request.getLoading().merge(loadingWeight));
                    if (slotCandidates.isEmpty()) {
                        taskExecutorsLoadingWeight.remove(
                                slot.getTaskManagerLocation().getResourceID());
                    }
                    break;
                }
            }
        }

        return resultingMatches;
    }

    private Map<ResourceID, ? extends List<? extends PhysicalSlot>> getSlotsPerTaskExecutor(
            Collection<? extends PhysicalSlot> slots) {
        return slots.stream()
                .collect(
                        Collectors.groupingBy(
                                physicalSlot ->
                                        physicalSlot.getTaskManagerLocation().getResourceID(),
                                Collectors.toList()));
    }

    private List<PendingRequest> sortByLoadingDescend(Collection<PendingRequest> pendingRequests) {
        return pendingRequests.stream()
                .sorted((o1, o2) -> o2.getLoading().compareTo(o1.getLoading()))
                .collect(Collectors.toList());
    }

    private ResourceID getCandidateTaskExecutor(
            Map<ResourceID, LoadingWeight> taskExecutorsLoadingWeight) {
        Optional<Map.Entry<ResourceID, LoadingWeight>> minOpt =
                taskExecutorsLoadingWeight.entrySet().stream().min(Map.Entry.comparingByValue());
        Preconditions.checkState(minOpt.isPresent());
        return minOpt.get().getKey();
    }

    @Override
    public String toString() {
        return TasksBalancedRequestSlotMatchingStrategy.class.getSimpleName();
    }
}
