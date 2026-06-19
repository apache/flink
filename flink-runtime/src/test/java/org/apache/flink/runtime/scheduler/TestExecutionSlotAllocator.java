/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkState;

/** Test {@link ExecutionSlotAllocator} implementation. */
public class TestExecutionSlotAllocator implements ExecutionSlotAllocator, SlotOwner {

    private final Map<ExecutionAttemptID, ExecutionSlotAssignment> pendingRequests =
            new HashMap<>();

    private final TestingLogicalSlotBuilder logicalSlotBuilder;

    private boolean autoCompletePendingRequests = true;

    private boolean completePendingRequestsWithReturnedSlots = false;

    private final List<LogicalSlot> returnedSlots = new ArrayList<>();

    public TestExecutionSlotAllocator() {
        this(new TestingLogicalSlotBuilder());
    }

    public TestExecutionSlotAllocator(TaskManagerGateway taskManagerGateway) {
        this(new TestingLogicalSlotBuilder().setTaskManagerGateway(taskManagerGateway));
    }

    public TestExecutionSlotAllocator(TestingLogicalSlotBuilder logicalSlotBuilder) {
        this.logicalSlotBuilder = logicalSlotBuilder;
    }

    @Override
    public Map<ExecutionAttemptID, ExecutionSlotAssignment> allocateSlotsFor(
            final List<ExecutionAttemptID> executionAttemptIds) {
        final Map<ExecutionAttemptID, ExecutionSlotAssignment> executionSlotAssignments =
                createExecutionSlotAssignments(executionAttemptIds);
        registerPendingRequests(executionSlotAssignments);
        maybeCompletePendingRequests();
        return executionSlotAssignments;
    }

    private Map<ExecutionAttemptID, ExecutionSlotAssignment> createExecutionSlotAssignments(
            final List<ExecutionAttemptID> executionAttemptIds) {

        final Map<ExecutionAttemptID, ExecutionSlotAssignment> result = new HashMap<>();
        for (ExecutionAttemptID executionAttemptId : executionAttemptIds) {
            final CompletableFuture<LogicalSlot> logicalSlotFuture = new CompletableFuture<>();
            result.put(
                    executionAttemptId,
                    new ExecutionSlotAssignment(executionAttemptId, logicalSlotFuture));
        }
        return result;
    }

    private void registerPendingRequests(
            final Map<ExecutionAttemptID, ExecutionSlotAssignment> executionSlotAssignments) {
        pendingRequests.putAll(executionSlotAssignments);
    }

    private void maybeCompletePendingRequests() {
        if (autoCompletePendingRequests) {
            completePendingRequests();
        }
    }

    public void completePendingRequests() {
        final Collection<ExecutionAttemptID> executionIds =
                new ArrayList<>(pendingRequests.keySet());
        executionIds.forEach(this::completePendingRequest);
    }

    public LogicalSlot completePendingRequest(final ExecutionAttemptID executionAttemptId) {
        final LogicalSlot slot = logicalSlotBuilder.setSlotOwner(this).createTestingLogicalSlot();
        final ExecutionSlotAssignment executionSlotAssignment =
                removePendingRequest(executionAttemptId);
        checkState(executionSlotAssignment != null);
        executionSlotAssignment.getLogicalSlotFuture().complete(slot);
        return slot;
    }

    public LogicalSlot completePendingRequest(final ExecutionVertexID executionVertexId) {
        return completePendingRequest(findExecutionIdByVertexId(executionVertexId));
    }

    private ExecutionSlotAssignment removePendingRequest(
            final ExecutionAttemptID executionAttemptId) {
        return pendingRequests.remove(executionAttemptId);
    }

    public void timeoutPendingRequests() {
        final Collection<ExecutionAttemptID> executionIds =
                new ArrayList<>(pendingRequests.keySet());
        executionIds.forEach(this::timeoutPendingRequest);
    }

    public void timeoutPendingRequest(final ExecutionAttemptID executionAttemptId) {
        final ExecutionSlotAssignment slotVertexAssignment =
                removePendingRequest(executionAttemptId);
        checkState(slotVertexAssignment != null);
        slotVertexAssignment.getLogicalSlotFuture().completeExceptionally(new TimeoutException());
    }

    public void timeoutPendingRequest(final ExecutionVertexID executionVertexId) {
        timeoutPendingRequest(findExecutionIdByVertexId(executionVertexId));
    }

    private ExecutionAttemptID findExecutionIdByVertexId(
            final ExecutionVertexID executionVertexId) {
        final List<ExecutionAttemptID> executionIds = new ArrayList<>();
        for (ExecutionAttemptID executionAttemptId : pendingRequests.keySet()) {
            if (executionAttemptId.getExecutionVertexId().equals(executionVertexId)) {
                executionIds.add(executionAttemptId);
            }
        }
        checkState(
                executionIds.size() == 1,
                "It is expected to find one and only one ExecutionAttemptID of the given ExecutionVertexID.");
        return executionIds.get(0);
    }

    public void enableAutoCompletePendingRequests() {
        autoCompletePendingRequests = true;
    }

    public void disableAutoCompletePendingRequests() {
        autoCompletePendingRequests = false;
    }

    public void enableCompletePendingRequestsWithReturnedSlots() {
        completePendingRequestsWithReturnedSlots = true;
    }

    @Override
    public void cancel(ExecutionAttemptID executionAttemptId) {
        final ExecutionSlotAssignment executionSlotAssignment =
                removePendingRequest(executionAttemptId);
        if (executionSlotAssignment != null) {
            executionSlotAssignment.getLogicalSlotFuture().cancel(false);
        }
    }

    @Override
    public void returnLogicalSlot(final LogicalSlot logicalSlot) {
        returnedSlots.add(logicalSlot);

        if (completePendingRequestsWithReturnedSlots) {
            if (pendingRequests.size() > 0) {
                // logical slots are not re-usable, creating a new one instead.
                final LogicalSlot slot =
                        logicalSlotBuilder.setSlotOwner(this).createTestingLogicalSlot();

                final ExecutionSlotAssignment executionSlotAssignment =
                        pendingRequests.remove(pendingRequests.keySet().stream().findAny().get());

                executionSlotAssignment.getLogicalSlotFuture().complete(slot);
            }
        }
    }

    public List<LogicalSlot> getReturnedSlots() {
        return new ArrayList<>(returnedSlots);
    }

    public TestingLogicalSlotBuilder getLogicalSlotBuilder() {
        return logicalSlotBuilder;
    }

    public Map<ExecutionAttemptID, ExecutionSlotAssignment> getPendingRequests() {
        return Collections.unmodifiableMap(pendingRequests);
    }
}
