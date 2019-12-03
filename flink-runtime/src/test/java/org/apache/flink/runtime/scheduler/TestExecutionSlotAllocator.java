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

import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotOwner;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Test {@link ExecutionSlotAllocator} implementation.
 */
public class TestExecutionSlotAllocator implements ExecutionSlotAllocator, SlotOwner {

	private final Map<ExecutionVertexID, SlotExecutionVertexAssignment> pendingRequests = new HashMap<>();

	private boolean autoCompletePendingRequests = true;

	private final List<LogicalSlot> returnedSlots = new ArrayList<>();

	@Override
	public List<SlotExecutionVertexAssignment> allocateSlotsFor(final List<ExecutionVertexSchedulingRequirements> schedulingRequirementsCollection) {
		final List<SlotExecutionVertexAssignment> slotVertexAssignments = createSlotVertexAssignments(schedulingRequirementsCollection);
		registerPendingRequests(slotVertexAssignments);
		maybeCompletePendingRequests();
		return slotVertexAssignments;
	}

	private void registerPendingRequests(final List<SlotExecutionVertexAssignment> slotVertexAssignments) {
		for (SlotExecutionVertexAssignment slotVertexAssignment : slotVertexAssignments) {
			pendingRequests.put(slotVertexAssignment.getExecutionVertexId(), slotVertexAssignment);
		}
	}

	private List<SlotExecutionVertexAssignment> createSlotVertexAssignments(
		final Collection<ExecutionVertexSchedulingRequirements> schedulingRequirementsCollection) {

		final List<SlotExecutionVertexAssignment> result = new ArrayList<>();
		for (ExecutionVertexSchedulingRequirements schedulingRequirements : schedulingRequirementsCollection) {
			final ExecutionVertexID executionVertexId = schedulingRequirements.getExecutionVertexId();
			final CompletableFuture<LogicalSlot> logicalSlotFuture = new CompletableFuture<>();
			result.add(new SlotExecutionVertexAssignment(executionVertexId, logicalSlotFuture));
		}
		return result;
	}

	private void maybeCompletePendingRequests() {
		if (autoCompletePendingRequests) {
			completePendingRequests();
		}
	}

	public void completePendingRequests() {
		final Collection<ExecutionVertexID> vertexIds = new ArrayList<>(pendingRequests.keySet());
		vertexIds.forEach(this::completePendingRequest);
	}

	public void completePendingRequest(final ExecutionVertexID executionVertexId) {
		final SlotExecutionVertexAssignment slotVertexAssignment = removePendingRequest(executionVertexId);
		checkState(slotVertexAssignment != null);
		slotVertexAssignment
			.getLogicalSlotFuture()
			.complete(new TestingLogicalSlotBuilder()
				.setSlotOwner(this)
				.createTestingLogicalSlot());
	}

	private SlotExecutionVertexAssignment removePendingRequest(final ExecutionVertexID executionVertexId) {
		return pendingRequests.remove(executionVertexId);
	}

	public void timeoutPendingRequests() {
		final Collection<ExecutionVertexID> vertexIds = new ArrayList<>(pendingRequests.keySet());
		vertexIds.forEach(this::timeoutPendingRequest);
	}

	public void timeoutPendingRequest(final ExecutionVertexID executionVertexId) {
		final SlotExecutionVertexAssignment slotVertexAssignment = removePendingRequest(executionVertexId);
		checkState(slotVertexAssignment != null);
		slotVertexAssignment
			.getLogicalSlotFuture()
			.completeExceptionally(new TimeoutException());
	}

	public void enableAutoCompletePendingRequests() {
		autoCompletePendingRequests = true;
	}

	public void disableAutoCompletePendingRequests() {
		autoCompletePendingRequests = false;
	}

	@Override
	public void cancel(final ExecutionVertexID executionVertexId) {
		final SlotExecutionVertexAssignment slotVertexAssignment = removePendingRequest(executionVertexId);
		if (slotVertexAssignment != null) {
			slotVertexAssignment
				.getLogicalSlotFuture()
				.cancel(false);
		}
	}

	@Override
	public CompletableFuture<Void> stop() {
		return CompletableFuture.completedFuture(null);
	}

	@Override
	public void returnLogicalSlot(final LogicalSlot logicalSlot) {
		returnedSlots.add(logicalSlot);
	}

	public List<LogicalSlot> getReturnedSlots() {
		return new ArrayList<>(returnedSlots);
	}
}
