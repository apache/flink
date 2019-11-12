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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.Scheduler;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A slot provider where one can pre-set the slot futures for tasks based on
 * vertex ID and subtask index.
 */
class ProgrammedSlotProvider implements Scheduler {

	private final Map<JobVertexID, CompletableFuture<LogicalSlot>[]> slotFutures = new HashMap<>();

	private final Map<JobVertexID, CompletableFuture<Boolean>[]> slotFutureRequested = new HashMap<>();

	private final Set<SlotRequestId> slotRequests = new HashSet<>();

	private final Set<SlotRequestId> canceledSlotRequests = new HashSet<>();

	private final int parallelism;

	public ProgrammedSlotProvider(int parallelism) {
		checkArgument(parallelism > 0);
		this.parallelism = parallelism;
	}

	public void addSlot(JobVertexID vertex, int subtaskIndex, CompletableFuture<LogicalSlot> future) {
		checkNotNull(vertex);
		checkNotNull(future);
		checkArgument(subtaskIndex >= 0 && subtaskIndex < parallelism);

		CompletableFuture<LogicalSlot>[] futures = slotFutures.get(vertex);
		CompletableFuture<Boolean>[] requestedFutures = slotFutureRequested.get(vertex);

		if (futures == null) {
			@SuppressWarnings("unchecked")
			CompletableFuture<LogicalSlot>[] newArray = (CompletableFuture<LogicalSlot>[]) new CompletableFuture<?>[parallelism];
			futures = newArray;
			slotFutures.put(vertex, futures);

			requestedFutures = new CompletableFuture[parallelism];
			slotFutureRequested.put(vertex, requestedFutures);
		}

		futures[subtaskIndex] = future;
		requestedFutures[subtaskIndex] = new CompletableFuture<>();
	}

	public void addSlots(JobVertexID vertex, CompletableFuture<LogicalSlot>[] futures) {
		checkNotNull(vertex);
		checkNotNull(futures);
		checkArgument(futures.length == parallelism);

		slotFutures.put(vertex, futures);

		CompletableFuture<Boolean>[] requestedFutures = new CompletableFuture[futures.length];

		for (int i = 0; i < futures.length; i++) {
			requestedFutures[i] = new CompletableFuture<>();
		}

		slotFutureRequested.put(vertex, requestedFutures);
	}

	public CompletableFuture<Boolean> getSlotRequestedFuture(JobVertexID jobVertexId, int subtaskIndex) {
		return slotFutureRequested.get(jobVertexId)[subtaskIndex];
	}

	public Set<SlotRequestId> getSlotRequests() {
		return Collections.unmodifiableSet(slotRequests);
	}

	public Set<SlotRequestId> getCanceledSlotRequests() {
		return Collections.unmodifiableSet(canceledSlotRequests);
	}

	@Override
	public CompletableFuture<LogicalSlot> allocateSlot(
			SlotRequestId slotRequestId,
			ScheduledUnit task,
			SlotProfile slotProfile,
			Time allocationTimeout) {
		JobVertexID vertexId = task.getTaskToExecute().getVertex().getJobvertexId();
		int subtask = task.getTaskToExecute().getParallelSubtaskIndex();

		CompletableFuture<LogicalSlot>[] forTask = slotFutures.get(vertexId);
		if (forTask != null) {
			CompletableFuture<LogicalSlot> future = forTask[subtask];
			if (future != null) {
				slotFutureRequested.get(vertexId)[subtask].complete(true);
				slotRequests.add(slotRequestId);

				return future;
			}
		}

		throw new IllegalArgumentException("No registered slot future for task " + vertexId + " (" + subtask + ')');
	}

	@Override
	public void cancelSlotRequest(
			SlotRequestId slotRequestId,
			@Nullable SlotSharingGroupId slotSharingGroupId,
			Throwable cause) {
		canceledSlotRequests.add(slotRequestId);
	}

	@Override
	public void start(@Nonnull ComponentMainThreadExecutor mainThreadExecutor) {

	}

	@Override
	public boolean requiresPreviousExecutionGraphAllocations() {
		return false;
	}

	@Override
	public void returnLogicalSlot(LogicalSlot logicalSlot) {
		throw new UnsupportedOperationException();
	}
}
