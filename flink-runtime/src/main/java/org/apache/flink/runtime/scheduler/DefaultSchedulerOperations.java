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

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default {@link SchedulerOperations} which will allocate slots and deploy the vertices when all slots are returned.
 */
public class DefaultSchedulerOperations implements SchedulerOperations {

	static final Logger LOG = LoggerFactory.getLogger(DefaultSchedulerOperations.class);

	private final ExecutionGraph executionGraph;

	/**
	 * Allocator used for allocating slots.
	 */
	private final ExecutionSlotAllocator executionSlotAllocator;

	/**
	 * All operations should be executed in job master's main thread to make sure no concurrent modification.
	 */
	private final Executor jobMasterMainThreadExecutor;

	public DefaultSchedulerOperations(
			ExecutionGraph executionGraph,
			ExecutionSlotAllocator executionSlotAllocator,
			Executor executor) {
		this.executionGraph = checkNotNull(executionGraph);
		this.executionSlotAllocator = checkNotNull(executionSlotAllocator);
		this.jobMasterMainThreadExecutor = checkNotNull(executor);
	}

	@Override
	public void allocateSlotsAndDeploy(Collection<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptionList =
				new ArrayList<>(executionVertexDeploymentOptions);
		List<ExecutionVertexSchedulingRequirements> schedulingRequirements =
				new ArrayList<>(executionVertexDeploymentOptionList.size());
		List<Execution> scheduledExecutions = new ArrayList<>(executionVertexDeploymentOptionList.size());

		for (ExecutionVertexDeploymentOption executionVertexDeploymentOption : executionVertexDeploymentOptionList) {
			JobVertexID jobVertexId = executionVertexDeploymentOption.getExecutionVertexId().getJobVertexId();
			int subtaskIndex = executionVertexDeploymentOption.getExecutionVertexId().getSubtaskIndex();
			Execution execution = executionGraph.getJobVertex(jobVertexId).getTaskVertices()[subtaskIndex].getCurrentExecutionAttempt();
			schedulingRequirements.add(execution.startSchedulingAndCreateSchedulingRequirements());
			scheduledExecutions.add(execution);
		}

		// Allocates slots for the executions. The returned assignments should be in the same order with requirements.
		List<CompletableFuture<LogicalSlot>> allocationFutures = new ArrayList<>(executionVertexDeploymentOptionList.size());
		Collection<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
				executionSlotAllocator.allocateSlotsFor(schedulingRequirements);

		int i = 0;
		for (SlotExecutionVertexAssignment slotExecutionVertexAssignment : slotExecutionVertexAssignments) {
			final int index = i++;
			allocationFutures.add(slotExecutionVertexAssignment.getLogicalSlotFuture());
			slotExecutionVertexAssignment.getLogicalSlotFuture().handleAsync(
					(LogicalSlot logicalSlot, Throwable failure) -> {
						if (failure == null) {
							if (!scheduledExecutions.get(index).tryAssignResource(logicalSlot)) {
								// release the slot
								Exception e = new FlinkException("Could not assign logical slot to execution " + this + '.');
								logicalSlot.releaseSlot(e);
							}
						} else {
							executionSlotAllocator.cancel(slotExecutionVertexAssignment);
						}
						return null;
					}, jobMasterMainThreadExecutor);
		}

		// When all slots are returned, deploy the tasks.
		FutureUtils.ConjunctFuture<Collection<LogicalSlot>> combinedAllocationFuture = FutureUtils.combineAll(allocationFutures);
		combinedAllocationFuture.whenCompleteAsync(
				(ignored, throwable) -> {
					if (throwable == null) {
						for (int j = 0; j < executionVertexDeploymentOptionList.size(); j++) {
							try {
								scheduledExecutions.get(j).deploy(executionVertexDeploymentOptionList.get(j).getDeploymentOption());
							} catch (Exception e) {
								scheduledExecutions.get(j).fail(e);
							}
						}
					} else {
						LOG.info("Batch request {} slot, but only {} are fulfilled.",
								combinedAllocationFuture.getNumFuturesTotal(),
								combinedAllocationFuture.getNumFuturesCompleted(),
								throwable);
						// Cancel unfulfilled slot request.
						for (SlotExecutionVertexAssignment slotExecutionVertexAssignment : slotExecutionVertexAssignments) {
							slotExecutionVertexAssignment.getLogicalSlotFuture().completeExceptionally(throwable);
						}
						// Fail all executions.
						for (int j = 0; j < scheduledExecutions.size(); j++) {
							scheduledExecutions.get(j).fail(throwable);
						}
					}
				}, jobMasterMainThreadExecutor);
	}
}
