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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TestingComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Tests for {@link DefaultSchedulerOperations}.
 */
public class DefaultSchedulerOperationsTest extends TestLogger {

	/**
	 * Tests that when all slots are return, the executions will be deployed.
	 */
	@Test
	public void testAllocateSlotsAndDeploy() throws Exception {
		ExecutionGraph eg = ExecutionGraphTestUtils.createSimpleTestGraph();
		TestingExecutionSlotAllocator executionSlotAllocator = new TestingExecutionSlotAllocator();

		List<ExecutionVertexDeploymentOption> deploymentOptions = new ArrayList<>();
		for (ExecutionVertex executionVertex : eg.getAllExecutionVertices()) {
			deploymentOptions.add(new ExecutionVertexDeploymentOption(
					new ExecutionVertexID(executionVertex.getJobvertexId(), executionVertex.getParallelSubtaskIndex()),
					new DeploymentOption(false)
			));
			executionSlotAllocator.addSlot(CompletableFuture.completedFuture(new TestingLogicalSlot()));
		}

		DefaultSchedulerOperations schedulerOperations =
				new DefaultSchedulerOperations(eg, executionSlotAllocator, new DirectScheduledExecutorService());
		schedulerOperations.allocateSlotsAndDeploy(deploymentOptions);

		Predicate<AccessExecution> predicate = (execution) -> execution.getState() == ExecutionState.DEPLOYING;
		ExecutionGraphTestUtils.waitForAllExecutionsPredicate(eg, predicate, 2000L);
		assertEquals(0, executionSlotAllocator.getAvailSlotNumber());
	}

	/**
	 * Tests that when the slots request fail, unfulfilled requests will be cancelled.
	 */
	@Test
	public void testSlotsWillBeReturnedWhenAllocationFail() throws Exception {
		ExecutionGraph eg = ExecutionGraphTestUtils.createSimpleTestGraph();
		eg.start(TestingComponentMainThreadExecutorServiceAdapter.forMainThread());
		TestingExecutionSlotAllocator executionSlotAllocator = new TestingExecutionSlotAllocator();

		List<ExecutionVertexDeploymentOption> deploymentOptions = new ArrayList<>();
		for (ExecutionVertex executionVertex : eg.getAllExecutionVertices()) {
			deploymentOptions.add(new ExecutionVertexDeploymentOption(
					new ExecutionVertexID(executionVertex.getJobvertexId(), executionVertex.getParallelSubtaskIndex()),
					new DeploymentOption(false)
			));
		}

		// Half slot are fulfilled, and then an exception occur.
		List<LogicalSlot> slots = new ArrayList<>(deploymentOptions.size() / 2);
		for (int i = 0; i < deploymentOptions.size() / 2; i++) {
			LogicalSlot logicSlot = new TestingLogicalSlot();
			executionSlotAllocator.addSlot(CompletableFuture.completedFuture(logicSlot));
			slots.add(logicSlot);
		}
		for (int i = deploymentOptions.size() / 2; i < deploymentOptions.size() - 1; i++) {
			executionSlotAllocator.addSlot(new CompletableFuture<>());
		}
		executionSlotAllocator.addSlot(FutureUtils.completedExceptionally(new Exception("Test exception")));

		DefaultSchedulerOperations schedulerOperations =
				new DefaultSchedulerOperations(eg, executionSlotAllocator, new DirectScheduledExecutorService());
		schedulerOperations.allocateSlotsAndDeploy(deploymentOptions);

		ExecutionGraphTestUtils.waitUntilJobStatus(eg, JobStatus.FAILED, 2000L);
		assertEquals(0, executionSlotAllocator.getAvailSlotNumber());
		assertEquals(deploymentOptions.size() - deploymentOptions.size() / 2, executionSlotAllocator.getCancelledSlotRequestSize());
		for (LogicalSlot slot : slots) {
			assertFalse(slot.isAlive());
		}
	}

	private class TestingExecutionSlotAllocator implements ExecutionSlotAllocator {

		private final ArrayDeque<CompletableFuture<LogicalSlot>> slots =  new ArrayDeque<>();

		private final List<ExecutionVertexID> cancelledSlotReuqests = new ArrayList<>();

		@Override
		public Collection<SlotExecutionVertexAssignment> allocateSlotsFor(
				Collection<ExecutionVertexSchedulingRequirements> executionVertexSchedulingRequirements) {

			List<SlotExecutionVertexAssignment> assignments = new ArrayList<>(executionVertexSchedulingRequirements.size());
			for (ExecutionVertexSchedulingRequirements schedulingRequirements : executionVertexSchedulingRequirements) {
				CompletableFuture<LogicalSlot> slotFuture = slots.pollFirst();
				if (slotFuture == null) {
					slotFuture = FutureUtils.completedExceptionally(new NoResourceAvailableException("No slots anymore"));
				}
				assignments.add(new SlotExecutionVertexAssignment(
						schedulingRequirements.getExecutionVertexId(),
						slotFuture));
			}
			return assignments;
		}

		@Override
		public void cancel(SlotExecutionVertexAssignment slotExecutionVertexAssignment) {
			cancelledSlotReuqests.add(slotExecutionVertexAssignment.getExecutionVertexId());
		}

		@Override
		public CompletableFuture<Void> stop() {
			return null;
		}

		public void addSlot(CompletableFuture<LogicalSlot> logicalSlot) {
			slots.add(logicalSlot);
		}

		public int getAvailSlotNumber() {
			return slots.size();
		}

		public int getCancelledSlotRequestSize() {
			return cancelledSlotReuqests.size();
		}
	}
}
