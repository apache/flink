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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.Slot;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.LocationPreferenceConstraint;
import org.apache.flink.runtime.jobmanager.scheduler.SchedulerTestUtils;
import org.apache.flink.runtime.jobmanager.slots.SlotOwner;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link Execution}.
 */
public class ExecutionTest extends TestLogger {

	/**
	 * Tests that slots are released if we cannot assign the allocated resource to the
	 * Execution. In this case, a concurrent cancellation precedes the assignment.
	 */
	@Test
	public void testSlotReleaseOnFailedResourceAssignment() throws Exception {
		final JobVertexID jobVertexId = new JobVertexID();
		final JobVertex jobVertex = new JobVertex("Test vertex", jobVertexId);
		jobVertex.setInvokableClass(NoOpInvokable.class);

		final CompletableFuture<SimpleSlot> slotFuture = new CompletableFuture<>();
		final ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(1);
		slotProvider.addSlot(jobVertexId, 0, slotFuture);

		ExecutionGraph executionGraph = ExecutionGraphTestUtils.createSimpleTestGraph(
			new JobID(),
			slotProvider,
			new NoRestartStrategy(),
			jobVertex);

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexId);

		final Execution execution = executionJobVertex.getTaskVertices()[0].getCurrentExecutionAttempt();

		final TestingSlotOwner slotOwner = new TestingSlotOwner();

		final SimpleSlot slot = new SimpleSlot(
			new JobID(),
			slotOwner,
			new LocalTaskManagerLocation(),
			0,
			new SimpleAckingTaskManagerGateway());

		CompletableFuture<Execution> allocationFuture = execution.allocateAndAssignSlotForExecution(
			slotProvider,
			false,
			LocationPreferenceConstraint.ALL);

		assertFalse(allocationFuture.isDone());

		assertEquals(ExecutionState.SCHEDULED, execution.getState());

		// cancelling the execution should move it into state CANCELED; this happens before
		// the slot future has been completed
		execution.cancel();

		assertEquals(ExecutionState.CANCELED, execution.getState());

		// completing now the future should cause the slot to be released
		slotFuture.complete(slot);

		assertEquals(slot, slotOwner.getReturnedSlotFuture().get());
	}

	/**
	 * Tests that the slot is released in case of a execution cancellation when having
	 * a slot assigned and being in state SCHEDULED.
	 */
	@Test
	public void testSlotReleaseOnExecutionCancellationInScheduled() throws Exception {
		final JobVertexID jobVertexId = new JobVertexID();
		final JobVertex jobVertex = new JobVertex("Test vertex", jobVertexId);
		jobVertex.setInvokableClass(NoOpInvokable.class);

		final TestingSlotOwner slotOwner = new TestingSlotOwner();

		final SimpleSlot slot = new SimpleSlot(
			new JobID(),
			slotOwner,
			new LocalTaskManagerLocation(),
			0,
			new SimpleAckingTaskManagerGateway());

		final ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(1);
		slotProvider.addSlot(jobVertexId, 0, CompletableFuture.completedFuture(slot));

		ExecutionGraph executionGraph = ExecutionGraphTestUtils.createSimpleTestGraph(
			new JobID(),
			slotProvider,
			new NoRestartStrategy(),
			jobVertex);

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexId);

		final Execution execution = executionJobVertex.getTaskVertices()[0].getCurrentExecutionAttempt();

		CompletableFuture<Execution> allocationFuture = execution.allocateAndAssignSlotForExecution(
			slotProvider,
			false,
			LocationPreferenceConstraint.ALL);

		assertTrue(allocationFuture.isDone());

		assertEquals(ExecutionState.SCHEDULED, execution.getState());

		assertEquals(slot, execution.getAssignedResource());

		// cancelling the execution should move it into state CANCELED
		execution.cancel();
		assertEquals(ExecutionState.CANCELED, execution.getState());

		assertEquals(slot, slotOwner.getReturnedSlotFuture().get());
	}

	/**
	 * Tests that the slot is released in case of a execution cancellation when being in state
	 * RUNNING.
	 */
	@Test
	public void testSlotReleaseOnExecutionCancellationInRunning() throws Exception {
		final JobVertexID jobVertexId = new JobVertexID();
		final JobVertex jobVertex = new JobVertex("Test vertex", jobVertexId);
		jobVertex.setInvokableClass(NoOpInvokable.class);

		final TestingSlotOwner slotOwner = new TestingSlotOwner();

		final SimpleSlot slot = new SimpleSlot(
			new JobID(),
			slotOwner,
			new LocalTaskManagerLocation(),
			0,
			new SimpleAckingTaskManagerGateway());

		final ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(1);
		slotProvider.addSlot(jobVertexId, 0, CompletableFuture.completedFuture(slot));

		ExecutionGraph executionGraph = ExecutionGraphTestUtils.createSimpleTestGraph(
			new JobID(),
			slotProvider,
			new NoRestartStrategy(),
			jobVertex);

		ExecutionJobVertex executionJobVertex = executionGraph.getJobVertex(jobVertexId);

		final Execution execution = executionJobVertex.getTaskVertices()[0].getCurrentExecutionAttempt();

		CompletableFuture<Execution> allocationFuture = execution.allocateAndAssignSlotForExecution(
			slotProvider,
			false,
			LocationPreferenceConstraint.ALL);

		assertTrue(allocationFuture.isDone());

		assertEquals(ExecutionState.SCHEDULED, execution.getState());

		assertEquals(slot, execution.getAssignedResource());

		execution.deploy();

		execution.switchToRunning();

		// cancelling the execution should move it into state CANCELING
		execution.cancel();
		assertEquals(ExecutionState.CANCELING, execution.getState());

		execution.cancelingComplete();

		assertEquals(slot, slotOwner.getReturnedSlotFuture().get());
	}

	/**
	 * Tests that all preferred locations are calculated.
	 */
	@Test
	public void testAllPreferredLocationCalculation() throws ExecutionException, InterruptedException {
		final TaskManagerLocation taskManagerLocation1 = new LocalTaskManagerLocation();
		final TaskManagerLocation taskManagerLocation2 = new LocalTaskManagerLocation();
		final TaskManagerLocation taskManagerLocation3 = new LocalTaskManagerLocation();

		final CompletableFuture<TaskManagerLocation> locationFuture1 = CompletableFuture.completedFuture(taskManagerLocation1);
		final CompletableFuture<TaskManagerLocation> locationFuture2 = new CompletableFuture<>();
		final CompletableFuture<TaskManagerLocation> locationFuture3 = new CompletableFuture<>();

		final Execution execution = SchedulerTestUtils.getTestVertex(Arrays.asList(locationFuture1, locationFuture2, locationFuture3));

		CompletableFuture<Collection<TaskManagerLocation>> preferredLocationsFuture = execution.calculatePreferredLocations(LocationPreferenceConstraint.ALL);

		assertFalse(preferredLocationsFuture.isDone());

		locationFuture3.complete(taskManagerLocation3);

		assertFalse(preferredLocationsFuture.isDone());

		locationFuture2.complete(taskManagerLocation2);

		assertTrue(preferredLocationsFuture.isDone());

		final Collection<TaskManagerLocation> preferredLocations = preferredLocationsFuture.get();

		assertThat(preferredLocations, containsInAnyOrder(taskManagerLocation1, taskManagerLocation2, taskManagerLocation3));
	}

	/**
	 * Tests that any preferred locations are calculated.
	 */
	@Test
	public void testAnyPreferredLocationCalculation() throws ExecutionException, InterruptedException {
		final TaskManagerLocation taskManagerLocation1 = new LocalTaskManagerLocation();
		final TaskManagerLocation taskManagerLocation3 = new LocalTaskManagerLocation();

		final CompletableFuture<TaskManagerLocation> locationFuture1 = CompletableFuture.completedFuture(taskManagerLocation1);
		final CompletableFuture<TaskManagerLocation> locationFuture2 = new CompletableFuture<>();
		final CompletableFuture<TaskManagerLocation> locationFuture3 = CompletableFuture.completedFuture(taskManagerLocation3);

		final Execution execution = SchedulerTestUtils.getTestVertex(Arrays.asList(locationFuture1, locationFuture2, locationFuture3));

		CompletableFuture<Collection<TaskManagerLocation>> preferredLocationsFuture = execution.calculatePreferredLocations(LocationPreferenceConstraint.ANY);

		assertTrue(preferredLocationsFuture.isDone());

		final Collection<TaskManagerLocation> preferredLocations = preferredLocationsFuture.get();

		assertThat(preferredLocations, containsInAnyOrder(taskManagerLocation1, taskManagerLocation3));
	}

	/**
	 * Slot owner which records the first returned slot.
	 */
	public static final class TestingSlotOwner implements SlotOwner {

		final CompletableFuture<Slot> returnedSlot = new CompletableFuture<>();

		public CompletableFuture<Slot> getReturnedSlotFuture() {
			return returnedSlot;
		}

		@Override
		public boolean returnAllocatedSlot(Slot slot) {
			return returnedSlot.complete(slot);
		}
	}
}
