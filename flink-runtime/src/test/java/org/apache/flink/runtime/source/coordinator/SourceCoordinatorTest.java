/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.api.connector.source.mocks.MockSplitEnumerator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.source.event.AddSplitEvent;
import org.apache.flink.runtime.source.event.ReaderRegistrationEvent;
import org.apache.flink.runtime.source.event.SourceEventWrapper;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.verifyAssignment;
import static org.apache.flink.runtime.source.coordinator.CoordinatorTestUtils.verifyException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link SourceCoordinator}.
 */
public class SourceCoordinatorTest extends SourceCoordinatorTestBase {

	@Test
	public void testThrowExceptionWhenNotStarted() {
		// The following methods should only be invoked after the source coordinator has started.
		String failureMessage = "Call should fail when source coordinator has not started yet.";
		verifyException(() -> sourceCoordinator.checkpointComplete(100L),
				failureMessage, "The coordinator has not started yet.");
		verifyException(() -> sourceCoordinator.handleEventFromOperator(0, null),
				failureMessage, "The coordinator has not started yet.");
		verifyException(() -> sourceCoordinator.subtaskFailed(0, null),
				failureMessage, "The coordinator has not started yet.");
		verifyException(() -> sourceCoordinator.checkpointCoordinator(100L),
				failureMessage, "The coordinator has not started yet.");
	}

	@Test
	public void testRestCheckpointAfterCoordinatorStarted() throws Exception {
		// The following methods should only be invoked after the source coordinator has started.
		sourceCoordinator.start();
		verifyException(() -> sourceCoordinator.resetToCheckpoint(null),
				"Reset to checkpoint should fail after the coordinator has started",
				String.format("The coordinator for source %s has started. The source coordinator state can " +
						"only be reset to a checkpoint before it starts.", OPERATOR_NAME));
	}

	@Test
	public void testStart() throws Exception {
		assertFalse(enumerator.started());
		sourceCoordinator.start();
		assertTrue(enumerator.started());
	}

	@Test
	public void testClosed() throws Exception {
		assertFalse(enumerator.closed());
		sourceCoordinator.start();
		sourceCoordinator.close();
		assertTrue(enumerator.closed());
	}

	@Test
	public void testReaderRegistration() throws Exception {
		sourceCoordinator.start();
		sourceCoordinator.handleEventFromOperator(
				0, new ReaderRegistrationEvent(0, "location_0"));
		check(() -> {
			assertEquals("2 splits should have been assigned to reader 0",
					4, enumerator.getUnassignedSplits().size());
			assertTrue(context.registeredReaders().containsKey(0));
			assertTrue(enumerator.getHandledSourceEvent().isEmpty());
			verifyAssignment(Arrays.asList("0", "3"), splitSplitAssignmentTracker.uncheckpointedAssignments().get(0));
		});
	}

	@Test
	public void testHandleSourceEvent() throws Exception {
		sourceCoordinator.start();
		SourceEvent sourceEvent = new SourceEvent() {};
		sourceCoordinator.handleEventFromOperator(0, new SourceEventWrapper(sourceEvent));
		check(() -> {
			assertEquals(1, enumerator.getHandledSourceEvent().size());
			assertEquals(sourceEvent, enumerator.getHandledSourceEvent().get(0));
		});
	}

	@Test
	public void testCheckpointCoordinatorAndRestore() throws Exception {
		sourceCoordinator.start();
		sourceCoordinator.handleEventFromOperator(
				0, new ReaderRegistrationEvent(0, "location_0"));
		byte[] bytes = sourceCoordinator.checkpointCoordinator(100L).get();

		// restore from the checkpoints.
		SourceCoordinator<?, ?> restoredCoordinator = getNewSourceCoordinator();
		restoredCoordinator.resetToCheckpoint(bytes);
		MockSplitEnumerator restoredEnumerator = (MockSplitEnumerator) restoredCoordinator.getEnumerator();
		SourceCoordinatorContext restoredContext = restoredCoordinator.getContext();
		assertEquals("2 splits should have been assigned to reader 0",
				4, restoredEnumerator.getUnassignedSplits().size());
		assertTrue(restoredEnumerator.getHandledSourceEvent().isEmpty());
		assertEquals(1, restoredContext.registeredReaders().size());
		assertTrue(restoredContext.registeredReaders().containsKey(0));
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSubtaskFailedAndRevertUncompletedAssignments() throws Exception {
		sourceCoordinator.start();

		// Assign some splits to reader 0 then take snapshot 100.
		sourceCoordinator.handleEventFromOperator(
				0, new ReaderRegistrationEvent(0, "location_0"));
		sourceCoordinator.checkpointCoordinator(100L).get();

		// Add split 6, assign it to reader 0 and take another snapshot 101.
		enumerator.addNewSplits(Collections.singletonList(new MockSourceSplit(6)));
		sourceCoordinator.checkpointCoordinator(101L).get();

		// check the state.
		check(() -> {
			// There should be 4 unassigned splits.
			assertEquals(4, enumerator.getUnassignedSplits().size());
			verifyAssignment(
					Arrays.asList("0", "3"),
					splitSplitAssignmentTracker.assignmentsByCheckpointId().get(100L).get(0));
			assertTrue(splitSplitAssignmentTracker.uncheckpointedAssignments().isEmpty());
			verifyAssignment(Arrays.asList("0", "3"), splitSplitAssignmentTracker.assignmentsByCheckpointId(100L).get(0));
			verifyAssignment(Arrays.asList("6"), splitSplitAssignmentTracker.assignmentsByCheckpointId(101L).get(0));

			List<OperatorEvent> eventsToReader0 = operatorCoordinatorContext.getEventsToOperator().get(0);
			assertEquals(2, eventsToReader0.size());
			verifyAssignment(Arrays.asList("0", "3"), ((AddSplitEvent<MockSourceSplit>) eventsToReader0.get(0)).splits());
			verifyAssignment(Arrays.asList("6"), ((AddSplitEvent<MockSourceSplit>) eventsToReader0.get(1)).splits());
		});

		// Fail reader 0.
		sourceCoordinator.subtaskFailed(0, null);

		// check the state again.
		check(() -> {
			//
			assertFalse("Reader 0 should have been unregistered.",
					context.registeredReaders().containsKey(0));
			// The tracker should have reverted all the splits assignment to reader 0.
			for (Map<Integer, ?> assignment : splitSplitAssignmentTracker.assignmentsByCheckpointId().values()) {
				assertFalse("Assignment in uncompleted checkpoint should have been reverted.",
						assignment.containsKey(0));
			}
			assertFalse(splitSplitAssignmentTracker.uncheckpointedAssignments().containsKey(0));
			// The split enumerator should now contains the splits used to be assigned to reader 0.
			assertEquals(7, enumerator.getUnassignedSplits().size());
		});
	}

	@Test
	public void testFailedSubtaskDoNotRevertCompletedCheckpoint() throws Exception {
		sourceCoordinator.start();

		// Assign some splits to reader 0 then take snapshot 100.
		sourceCoordinator.handleEventFromOperator(
				0, new ReaderRegistrationEvent(0, "location_0"));
		sourceCoordinator.checkpointCoordinator(100L).get();
		// Complete checkpoint 100.
		sourceCoordinator.checkpointComplete(100L);

		// Fail reader 0.
		sourceCoordinator.subtaskFailed(0, null);

		check(() -> {
			// Reader 0 hase been unregistered.
			assertFalse(context.registeredReaders().containsKey(0));
			// The assigned splits are not reverted.
			assertEquals(4, enumerator.getUnassignedSplits().size());
			assertFalse(splitSplitAssignmentTracker.uncheckpointedAssignments().containsKey(0));
			assertTrue(splitSplitAssignmentTracker.assignmentsByCheckpointId().isEmpty());
		});
	}

	// -------------------------------

	private void check(Runnable runnable) {
		try {
			coordinatorExecutor.submit(runnable).get();
		} catch (Exception e) {
			fail("Test failed due to " + e);
		}
	}
}
