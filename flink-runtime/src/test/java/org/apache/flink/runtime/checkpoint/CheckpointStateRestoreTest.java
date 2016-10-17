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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.stats.DisabledCheckpointStatsTracker;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.ExternalizedCheckpointSettings;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.runtime.util.SerializableObject;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests concerning the restoring of state from a checkpoint to the task executions.
 */
public class CheckpointStateRestoreTest {

	@Test
	public void testSetState() {
		try {

			final ChainedStateHandle<StreamStateHandle> serializedState = CheckpointCoordinatorTest.generateChainedStateHandle(new SerializableObject());
			KeyGroupRange keyGroupRange = KeyGroupRange.of(0,0);
			List<SerializableObject> testStates = Collections.singletonList(new SerializableObject());
			final KeyGroupsStateHandle serializedKeyGroupStates = CheckpointCoordinatorTest.generateKeyGroupState(keyGroupRange, testStates);

			final JobID jid = new JobID();
			final JobVertexID statefulId = new JobVertexID();
			final JobVertexID statelessId = new JobVertexID();

			Execution statefulExec1 = mockExecution();
			Execution statefulExec2 = mockExecution();
			Execution statefulExec3 = mockExecution();
			Execution statelessExec1 = mockExecution();
			Execution statelessExec2 = mockExecution();

			ExecutionVertex stateful1 = mockExecutionVertex(statefulExec1, statefulId, 0, 3);
			ExecutionVertex stateful2 = mockExecutionVertex(statefulExec2, statefulId, 1, 3);
			ExecutionVertex stateful3 = mockExecutionVertex(statefulExec3, statefulId, 2, 3);
			ExecutionVertex stateless1 = mockExecutionVertex(statelessExec1, statelessId, 0, 2);
			ExecutionVertex stateless2 = mockExecutionVertex(statelessExec2, statelessId, 1, 2);

			ExecutionJobVertex stateful = mockExecutionJobVertex(statefulId,
					new ExecutionVertex[] { stateful1, stateful2, stateful3 });
			ExecutionJobVertex stateless = mockExecutionJobVertex(statelessId,
					new ExecutionVertex[] { stateless1, stateless2 });

			Map<JobVertexID, ExecutionJobVertex> map = new HashMap<JobVertexID, ExecutionJobVertex>();
			map.put(statefulId, stateful);
			map.put(statelessId, stateless);


			CheckpointCoordinator coord = new CheckpointCoordinator(
					jid,
					200000L,
					200000L,
					0,
					Integer.MAX_VALUE,
					ExternalizedCheckpointSettings.none(),
					new ExecutionVertex[] { stateful1, stateful2, stateful3, stateless1, stateless2 },
					new ExecutionVertex[] { stateful1, stateful2, stateful3, stateless1, stateless2 },
					new ExecutionVertex[0],
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(1),
					null,
					new DisabledCheckpointStatsTracker());

			// create ourselves a checkpoint with state
			final long timestamp = 34623786L;
			coord.triggerCheckpoint(timestamp, false);

			PendingCheckpoint pending = coord.getPendingCheckpoints().values().iterator().next();
			final long checkpointId = pending.getCheckpointId();

			SubtaskState checkpointStateHandles = new SubtaskState(serializedState, null, null, serializedKeyGroupStates, null, 0L);
			CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, 0L);
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec1.getAttemptId(), checkpointMetaData, checkpointStateHandles));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec2.getAttemptId(), checkpointMetaData, checkpointStateHandles));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec3.getAttemptId(), checkpointMetaData, checkpointStateHandles));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statelessExec1.getAttemptId(), checkpointMetaData));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statelessExec2.getAttemptId(), checkpointMetaData));

			assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(0, coord.getNumberOfPendingCheckpoints());

			// let the coordinator inject the state
			coord.restoreLatestCheckpointedState(map, true, false);

			// verify that each stateful vertex got the state

			final TaskStateHandles taskStateHandles = new TaskStateHandles(
					serializedState,
					Collections.<Collection<OperatorStateHandle>>singletonList(null),
					Collections.<Collection<OperatorStateHandle>>singletonList(null),
					Collections.singletonList(serializedKeyGroupStates),
					null);

			BaseMatcher<TaskStateHandles> matcher = new BaseMatcher<TaskStateHandles>() {
				@Override
				public boolean matches(Object o) {
					if (o instanceof TaskStateHandles) {
						return o.equals(taskStateHandles);
					}
					return false;
				}

				@Override
				public void describeTo(Description description) {
					description.appendValue(taskStateHandles);
				}
			};

			verify(statefulExec1, times(1)).setInitialState(Mockito.argThat(matcher));
			verify(statefulExec2, times(1)).setInitialState(Mockito.argThat(matcher));
			verify(statefulExec3, times(1)).setInitialState(Mockito.argThat(matcher));
			verify(statelessExec1, times(0)).setInitialState(Mockito.<TaskStateHandles>any());
			verify(statelessExec2, times(0)).setInitialState(Mockito.<TaskStateHandles>any());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testStateOnlyPartiallyAvailable() {
		try {
			final ChainedStateHandle<StreamStateHandle> serializedState = CheckpointCoordinatorTest.generateChainedStateHandle(new SerializableObject());
			KeyGroupRange keyGroupRange = KeyGroupRange.of(0,0);
			List<SerializableObject> testStates = Collections.singletonList(new SerializableObject());
			final KeyGroupsStateHandle serializedKeyGroupStates = CheckpointCoordinatorTest.generateKeyGroupState(keyGroupRange, testStates);

			final JobID jid = new JobID();
			final JobVertexID statefulId = new JobVertexID();
			final JobVertexID statelessId = new JobVertexID();

			Execution statefulExec1 = mockExecution();
			Execution statefulExec2 = mockExecution();
			Execution statefulExec3 = mockExecution();
			Execution statelessExec1 = mockExecution();
			Execution statelessExec2 = mockExecution();

			ExecutionVertex stateful1 = mockExecutionVertex(statefulExec1, statefulId, 0, 3);
			ExecutionVertex stateful2 = mockExecutionVertex(statefulExec2, statefulId, 1, 3);
			ExecutionVertex stateful3 = mockExecutionVertex(statefulExec3, statefulId, 2, 3);
			ExecutionVertex stateless1 = mockExecutionVertex(statelessExec1, statelessId, 0, 2);
			ExecutionVertex stateless2 = mockExecutionVertex(statelessExec2, statelessId, 1, 2);

			ExecutionJobVertex stateful = mockExecutionJobVertex(statefulId,
					new ExecutionVertex[] { stateful1, stateful2, stateful3 });
			ExecutionJobVertex stateless = mockExecutionJobVertex(statelessId,
					new ExecutionVertex[] { stateless1, stateless2 });

			Map<JobVertexID, ExecutionJobVertex> map = new HashMap<JobVertexID, ExecutionJobVertex>();
			map.put(statefulId, stateful);
			map.put(statelessId, stateless);


			CheckpointCoordinator coord = new CheckpointCoordinator(
					jid,
					200000L,
					200000L,
					0,
					Integer.MAX_VALUE,
					ExternalizedCheckpointSettings.none(),
					new ExecutionVertex[] { stateful1, stateful2, stateful3, stateless1, stateless2 },
					new ExecutionVertex[] { stateful1, stateful2, stateful3, stateless1, stateless2 },
					new ExecutionVertex[0],
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(1),
					null,
					new DisabledCheckpointStatsTracker());

			// create ourselves a checkpoint with state
			final long timestamp = 34623786L;
			coord.triggerCheckpoint(timestamp, false);

			PendingCheckpoint pending = coord.getPendingCheckpoints().values().iterator().next();
			final long checkpointId = pending.getCheckpointId();

			// the difference to the test "testSetState" is that one stateful subtask does not report state
			SubtaskState checkpointStateHandles =
					new SubtaskState(serializedState, null, null, serializedKeyGroupStates, null, 0L);

			CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, 0L);

			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec1.getAttemptId(), checkpointMetaData, checkpointStateHandles));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec2.getAttemptId(), checkpointMetaData));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec3.getAttemptId(), checkpointMetaData, checkpointStateHandles));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statelessExec1.getAttemptId(), checkpointMetaData));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statelessExec2.getAttemptId(), checkpointMetaData));

			assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(0, coord.getNumberOfPendingCheckpoints());

			// let the coordinator inject the state
			try {
				coord.restoreLatestCheckpointedState(map, true, true);
				fail("this should fail with an exception");
			}
			catch (IllegalStateException e) {
				// swish!
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testNoCheckpointAvailable() {
		try {
			CheckpointCoordinator coord = new CheckpointCoordinator(
					new JobID(),
					200000L,
					200000L,
					0,
					Integer.MAX_VALUE,
					ExternalizedCheckpointSettings.none(),
					new ExecutionVertex[] { mock(ExecutionVertex.class) },
					new ExecutionVertex[] { mock(ExecutionVertex.class) },
					new ExecutionVertex[0],
					new StandaloneCheckpointIDCounter(),
					new StandaloneCompletedCheckpointStore(1),
					null,
					new DisabledCheckpointStatsTracker());

			try {
				coord.restoreLatestCheckpointedState(new HashMap<JobVertexID, ExecutionJobVertex>(), true, false);
				fail("this should throw an exception");
			}
			catch (IllegalStateException e) {
				// expected
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// ------------------------------------------------------------------------

	private Execution mockExecution() {
		return mockExecution(ExecutionState.RUNNING);
	}

	private Execution mockExecution(ExecutionState state) {
		Execution mock = mock(Execution.class);
		when(mock.getAttemptId()).thenReturn(new ExecutionAttemptID());
		when(mock.getState()).thenReturn(state);
		return mock;
	}

	private ExecutionVertex mockExecutionVertex(Execution execution, JobVertexID vertexId, int subtask, int parallelism) {
		ExecutionVertex mock = mock(ExecutionVertex.class);
		when(mock.getJobvertexId()).thenReturn(vertexId);
		when(mock.getParallelSubtaskIndex()).thenReturn(subtask);
		when(mock.getCurrentExecutionAttempt()).thenReturn(execution);
		when(mock.getTotalNumberOfParallelSubtasks()).thenReturn(parallelism);
		when(mock.getMaxParallelism()).thenReturn(parallelism);
		return mock;
	}

	private ExecutionJobVertex mockExecutionJobVertex(JobVertexID id, ExecutionVertex[] vertices) {
		ExecutionJobVertex vertex = mock(ExecutionJobVertex.class);
		when(vertex.getParallelism()).thenReturn(vertices.length);
		when(vertex.getMaxParallelism()).thenReturn(vertices.length);
		when(vertex.getJobVertexId()).thenReturn(id);
		when(vertex.getTaskVertices()).thenReturn(vertices);
		return vertex;
	}
}
