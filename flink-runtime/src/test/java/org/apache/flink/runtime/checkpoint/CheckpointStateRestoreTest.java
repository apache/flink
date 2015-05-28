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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.state.LocalStateHandle;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.util.SerializableObject;
import org.apache.flink.runtime.util.SerializedValue;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

/**
 * Tests concerning the restoring of state from a checkpoint to the task executions.
 */
public class CheckpointStateRestoreTest {
	
	ClassLoader cl = Thread.currentThread().getContextClassLoader();
	
	@Test
	public void testSetState() {
		try {
			final SerializedValue<StateHandle<?>> serializedState = new SerializedValue<StateHandle<?>>(
					new LocalStateHandle(new SerializableObject()));
			
			final JobID jid = new JobID();
			final JobVertexID statefulId = new JobVertexID();
			final JobVertexID statelessId = new JobVertexID();
			
			Execution statefulExec1 = mockExecution();
			Execution statefulExec2 = mockExecution();
			Execution statefulExec3 = mockExecution();
			Execution statelessExec1 = mockExecution();
			Execution statelessExec2 = mockExecution();
			
			ExecutionVertex stateful1 = mockExecutionVertex(statefulExec1, statefulId, 0);
			ExecutionVertex stateful2 = mockExecutionVertex(statefulExec2, statefulId, 1);
			ExecutionVertex stateful3 = mockExecutionVertex(statefulExec3, statefulId, 2);
			ExecutionVertex stateless1 = mockExecutionVertex(statelessExec1, statelessId, 0);
			ExecutionVertex stateless2 = mockExecutionVertex(statelessExec2, statelessId, 1);

			ExecutionJobVertex stateful = mockExecutionJobVertex(statefulId,
					new ExecutionVertex[] { stateful1, stateful2, stateful3 });
			ExecutionJobVertex stateless = mockExecutionJobVertex(statelessId,
					new ExecutionVertex[] { stateless1, stateless2 });
			
			Map<JobVertexID, ExecutionJobVertex> map = new HashMap<JobVertexID, ExecutionJobVertex>();
			map.put(statefulId, stateful);
			map.put(statelessId, stateless);
			
			
			CheckpointCoordinator coord = new CheckpointCoordinator(jid, 1, 200000L, 
					new ExecutionVertex[] { stateful1, stateful2, stateful3, stateless1, stateless2 },
					new ExecutionVertex[] { stateful1, stateful2, stateful3, stateless1, stateless2 },
					new ExecutionVertex[0], cl);
			
			// create ourselves a checkpoint with state
			final long timestamp = 34623786L;
			coord.triggerCheckpoint(timestamp);
			
			PendingCheckpoint pending = coord.getPendingCheckpoints().values().iterator().next();
			final long checkpointId = pending.getCheckpointId();
			
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec1.getAttemptId(), checkpointId, serializedState));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec2.getAttemptId(), checkpointId, serializedState));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec3.getAttemptId(), checkpointId, serializedState));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statelessExec1.getAttemptId(), checkpointId));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statelessExec2.getAttemptId(), checkpointId));
			
			assertEquals(1, coord.getNumberOfRetainedSuccessfulCheckpoints());
			assertEquals(0, coord.getNumberOfPendingCheckpoints());
			
			// let the coordinator inject the state
			coord.restoreLatestCheckpointedState(map, true, false);
			
			// verify that each stateful vertex got the state
			verify(statefulExec1, times(1)).setInitialState(serializedState);
			verify(statefulExec2, times(1)).setInitialState(serializedState);
			verify(statefulExec3, times(1)).setInitialState(serializedState);
			verify(statelessExec1, times(0)).setInitialState(Mockito.<SerializedValue<StateHandle<?>>>any());
			verify(statelessExec2, times(0)).setInitialState(Mockito.<SerializedValue<StateHandle<?>>>any());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testStateOnlyPartiallyAvailable() {
		try {
			final SerializedValue<StateHandle<?>> serializedState = new SerializedValue<StateHandle<?>>(
					new LocalStateHandle(new SerializableObject()));

			final JobID jid = new JobID();
			final JobVertexID statefulId = new JobVertexID();
			final JobVertexID statelessId = new JobVertexID();

			Execution statefulExec1 = mockExecution();
			Execution statefulExec2 = mockExecution();
			Execution statefulExec3 = mockExecution();
			Execution statelessExec1 = mockExecution();
			Execution statelessExec2 = mockExecution();

			ExecutionVertex stateful1 = mockExecutionVertex(statefulExec1, statefulId, 0);
			ExecutionVertex stateful2 = mockExecutionVertex(statefulExec2, statefulId, 1);
			ExecutionVertex stateful3 = mockExecutionVertex(statefulExec3, statefulId, 2);
			ExecutionVertex stateless1 = mockExecutionVertex(statelessExec1, statelessId, 0);
			ExecutionVertex stateless2 = mockExecutionVertex(statelessExec2, statelessId, 1);

			ExecutionJobVertex stateful = mockExecutionJobVertex(statefulId,
					new ExecutionVertex[] { stateful1, stateful2, stateful3 });
			ExecutionJobVertex stateless = mockExecutionJobVertex(statelessId,
					new ExecutionVertex[] { stateless1, stateless2 });

			Map<JobVertexID, ExecutionJobVertex> map = new HashMap<JobVertexID, ExecutionJobVertex>();
			map.put(statefulId, stateful);
			map.put(statelessId, stateless);


			CheckpointCoordinator coord = new CheckpointCoordinator(jid, 1, 200000L,
					new ExecutionVertex[] { stateful1, stateful2, stateful3, stateless1, stateless2 },
					new ExecutionVertex[] { stateful1, stateful2, stateful3, stateless1, stateless2 },
					new ExecutionVertex[0], cl);

			// create ourselves a checkpoint with state
			final long timestamp = 34623786L;
			coord.triggerCheckpoint(timestamp);

			PendingCheckpoint pending = coord.getPendingCheckpoints().values().iterator().next();
			final long checkpointId = pending.getCheckpointId();

			// the difference to the test "testSetState" is that one stateful subtask does not report state
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec1.getAttemptId(), checkpointId, serializedState));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec2.getAttemptId(), checkpointId));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statefulExec3.getAttemptId(), checkpointId, serializedState));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statelessExec1.getAttemptId(), checkpointId));
			coord.receiveAcknowledgeMessage(new AcknowledgeCheckpoint(jid, statelessExec2.getAttemptId(), checkpointId));

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
			CheckpointCoordinator coord = new CheckpointCoordinator(new JobID(), 1, 200000L,
					new ExecutionVertex[] { mock(ExecutionVertex.class) },
					new ExecutionVertex[] { mock(ExecutionVertex.class) },
					new ExecutionVertex[0], cl);

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
		Execution mock = mock(Execution.class);
		when(mock.getAttemptId()).thenReturn(new ExecutionAttemptID());
		when(mock.getState()).thenReturn(ExecutionState.CREATED);
		return mock;
	}
	
	private ExecutionVertex mockExecutionVertex(Execution execution, JobVertexID vertexId, int subtask) {
		ExecutionVertex mock = mock(ExecutionVertex.class);
		when(mock.getJobvertexId()).thenReturn(vertexId);
		when(mock.getParallelSubtaskIndex()).thenReturn(subtask);
		when(mock.getCurrentExecutionAttempt()).thenReturn(execution);
		return mock;
	}
	
	private ExecutionJobVertex mockExecutionJobVertex(JobVertexID id, ExecutionVertex[] vertices) {
		ExecutionJobVertex vertex = mock(ExecutionJobVertex.class);
		when(vertex.getParallelism()).thenReturn(vertices.length);
		when(vertex.getJobVertexId()).thenReturn(id);
		when(vertex.getTaskVertices()).thenReturn(vertices);
		return vertex;
	}
}
