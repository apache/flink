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
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CompletedCheckpointTest {

	/**
	 * Tests that the `deleteStateWhenDisposed` flag is correctly forwarded.
	 */
	@Test
	public void testDiscard() throws Exception {
		TaskState state = mock(TaskState.class);
		Map<JobVertexID, TaskState> taskStates = new HashMap<>();
		taskStates.put(new JobVertexID(), state);

		// Verify discard call is forwarded to state
		CompletedCheckpoint checkpoint = new CompletedCheckpoint(new JobID(), 0, 0, 1, taskStates, true);
		checkpoint.discard(ClassLoader.getSystemClassLoader());
		verify(state, times(1)).discard(Matchers.any(ClassLoader.class));

		Mockito.reset(state);

		// Verify discard call is not forwarded to state
		checkpoint = new CompletedCheckpoint(new JobID(), 0, 0, 1, taskStates, false);
		checkpoint.discard(ClassLoader.getSystemClassLoader());
		verify(state, times(0)).discard(Matchers.any(ClassLoader.class));
	}
}
