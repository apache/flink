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


import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TaskStateTest {

	@Test
	public void testNoOverlapKeyRange(){
		JobVertexID jobVertexId = new JobVertexID();
		TaskState taskState = new TaskState(jobVertexId,3,128,3);

		SubtaskState subTaskState0 = mock(SubtaskState.class);
		/* KeyGroupStateHandel0 [0,9] */
		KeyGroupsStateHandle keyGroupsStateHandle0 = mock(KeyGroupsStateHandle.class);
		KeyGroupRange keyGroupRange0 = KeyGroupRange.of(0,9);
		when(keyGroupsStateHandle0.getKeyGroupRange()).thenReturn(keyGroupRange0);
		when(subTaskState0.getManagedKeyedState()).thenReturn(keyGroupsStateHandle0);

		SubtaskState subTaskState1 = mock(SubtaskState.class);
		/* KeyGroupStateHandel1 [10,19] */
		KeyGroupsStateHandle keyGroupsStateHandle1 = mock(KeyGroupsStateHandle.class);
		KeyGroupRange keyGroupRange1 = KeyGroupRange.of(10,19);
		when(keyGroupsStateHandle1.getKeyGroupRange()).thenReturn(keyGroupRange1);
		when(subTaskState1.getManagedKeyedState()).thenReturn(keyGroupsStateHandle1);



		taskState.putState(0, subTaskState0);
		taskState.putState(1, subTaskState1);

		SubtaskState subTaskState2 = mock(SubtaskState.class);
		/* KeyGroupStateHandel1 [20,29] */
		KeyGroupsStateHandle keyGroupsStateHandle2 = mock(KeyGroupsStateHandle.class);
		KeyGroupRange keyGroupRange2 = KeyGroupRange.of(20,29);
		when(keyGroupsStateHandle2.getKeyGroupRange()).thenReturn(keyGroupRange2);
		when(subTaskState2.getManagedKeyedState()).thenReturn(keyGroupsStateHandle2);

		taskState.putState(2, subTaskState2);

		assertEquals(3, taskState.getNumberCollectedStates());

	}

	@Test
	public void testOverlapKeyRange(){
		JobVertexID jobVertexId = new JobVertexID();
		TaskState taskState = new TaskState(jobVertexId,3,128,3);

		SubtaskState subTaskState0 = mock(SubtaskState.class);
		/* KeyGroupStateHandel0 [0,9] */
		KeyGroupsStateHandle keyGroupsStateHandle0 = mock(KeyGroupsStateHandle.class);
		KeyGroupRange keyGroupRange0 = KeyGroupRange.of(0,9);
		when(keyGroupsStateHandle0.getKeyGroupRange()).thenReturn(keyGroupRange0);
		when(subTaskState0.getManagedKeyedState()).thenReturn(keyGroupsStateHandle0);

		SubtaskState subTaskState1 = mock(SubtaskState.class);
		/* KeyGroupStateHandel1 [10,19] */
		KeyGroupsStateHandle keyGroupsStateHandle1 = mock(KeyGroupsStateHandle.class);
		KeyGroupRange keyGroupRange1 = KeyGroupRange.of(10,19);
		when(keyGroupsStateHandle1.getKeyGroupRange()).thenReturn(keyGroupRange1);
		when(subTaskState1.getManagedKeyedState()).thenReturn(keyGroupsStateHandle1);



		taskState.putState(0, subTaskState0);
		taskState.putState(1, subTaskState1);


		SubtaskState subTaskState2 = mock(SubtaskState.class);
		/* KeyGroupStateHandel1 [15,29] */
		KeyGroupsStateHandle keyGroupsStateHandle2 = mock(KeyGroupsStateHandle.class);
		KeyGroupRange keyGroupRange2 = KeyGroupRange.of(15,29);
		when(keyGroupsStateHandle2.getKeyGroupRange()).thenReturn(keyGroupRange2);
		when(subTaskState2.getManagedKeyedState()).thenReturn(keyGroupsStateHandle2);

		try {
			taskState.putState(2, subTaskState2);
			fail("Did not throw expected Exception");
		}catch (Exception expected){
			assertTrue(expected.getMessage().contains("has overlap with"));
		}
	}
}
