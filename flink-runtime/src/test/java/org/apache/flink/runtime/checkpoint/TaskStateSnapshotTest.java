/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Random;

import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewInputChannelStateHandle;
import static org.apache.flink.runtime.checkpoint.StateHandleDummyUtil.createNewResultSubpartitionStateHandle;
import static org.apache.flink.runtime.checkpoint.StateObjectCollection.singleton;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TaskStateSnapshotTest extends TestLogger {

	@Test
	public void putGetSubtaskStateByOperatorID() {
		TaskStateSnapshot taskStateSnapshot = new TaskStateSnapshot();

		OperatorID operatorID_1 = new OperatorID();
		OperatorID operatorID_2 = new OperatorID();
		OperatorSubtaskState operatorSubtaskState_1 = new OperatorSubtaskState();
		OperatorSubtaskState operatorSubtaskState_2 = new OperatorSubtaskState();
		OperatorSubtaskState operatorSubtaskState_1_replace = new OperatorSubtaskState();

		Assert.assertNull(taskStateSnapshot.getSubtaskStateByOperatorID(operatorID_1));
		Assert.assertNull(taskStateSnapshot.getSubtaskStateByOperatorID(operatorID_2));
		taskStateSnapshot.putSubtaskStateByOperatorID(operatorID_1, operatorSubtaskState_1);
		taskStateSnapshot.putSubtaskStateByOperatorID(operatorID_2, operatorSubtaskState_2);
		Assert.assertEquals(operatorSubtaskState_1, taskStateSnapshot.getSubtaskStateByOperatorID(operatorID_1));
		Assert.assertEquals(operatorSubtaskState_2, taskStateSnapshot.getSubtaskStateByOperatorID(operatorID_2));
		Assert.assertEquals(operatorSubtaskState_1, taskStateSnapshot.putSubtaskStateByOperatorID(operatorID_1, operatorSubtaskState_1_replace));
		Assert.assertEquals(operatorSubtaskState_1_replace, taskStateSnapshot.getSubtaskStateByOperatorID(operatorID_1));
	}

	@Test
	public void hasState() {
		Random random = new Random(0x42);
		TaskStateSnapshot taskStateSnapshot = new TaskStateSnapshot();
		Assert.assertFalse(taskStateSnapshot.hasState());

		OperatorSubtaskState emptyOperatorSubtaskState = new OperatorSubtaskState();
		Assert.assertFalse(emptyOperatorSubtaskState.hasState());
		taskStateSnapshot.putSubtaskStateByOperatorID(new OperatorID(), emptyOperatorSubtaskState);
		Assert.assertFalse(taskStateSnapshot.hasState());

		OperatorStateHandle stateHandle = StateHandleDummyUtil.createNewOperatorStateHandle(2, random);
		OperatorSubtaskState nonEmptyOperatorSubtaskState = new OperatorSubtaskState(
			stateHandle,
			null,
			null,
			null,
			null,
			null
		);

		Assert.assertTrue(nonEmptyOperatorSubtaskState.hasState());
		taskStateSnapshot.putSubtaskStateByOperatorID(new OperatorID(), nonEmptyOperatorSubtaskState);
		Assert.assertTrue(taskStateSnapshot.hasState());
	}

	@Test
	public void discardState() throws Exception {
		TaskStateSnapshot taskStateSnapshot = new TaskStateSnapshot();
		OperatorID operatorID_1 = new OperatorID();
		OperatorID operatorID_2 = new OperatorID();

		OperatorSubtaskState operatorSubtaskState_1 = mock(OperatorSubtaskState.class);
		OperatorSubtaskState operatorSubtaskState_2 = mock(OperatorSubtaskState.class);

		taskStateSnapshot.putSubtaskStateByOperatorID(operatorID_1, operatorSubtaskState_1);
		taskStateSnapshot.putSubtaskStateByOperatorID(operatorID_2, operatorSubtaskState_2);

		taskStateSnapshot.discardState();
		verify(operatorSubtaskState_1).discardState();
		verify(operatorSubtaskState_2).discardState();
	}

	@Test
	public void getStateSize() {
		Random random = new Random(0x42);
		TaskStateSnapshot taskStateSnapshot = new TaskStateSnapshot();
		Assert.assertEquals(0, taskStateSnapshot.getStateSize());

		OperatorSubtaskState emptyOperatorSubtaskState = new OperatorSubtaskState();
		Assert.assertFalse(emptyOperatorSubtaskState.hasState());
		taskStateSnapshot.putSubtaskStateByOperatorID(new OperatorID(), emptyOperatorSubtaskState);
		Assert.assertEquals(0, taskStateSnapshot.getStateSize());


		OperatorStateHandle stateHandle_1 = StateHandleDummyUtil.createNewOperatorStateHandle(2, random);
		OperatorSubtaskState nonEmptyOperatorSubtaskState_1 = new OperatorSubtaskState(
			stateHandle_1,
			null,
			null,
			null,
			null,
			null
		);

		OperatorStateHandle stateHandle_2 = StateHandleDummyUtil.createNewOperatorStateHandle(2, random);
		OperatorSubtaskState nonEmptyOperatorSubtaskState_2 = new OperatorSubtaskState(
			null,
			stateHandle_2,
			null,
			null,
			null,
			null
		);

		taskStateSnapshot.putSubtaskStateByOperatorID(new OperatorID(), nonEmptyOperatorSubtaskState_1);
		taskStateSnapshot.putSubtaskStateByOperatorID(new OperatorID(), nonEmptyOperatorSubtaskState_2);

		long totalSize = stateHandle_1.getStateSize() + stateHandle_2.getStateSize();
		Assert.assertEquals(totalSize, taskStateSnapshot.getStateSize());
	}

	@Test
	public void testSizeIncludesChannelState() {
		final Random random = new Random();
		InputChannelStateHandle inputChannelStateHandle = createNewInputChannelStateHandle(10, random);
		ResultSubpartitionStateHandle resultSubpartitionStateHandle = createNewResultSubpartitionStateHandle(10, random);
		final TaskStateSnapshot taskStateSnapshot = new TaskStateSnapshot(Collections.singletonMap(
			new OperatorID(),
			new OperatorSubtaskState(
				StateObjectCollection.empty(),
				StateObjectCollection.empty(),
				StateObjectCollection.empty(),
				StateObjectCollection.empty(),
				singleton(inputChannelStateHandle),
				singleton(resultSubpartitionStateHandle))));
		Assert.assertEquals(inputChannelStateHandle.getStateSize() + resultSubpartitionStateHandle.getStateSize(), taskStateSnapshot.getStateSize());
		Assert.assertTrue(taskStateSnapshot.hasState());
	}
}
