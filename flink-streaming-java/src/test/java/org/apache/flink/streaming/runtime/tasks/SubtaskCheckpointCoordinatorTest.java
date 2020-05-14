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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.api.writer.NonRecordWriter;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.streaming.runtime.tasks.StreamTaskTest.NoOpStreamTask;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;

import org.junit.Test;

import static org.apache.flink.runtime.checkpoint.CheckpointType.SAVEPOINT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link SubtaskCheckpointCoordinator}.
 */
public class SubtaskCheckpointCoordinatorTest {

	@Test
	public void testNotifyCheckpointComplete() throws Exception {
		TestTaskStateManager stateManager = new TestTaskStateManager();
		MockEnvironment mockEnvironment = MockEnvironment.builder().setTaskStateManager(stateManager).build();
		SubtaskCheckpointCoordinator subtaskCheckpointCoordinator = new MockSubtaskCheckpointCoordinatorBuilder()
			.setEnvironment(mockEnvironment)
			.build();

		final OperatorChain<?, ?> operatorChain = new OperatorChain<>(
			new MockStreamTaskBuilder(new DummyEnvironment()).build(),
			new NonRecordWriter<>());

		long checkpointId = 42L;
		{
			subtaskCheckpointCoordinator.notifyCheckpointComplete(checkpointId, operatorChain, () -> true);
			assertEquals(checkpointId, stateManager.getNotifiedCompletedCheckpointId());
		}

		long newCheckpointId = checkpointId + 1;
		{
			subtaskCheckpointCoordinator.notifyCheckpointComplete(newCheckpointId, operatorChain, () -> false);
			// even task is not running, state manager could still receive the notification.
			assertEquals(newCheckpointId, stateManager.getNotifiedCompletedCheckpointId());
		}
	}

	@Test
	public void testSkipChannelStateForSavepoints() throws Exception {
		SubtaskCheckpointCoordinator coordinator = new MockSubtaskCheckpointCoordinatorBuilder()
			.setUnalignedCheckpointEnabled(false)
			.setPrepareInputSnapshot((u1, u2) -> {
				fail("should not prepare input snapshot for savepoint");
				return null;
			}).build();

		coordinator.checkpointState(
			new CheckpointMetaData(0, 0),
			new CheckpointOptions(SAVEPOINT, CheckpointStorageLocationReference.getDefault()),
			new CheckpointMetrics(),
			new OperatorChain<>(new NoOpStreamTask<>(new DummyEnvironment()), new NonRecordWriter<>()),
			() -> false);
	}
}
