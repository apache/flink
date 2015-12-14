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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.AsynchronousStateHandle;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.taskmanager.OneShotLatch;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for asynchronous checkpoints.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(ResultPartitionWriter.class)
@SuppressWarnings("serial")
public class StreamTaskAsyncCheckpointTest {

	/**
	 * This ensures that asynchronous state handles are actually materialized asynchonously.
	 *
	 * <p>We use latches to block at various stages and see if the code still continues through
	 * the parts that are not asynchronous.
	 * @throws Exception
	 */
	@Test
	public void testAsyncCheckpoints() throws Exception {
		final OneShotLatch delayCheckpointLatch = new OneShotLatch();
		final OneShotLatch ensureCheckpointLatch = new OneShotLatch();

		final OneInputStreamTask<String, String> task = new OneInputStreamTask<>();
		
		final OneInputStreamTaskTestHarness<String, String> testHarness = new OneInputStreamTaskTestHarness<>(task, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();
		
		streamConfig.setStreamOperator(new AsyncCheckpointOperator());

		StreamMockEnvironment mockEnv = new StreamMockEnvironment(
			testHarness.jobConfig,
			testHarness.taskConfig,
			testHarness.memorySize,
			new MockInputSplitProvider(),
			testHarness.bufferSize) {

			@Override
			public void acknowledgeCheckpoint(long checkpointId) {
				super.acknowledgeCheckpoint(checkpointId);
			}

			@Override
			public void acknowledgeCheckpoint(long checkpointId, StateHandle<?> state) {
				super.acknowledgeCheckpoint(checkpointId, state);

				// block on the latch, to verify that triggerCheckpoint returns below,
				// even though the async checkpoint would not finish
				try {
					delayCheckpointLatch.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

				assertTrue(state instanceof StreamTaskStateList);
				StreamTaskStateList stateList = (StreamTaskStateList) state;

				// should be only one state
				StreamTaskState taskState = stateList.getState(this.getUserClassLoader())[0];
				StateHandle<?> operatorState = taskState.getOperatorState();
				assertTrue("It must be a TestStateHandle", operatorState instanceof TestStateHandle);
				TestStateHandle testState = (TestStateHandle) operatorState;
				assertEquals(42, testState.checkpointId);
				assertEquals(17, testState.timestamp);

				// we now know that the checkpoint went through
				ensureCheckpointLatch.trigger();
			}
		};

		testHarness.invoke(mockEnv);

		// wait for the task to be running
		for (Field field: StreamTask.class.getDeclaredFields()) {
			if (field.getName().equals("isRunning")) {
				field.setAccessible(true);
				while (!field.getBoolean(task)) {
					Thread.sleep(10);
				}

			}
		}

		task.triggerCheckpoint(42, 17);

		// now we allow the checkpoint
		delayCheckpointLatch.trigger();

		// wait for the checkpoint to go through
		ensureCheckpointLatch.await();

		testHarness.endInput();
		testHarness.waitForTaskCompletion();
	}


	// ------------------------------------------------------------------------

	public static class AsyncCheckpointOperator
		extends AbstractStreamOperator<String>
		implements OneInputStreamOperator<String, String> {
		@Override
		public void processElement(StreamRecord<String> element) throws Exception {
			// we also don't care
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			// not interested
		}


		@Override
		public StreamTaskState snapshotOperatorState(final long checkpointId, final long timestamp) throws Exception {
			StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

			AsynchronousStateHandle<String> asyncState =
				new DataInputViewAsynchronousStateHandle(checkpointId, timestamp);

			taskState.setOperatorState(asyncState);

			return taskState;
		}

		@Override
		public void restoreState(StreamTaskState taskState, long recoveryTimestamp) throws Exception {
			super.restoreState(taskState, recoveryTimestamp);
		}
	}

	private static class DataInputViewAsynchronousStateHandle extends AsynchronousStateHandle<String> {

		private final long checkpointId;
		private final long timestamp;

		public DataInputViewAsynchronousStateHandle(long checkpointId, long timestamp) {
			this.checkpointId = checkpointId;
			this.timestamp = timestamp;
		}

		@Override
		public StateHandle<String> materialize() throws Exception {
			return new TestStateHandle(checkpointId, timestamp);
		}

		@Override
		public long getStateSize() {
			return 0;
		}
	}

	private static class TestStateHandle implements StateHandle<String> {

		public final long checkpointId;
		public final long timestamp;

		public TestStateHandle(long checkpointId, long timestamp) {
			this.checkpointId = checkpointId;
			this.timestamp = timestamp;
		}

		@Override
		public String getState(ClassLoader userCodeClassLoader) throws Exception {
			return null;
		}

		@Override
		public void discardState() throws Exception {
		}

		@Override
		public long getStateSize() {
			return 0;
		}
	}
	
	public static class DummyMapFunction<T> implements MapFunction<T, T> {
		@Override
		public T map(T value) { return value; }
	}
}
