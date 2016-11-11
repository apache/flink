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

package org.apache.flink.streaming.api.operators.async;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link AsyncWaitOperator}. These test that:
 *
 * <ul>
 *     <li>Process StreamRecords and Watermarks in ORDERED mode</li>
 *     <li>Process StreamRecords and Watermarks in UNORDERED mode</li>
 *     <li>Snapshot state and restore state</li>
 * </ul>
 */
public class AsyncWaitOperatorTest {

	public static class MyAsyncFunction extends RichAsyncFunction<Integer, Integer> {
		transient final int SLEEP_FACTOR = 100;
		transient final int THREAD_POOL_SIZE = 10;
		transient ExecutorService executorService;
		transient Random random;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
			random = new Random();
		}

		@Override
		public void close() throws Exception {
			super.close();
			executorService.shutdown();
			executorService.awaitTermination(SLEEP_FACTOR*THREAD_POOL_SIZE, TimeUnit.MILLISECONDS);
		}

		@Override
		public void asyncInvoke(final Integer input, final AsyncCollector<Integer, Integer> collector) throws Exception {
			this.executorService.submit(new Runnable() {
				@Override
				public void run() {
					// wait for while to simulate async operation here
					int sleep = (int) (random.nextFloat() * SLEEP_FACTOR);
					try {
						Thread.sleep(sleep);
						List<Integer> ret = new ArrayList<>();
						ret.add(input*2);
						collector.collect(ret);
					}
					catch (InterruptedException e) {
					}
				}
			});
		}
	}

	@Test
	public void testWaterMarkOrdered() throws Exception {
		testWithWatermark(AsyncDataStream.OutputMode.ORDERED);
	}

	@Test
	public void testWaterMarkUnordered() throws Exception {
		testWithWatermark(AsyncDataStream.OutputMode.UNORDERED);
	}

	private void testWithWatermark(AsyncDataStream.OutputMode mode) throws Exception {
		final AsyncWaitOperator<Integer, Integer> operator = new AsyncWaitOperator<>(new MyAsyncFunction());
		operator.setOutputMode(mode);
		operator.setBufferSize(6);
		operator.setInStreamElementSerializerForTest(new StreamElementSerializer<>(IntSerializer.INSTANCE));

		final OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
				new OneInputStreamOperatorTestHarness<>(operator);

		final long initialTime = 0L;
		final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
		testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
		testHarness.processWatermark(new Watermark(initialTime + 2));
		testHarness.processElement(new StreamRecord<>(3, initialTime + 3));

		// wait until all async collectors in the buffer have been emitted out.
		synchronized (testHarness.getCheckpointLock()) {
			testHarness.close();
		}

		expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
		expectedOutput.add(new StreamRecord<>(4, initialTime + 2));
		expectedOutput.add(new Watermark(initialTime + 2));
		expectedOutput.add(new StreamRecord<>(6, initialTime + 3));

		if (AsyncDataStream.OutputMode.ORDERED == mode) {
			TestHarnessUtil.assertOutputEquals("Output with watermark was not correct.", expectedOutput, testHarness.getOutput());
		}
		else {
			Assert.assertTrue(expectedOutput.size() == 4);
			Assert.assertTrue(expectedOutput.toArray(new StreamElement[4])[2].asWatermark().getTimestamp() == (initialTime+2));
		}
	}

	@Test
	public void testOrdered() throws Exception {
		testRun(AsyncDataStream.OutputMode.ORDERED);
	}

	@Test
	public void testUnordered() throws Exception {
		testRun(AsyncDataStream.OutputMode.UNORDERED);
	}

	private void testRun(AsyncDataStream.OutputMode mode) throws Exception {
		final OneInputStreamTask<Integer, Integer> task = new OneInputStreamTask<>();
		final OneInputStreamTaskTestHarness<Integer, Integer> testHarness =
				new OneInputStreamTaskTestHarness<>(task, 1, 1, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

		final AsyncWaitOperator<Integer, Integer> operator = new AsyncWaitOperator<>(new MyAsyncFunction());
		operator.setOutputMode(mode);
		operator.setBufferSize(6);

		final StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setStreamOperator(operator);

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		final long initialTime = 0L;
		final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();

		testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
		testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
		testHarness.processElement(new StreamRecord<>(3, initialTime + 3));
		testHarness.processElement(new StreamRecord<>(4, initialTime + 4));
		testHarness.processElement(new StreamRecord<>(5, initialTime + 5));
		testHarness.processElement(new StreamRecord<>(6, initialTime + 6));
		testHarness.processElement(new StreamRecord<>(7, initialTime + 7));
		testHarness.processElement(new StreamRecord<>(8, initialTime + 8));

		expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
		expectedOutput.add(new StreamRecord<>(4, initialTime + 2));
		expectedOutput.add(new StreamRecord<>(6, initialTime + 3));
		expectedOutput.add(new StreamRecord<>(8, initialTime + 4));
		expectedOutput.add(new StreamRecord<>(10, initialTime + 5));
		expectedOutput.add(new StreamRecord<>(12, initialTime + 6));
		expectedOutput.add(new StreamRecord<>(14, initialTime + 7));
		expectedOutput.add(new StreamRecord<>(16, initialTime + 8));

		testHarness.waitForInputProcessing();

		testHarness.endInput();

		testHarness.waitForTaskCompletion();

		if (mode == AsyncDataStream.OutputMode.ORDERED) {
			TestHarnessUtil.assertOutputEquals("ORDERED Output was not correct.", expectedOutput, testHarness.getOutput());
		}
		else {
			TestHarnessUtil.assertOutputEqualsSorted(
					"UNORDERED Output was not correct.",
					expectedOutput,
					testHarness.getOutput(),
					new Comparator<Object>() {
						@Override
						public int compare(Object o1, Object o2) {
							Integer val1 = ((StreamRecord<Integer>)o1).getValue();
							Integer val2 = ((StreamRecord<Integer>)o2).getValue();
							if (val1 > val2) {
								return 1;
							}
							else if (val1 == val2) {
								return 0;
							}
							else {
								return -1;
							}
						}
					});
		}
	}

	@Test
	public void testStateSnapshotAndRestore() throws Exception {
		final OneInputStreamTask<Integer, Integer> task = new OneInputStreamTask<>();
		final OneInputStreamTaskTestHarness<Integer, Integer> testHarness =
				new OneInputStreamTaskTestHarness<>(task, 1, 1, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

		final AsyncWaitOperator<Integer, Integer> operator = new AsyncWaitOperator<>(new MyAsyncFunction());
		operator.setOutputMode(AsyncDataStream.OutputMode.ORDERED);
		operator.setBufferSize(6);
		// disable starting emitter thread, so that all inputs will be kept in the buffer, which will be
		// snapshotted into operator state
		operator.setEmitFlag(false);

		final StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setStreamOperator(operator);

		final AcknowledgeStreamMockEnvironment env = new AcknowledgeStreamMockEnvironment(
				testHarness.jobConfig,
				testHarness.taskConfig,
				testHarness.getExecutionConfig(),
				testHarness.memorySize,
				new MockInputSplitProvider(),
				testHarness.bufferSize);

		testHarness.invoke(env);
		testHarness.waitForTaskRunning();

		final long initialTime = 0L;

		testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
		testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
		testHarness.processElement(new StreamRecord<>(3, initialTime + 3));
		testHarness.processElement(new StreamRecord<>(4, initialTime + 4));

		testHarness.waitForInputProcessing();

		final long checkpointId = 1L;
		final long checkpointTimestamp = 1L;

		final CheckpointMetaData checkpointMetaData = new CheckpointMetaData(checkpointId, checkpointTimestamp);

		while(!task.triggerCheckpoint(checkpointMetaData));

		env.getCheckpointLatch().await();

		assertEquals(checkpointId, env.getCheckpointId());

		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		env.getCheckpointStateHandles();

		// set the operator state from previous attempt into the restored one
		final OneInputStreamTask<Integer, Integer> restoredTask = new OneInputStreamTask<>();
		restoredTask.setInitialState(new TaskStateHandles(env.getCheckpointStateHandles()));

		final OneInputStreamTaskTestHarness<Integer, Integer> restoredTaskHarness =
				new OneInputStreamTaskTestHarness<>(restoredTask, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

		final AsyncWaitOperator<Integer, Integer> restoredOperator = new AsyncWaitOperator<>(new MyAsyncFunction());
		restoredOperator.setOutputMode(AsyncDataStream.OutputMode.ORDERED);
		restoredOperator.setBufferSize(6);

		restoredTaskHarness.getStreamConfig().setStreamOperator(restoredOperator);

		restoredTaskHarness.invoke();
		restoredTaskHarness.waitForTaskRunning();

		restoredTaskHarness.processElement(new StreamRecord<>(5, initialTime + 5));
		restoredTaskHarness.processElement(new StreamRecord<>(6, initialTime + 6));
		restoredTaskHarness.processElement(new StreamRecord<>(7, initialTime + 7));
		restoredTaskHarness.processElement(new StreamRecord<>(8, initialTime + 8));

		restoredTaskHarness.endInput();
		restoredTaskHarness.waitForTaskCompletion();

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
		expectedOutput.add(new StreamRecord<>(4, initialTime + 2));
		expectedOutput.add(new StreamRecord<>(6, initialTime + 3));
		expectedOutput.add(new StreamRecord<>(8, initialTime + 4));
		expectedOutput.add(new StreamRecord<>(10, initialTime + 5));
		expectedOutput.add(new StreamRecord<>(12, initialTime + 6));
		expectedOutput.add(new StreamRecord<>(14, initialTime + 7));
		expectedOutput.add(new StreamRecord<>(16, initialTime + 8));

		TestHarnessUtil.assertOutputEquals("StateAndRestored Test Output was not correct.", expectedOutput, restoredTaskHarness.getOutput());
	}

	private static class AcknowledgeStreamMockEnvironment extends StreamMockEnvironment {
		private volatile long checkpointId;
		private volatile SubtaskState checkpointStateHandles;

		private final OneShotLatch checkpointLatch = new OneShotLatch();

		public long getCheckpointId() {
			return checkpointId;
		}

		AcknowledgeStreamMockEnvironment(
				Configuration jobConfig, Configuration taskConfig,
				ExecutionConfig executionConfig, long memorySize,
				MockInputSplitProvider inputSplitProvider, int bufferSize) {
				super(jobConfig, taskConfig, executionConfig, memorySize, inputSplitProvider, bufferSize);
		}


		@Override
		public void acknowledgeCheckpoint(
				CheckpointMetaData checkpointMetaData,
				SubtaskState checkpointStateHandles) {

			this.checkpointId = checkpointMetaData.getCheckpointId();
			this.checkpointStateHandles = checkpointStateHandles;
			checkpointLatch.trigger();
		}

		public OneShotLatch getCheckpointLatch() {
			return checkpointLatch;
		}

		public SubtaskState getCheckpointStateHandles() {
			return checkpointStateHandles;
		}
	}
}
