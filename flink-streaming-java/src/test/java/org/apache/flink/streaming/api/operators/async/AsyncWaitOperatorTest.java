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
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.TaskStateHandles;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.StreamMockEnvironment;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
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
 *     <li>AsyncWaitOperator in operator chain</li>
 *     <li>Snapshot state and restore state</li>
 * </ul>
 */
public class AsyncWaitOperatorTest {

	// hold sink result
	private static Queue<Object> sinkResult;

	private static class MyAsyncFunction extends RichAsyncFunction<Integer, Integer> {
		final int SLEEP_FACTOR = 100;
		final int THREAD_POOL_SIZE = 10;

		transient static ExecutorService executorService;
		static int counter = 0;

		static Random random = new Random();

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			synchronized (MyAsyncFunction.class) {
				if (counter == 0) {
					executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
				}

				++counter;
			}
		}

		@Override
		public void close() throws Exception {
			super.close();

			synchronized (MyAsyncFunction.class) {
				--counter;

				if (counter == 0) {
					executorService.shutdown();
					executorService.awaitTermination(SLEEP_FACTOR * THREAD_POOL_SIZE, TimeUnit.MILLISECONDS);
				}
			}
		}

		@Override
		public void asyncInvoke(final Integer input, final AsyncCollector<Integer> collector) throws Exception {
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
						// do nothing
					}
				}
			});
		}
	}

	/**
	 * A special {@link org.apache.flink.streaming.api.functions.async.AsyncFunction} without issuing
	 * {@link AsyncCollector#collect} until the latch counts to zero.
	 * This function is used in the testStateSnapshotAndRestore, ensuring
	 * that {@link org.apache.flink.streaming.api.functions.async.buffer.StreamElementEntry} can stay
	 * in the {@link org.apache.flink.streaming.api.functions.async.buffer.AsyncCollectorBuffer} to be
	 * snapshotted while checkpointing.
	 */
	private static class LazyAsyncFunction extends MyAsyncFunction {
		private static CountDownLatch latch;

		public LazyAsyncFunction() {
			latch = new CountDownLatch(1);
		}

		@Override
		public void asyncInvoke(final Integer input, final AsyncCollector<Integer> collector) throws Exception {
			this.executorService.submit(new Runnable() {
				@Override
				public void run() {
					try {
						latch.await();
					}
					catch (InterruptedException e) {
						// do nothing
					}

					collector.collect(Collections.singletonList(input));
				}
			});
		}

		public static void countDown() {
			latch.countDown();
		}
	}

	/**
	 * A {@link Comparator} to compare {@link StreamRecord} while sorting them.
	 */
	private class StreamRecordComparator implements Comparator<Object> {
		@Override
		public int compare(Object o1, Object o2) {
			if (o1 instanceof Watermark || o2 instanceof Watermark) {
				return 0;
			} else {
				StreamRecord<Integer> sr0 = (StreamRecord<Integer>) o1;
				StreamRecord<Integer> sr1 = (StreamRecord<Integer>) o2;

				if (sr0.getTimestamp() != sr1.getTimestamp()) {
					return (int) (sr0.getTimestamp() - sr1.getTimestamp());
				}

				int comparison = sr0.getValue().compareTo(sr1.getValue());
				if (comparison != 0) {
					return comparison;
				} else {
					return sr0.getValue() - sr1.getValue();
				}
			}
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
		final AsyncWaitOperator<Integer, Integer> operator = new AsyncWaitOperator<>(new MyAsyncFunction(), 2, mode);

		final OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
				new OneInputStreamOperatorTestHarness<>(operator, IntSerializer.INSTANCE);

		final long initialTime = 0L;
		final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		synchronized (testHarness.getCheckpointLock()) {
			testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
			testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
			testHarness.processWatermark(new Watermark(initialTime + 2));
			testHarness.processElement(new StreamRecord<>(3, initialTime + 3));
		}

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
			Object[] jobOutputQueue = testHarness.getOutput().toArray();

			Assert.assertEquals("Watermark should be at index 2", new Watermark(initialTime + 2), jobOutputQueue[2]);
			Assert.assertEquals("StreamRecord 3 should be at the end", new StreamRecord<>(6, initialTime + 3), jobOutputQueue[3]);

			TestHarnessUtil.assertOutputEqualsSorted(
					"Output for StreamRecords does not match",
					expectedOutput,
					testHarness.getOutput(),
					new StreamRecordComparator());
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

		final AsyncWaitOperator<Integer, Integer> operator = new AsyncWaitOperator<>(new MyAsyncFunction(), 6, mode);

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
					new StreamRecordComparator());
		}
	}

	private JobVertex createChainedVertex(boolean withLazyFunction) {
		StreamExecutionEnvironment chainEnv = StreamExecutionEnvironment.getExecutionEnvironment();

		// the input is only used to construct a chained operator, and they will not be used in the real tests.
		DataStream<Integer> input = chainEnv.fromElements(1, 2, 3);

		if (withLazyFunction) {
			input = AsyncDataStream.orderedWait(input, new LazyAsyncFunction(), 6);
		}
		else {
			input = AsyncDataStream.orderedWait(input, new MyAsyncFunction(), 6);
		}

		// the map function is designed to chain after async function. we place an Integer object in it and
		// it is initialized in the open() method.
		// it is used to verify that operators in the operator chain should be opened from the tail to the head,
		// so the result from AsyncWaitOperator can pass down successfully and correctly.
		// if not, the test can not be passed.
		input = input.map(new RichMapFunction<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			private Integer initialValue = null;

			@Override
			public void open(Configuration parameters) throws Exception {
				initialValue = 1;
			}

			@Override
			public Integer map(Integer value) throws Exception {
				return initialValue + value;
			}
		});

		input = AsyncDataStream.unorderedWait(input, new MyAsyncFunction(), 3);

		input.addSink(new SinkFunction<Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void invoke(Integer value) throws Exception {
				sinkResult.add(value);
			}
		});

		// be build our own OperatorChain
		final JobGraph jobGraph = chainEnv.getStreamGraph().getJobGraph();

		Assert.assertTrue(jobGraph.getVerticesSortedTopologicallyFromSources().size() == 2);

		return jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
	}

	/**
	 * Get the {@link SubtaskState} for the operator chain. The state will keep several inputs.
	 *
	 * @return A {@link SubtaskState}
	 * @throws Exception
     */
	private SubtaskState createTaskState() throws Exception {
		sinkResult = new ConcurrentLinkedDeque<>();

		final OneInputStreamTask<Integer, Integer> task = new OneInputStreamTask<>();
		final OneInputStreamTaskTestHarness<Integer, Integer> testHarness =
				new OneInputStreamTaskTestHarness<>(task, 1, 1, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

		JobVertex chainedVertex = createChainedVertex(true);

		testHarness.taskConfig = chainedVertex.getConfiguration();

		final AcknowledgeStreamMockEnvironment env = new AcknowledgeStreamMockEnvironment(
				testHarness.jobConfig,
				testHarness.taskConfig,
				testHarness.getExecutionConfig(),
				testHarness.memorySize,
				new MockInputSplitProvider(),
				testHarness.bufferSize);

		final StreamConfig streamConfig = testHarness.getStreamConfig();
		final StreamConfig operatorChainStreamConfig = new StreamConfig(chainedVertex.getConfiguration());
		final AsyncWaitOperator<Integer, Integer> headOperator =
				operatorChainStreamConfig.getStreamOperator(Thread.currentThread().getContextClassLoader());
		streamConfig.setStreamOperator(headOperator);

		testHarness.invoke(env);
		testHarness.waitForTaskRunning();

		testHarness.processElement(new StreamRecord<>(1));
		testHarness.processElement(new StreamRecord<>(2));
		testHarness.processElement(new StreamRecord<>(3));
		testHarness.processElement(new StreamRecord<>(4));

		testHarness.waitForInputProcessing();

		final CheckpointMetaData checkpointMetaData = new CheckpointMetaData(1L, 1L);

		task.triggerCheckpoint(checkpointMetaData);

		env.getCheckpointLatch().await();

		assertEquals(1L, env.getCheckpointId());

		LazyAsyncFunction.countDown();

		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		return env.getCheckpointStateHandles();
	}

	@Test
	public void testOperatorChain() throws Exception {

		JobVertex chainedVertex = createChainedVertex(false);

		final OneInputStreamTask<Integer, Integer> task = new OneInputStreamTask<>();
		final OneInputStreamTaskTestHarness<Integer, Integer> testHarness =
				new OneInputStreamTaskTestHarness<>(task, 1, 1, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

		task.setInitialState(new TaskStateHandles(createTaskState()));

		sinkResult = new ConcurrentLinkedDeque<>();

		testHarness.taskConfig = chainedVertex.getConfiguration();

		final AcknowledgeStreamMockEnvironment env = new AcknowledgeStreamMockEnvironment(
				testHarness.jobConfig,
				testHarness.taskConfig,
				testHarness.getExecutionConfig(),
				testHarness.memorySize,
				new MockInputSplitProvider(),
				testHarness.bufferSize);

		final StreamConfig streamConfig = testHarness.getStreamConfig();
		final StreamConfig operatorChainStreamConfig = new StreamConfig(chainedVertex.getConfiguration());
		final AsyncWaitOperator<Integer, Integer> headOperator =
				operatorChainStreamConfig.getStreamOperator(Thread.currentThread().getContextClassLoader());
		streamConfig.setStreamOperator(headOperator);

		testHarness.invoke(env);
		testHarness.waitForTaskRunning();

		testHarness.processElement(new StreamRecord<>(5));
		testHarness.processElement(new StreamRecord<>(6));
		testHarness.processElement(new StreamRecord<>(7));
		testHarness.processElement(new StreamRecord<>(8));
		testHarness.processElement(new StreamRecord<>(9));

		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		expectedOutput.add(6);
		expectedOutput.add(10);
		expectedOutput.add(14);
		expectedOutput.add(18);
		expectedOutput.add(22);
		expectedOutput.add(26);
		expectedOutput.add(30);
		expectedOutput.add(34);
		expectedOutput.add(38);

		TestHarnessUtil.assertOutputEqualsSorted(
				"Test for chained operator with AsyncWaitOperator failed",
				expectedOutput,
				sinkResult,
				new Comparator<Object>() {
					@Override
					public int compare(Object o1, Object o2) {
						return (Integer)o1 - (Integer)o2;
					}
				});
	}

	@Test
	public void testStateSnapshotAndRestore() throws Exception {
		final OneInputStreamTask<Integer, Integer> task = new OneInputStreamTask<>();
		final OneInputStreamTaskTestHarness<Integer, Integer> testHarness =
				new OneInputStreamTaskTestHarness<>(task, 1, 1, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

		AsyncWaitOperator<Integer, Integer> operator =
				new AsyncWaitOperator<>(new LazyAsyncFunction(), 6, AsyncDataStream.OutputMode.ORDERED);

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

		task.triggerCheckpoint(checkpointMetaData);

		env.getCheckpointLatch().await();

		assertEquals(checkpointId, env.getCheckpointId());

		LazyAsyncFunction.countDown();

		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		// set the operator state from previous attempt into the restored one
		final OneInputStreamTask<Integer, Integer> restoredTask = new OneInputStreamTask<>();
		restoredTask.setInitialState(new TaskStateHandles(env.getCheckpointStateHandles()));

		final OneInputStreamTaskTestHarness<Integer, Integer> restoredTaskHarness =
				new OneInputStreamTaskTestHarness<>(restoredTask, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

		AsyncWaitOperator<Integer, Integer> restoredOperator =
				new AsyncWaitOperator<>(new MyAsyncFunction(), 6, AsyncDataStream.OutputMode.ORDERED);

		restoredTaskHarness.getStreamConfig().setStreamOperator(restoredOperator);

		restoredTaskHarness.invoke();
		restoredTaskHarness.waitForTaskRunning();

		restoredTaskHarness.processElement(new StreamRecord<>(5, initialTime + 5));
		restoredTaskHarness.processElement(new StreamRecord<>(6, initialTime + 6));
		restoredTaskHarness.processElement(new StreamRecord<>(7, initialTime + 7));

		// trigger the checkpoint while processing stream elements
		restoredTask.triggerCheckpoint(new CheckpointMetaData(checkpointId, checkpointTimestamp));

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

		// remove CheckpointBarrier which is not expected
		Iterator<Object> iterator = restoredTaskHarness.getOutput().iterator();
		while (iterator.hasNext()) {
			if (iterator.next() instanceof CheckpointBarrier) {
				iterator.remove();
			}
		}

		TestHarnessUtil.assertOutputEquals(
				"StateAndRestored Test Output was not correct.",
				expectedOutput,
				restoredTaskHarness.getOutput());
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
