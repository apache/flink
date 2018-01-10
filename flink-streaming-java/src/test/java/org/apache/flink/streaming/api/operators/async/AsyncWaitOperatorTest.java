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
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueue;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueueEntry;
import org.apache.flink.streaming.api.operators.async.queue.StreamRecordQueueEntry;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.AcknowledgeStreamMockEnvironment;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nonnull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
public class AsyncWaitOperatorTest extends TestLogger {

	private static final long TIMEOUT = 1000L;

	private static class MyAsyncFunction extends RichAsyncFunction<Integer, Integer> {
		private static final long serialVersionUID = 8522411971886428444L;

		private static final long TERMINATION_TIMEOUT = 5000L;
		private static final int THREAD_POOL_SIZE = 10;

		static ExecutorService executorService;
		static int counter = 0;

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

			freeExecutor();
		}

		private void freeExecutor() {
			synchronized (MyAsyncFunction.class) {
				--counter;

				if (counter == 0) {
					executorService.shutdown();

					try {
						if (!executorService.awaitTermination(TERMINATION_TIMEOUT, TimeUnit.MILLISECONDS)) {
							executorService.shutdownNow();
						}
					} catch (InterruptedException interrupted) {
						executorService.shutdownNow();

						Thread.currentThread().interrupt();
					}
				}
			}
		}

		@Override
		public void asyncInvoke(final Integer input, final ResultFuture<Integer> resultFuture) throws Exception {
			executorService.submit(new Runnable() {
				@Override
				public void run() {
					resultFuture.complete(Collections.singletonList(input * 2));
				}
			});
		}
	}

	/**
	 * A special {@link AsyncFunction} without issuing
	 * {@link ResultFuture#complete} until the latch counts to zero.
	 * This function is used in the testStateSnapshotAndRestore, ensuring
	 * that {@link StreamElementQueueEntry} can stay
	 * in the {@link StreamElementQueue} to be
	 * snapshotted while checkpointing.
	 */
	private static class LazyAsyncFunction extends MyAsyncFunction {
		private static final long serialVersionUID = 3537791752703154670L;

		private static CountDownLatch latch;

		public LazyAsyncFunction() {
			latch = new CountDownLatch(1);
		}

		@Override
		public void asyncInvoke(final Integer input, final ResultFuture<Integer> resultFuture) throws Exception {
			this.executorService.submit(new Runnable() {
				@Override
				public void run() {
					try {
						latch.await();
					}
					catch (InterruptedException e) {
						// do nothing
					}

					resultFuture.complete(Collections.singletonList(input));
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

	/**
	 * Test the AsyncWaitOperator with ordered mode and event time.
	 */
	@Test
	public void testEventTimeOrdered() throws Exception {
		testEventTime(AsyncDataStream.OutputMode.ORDERED);
	}

	/**
	 * Test the AsyncWaitOperator with unordered mode and event time.
	 */
	@Test
	public void testWaterMarkUnordered() throws Exception {
		testEventTime(AsyncDataStream.OutputMode.UNORDERED);
	}

	private void testEventTime(AsyncDataStream.OutputMode mode) throws Exception {
		final AsyncWaitOperator<Integer, Integer> operator = new AsyncWaitOperator<>(
			new MyAsyncFunction(),
			TIMEOUT,
			2,
			mode);

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

	/**
	 * Test the AsyncWaitOperator with ordered mode and processing time.
	 */
	@Test
	public void testProcessingTimeOrdered() throws Exception {
		testProcessingTime(AsyncDataStream.OutputMode.ORDERED);
	}

	/**
	 * Test the AsyncWaitOperator with unordered mode and processing time.
	 */
	@Test
	public void testProcessingUnordered() throws Exception {
		testProcessingTime(AsyncDataStream.OutputMode.UNORDERED);
	}

	private void testProcessingTime(AsyncDataStream.OutputMode mode) throws Exception {
		final AsyncWaitOperator<Integer, Integer> operator = new AsyncWaitOperator<>(
			new MyAsyncFunction(), TIMEOUT, 6, mode);

		final OneInputStreamOperatorTestHarness<Integer, Integer> testHarness = new OneInputStreamOperatorTestHarness<>(operator, IntSerializer.INSTANCE);

		final long initialTime = 0L;
		final Queue<Object> expectedOutput = new ArrayDeque<>();

		testHarness.open();

		synchronized (testHarness.getCheckpointLock()) {
			testHarness.processElement(new StreamRecord<>(1, initialTime + 1));
			testHarness.processElement(new StreamRecord<>(2, initialTime + 2));
			testHarness.processElement(new StreamRecord<>(3, initialTime + 3));
			testHarness.processElement(new StreamRecord<>(4, initialTime + 4));
			testHarness.processElement(new StreamRecord<>(5, initialTime + 5));
			testHarness.processElement(new StreamRecord<>(6, initialTime + 6));
			testHarness.processElement(new StreamRecord<>(7, initialTime + 7));
			testHarness.processElement(new StreamRecord<>(8, initialTime + 8));
		}

		expectedOutput.add(new StreamRecord<>(2, initialTime + 1));
		expectedOutput.add(new StreamRecord<>(4, initialTime + 2));
		expectedOutput.add(new StreamRecord<>(6, initialTime + 3));
		expectedOutput.add(new StreamRecord<>(8, initialTime + 4));
		expectedOutput.add(new StreamRecord<>(10, initialTime + 5));
		expectedOutput.add(new StreamRecord<>(12, initialTime + 6));
		expectedOutput.add(new StreamRecord<>(14, initialTime + 7));
		expectedOutput.add(new StreamRecord<>(16, initialTime + 8));

		synchronized (testHarness.getCheckpointLock()) {
			testHarness.close();
		}

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

	/**
	 *	Tests that the AsyncWaitOperator works together with chaining.
	 */
	@Test
	public void testOperatorChainWithProcessingTime() throws Exception {

		JobVertex chainedVertex = createChainedVertex(false);

		final OneInputStreamTask<Integer, Integer> task = new OneInputStreamTask<>();
		final OneInputStreamTaskTestHarness<Integer, Integer> testHarness =
				new OneInputStreamTaskTestHarness<>(task, 1, 1, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
		testHarness.setupOutputForSingletonOperatorChain();

		testHarness.taskConfig = chainedVertex.getConfiguration();

		final StreamConfig streamConfig = testHarness.getStreamConfig();
		final StreamConfig operatorChainStreamConfig = new StreamConfig(chainedVertex.getConfiguration());
		final AsyncWaitOperator<Integer, Integer> headOperator =
				operatorChainStreamConfig.getStreamOperator(AsyncWaitOperatorTest.class.getClassLoader());
		streamConfig.setStreamOperator(headOperator);

		testHarness.invoke();
		testHarness.waitForTaskRunning();

		long initialTimestamp = 0L;

		testHarness.processElement(new StreamRecord<>(5, initialTimestamp));
		testHarness.processElement(new StreamRecord<>(6, initialTimestamp + 1L));
		testHarness.processElement(new StreamRecord<>(7, initialTimestamp + 2L));
		testHarness.processElement(new StreamRecord<>(8, initialTimestamp + 3L));
		testHarness.processElement(new StreamRecord<>(9, initialTimestamp + 4L));

		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		expectedOutput.add(new StreamRecord<>(22, initialTimestamp));
		expectedOutput.add(new StreamRecord<>(26, initialTimestamp + 1L));
		expectedOutput.add(new StreamRecord<>(30, initialTimestamp + 2L));
		expectedOutput.add(new StreamRecord<>(34, initialTimestamp + 3L));
		expectedOutput.add(new StreamRecord<>(38, initialTimestamp + 4L));

		TestHarnessUtil.assertOutputEqualsSorted(
				"Test for chained operator with AsyncWaitOperator failed",
				expectedOutput,
				testHarness.getOutput(),
				new StreamRecordComparator());
	}

	private JobVertex createChainedVertex(boolean withLazyFunction) {
		StreamExecutionEnvironment chainEnv = StreamExecutionEnvironment.getExecutionEnvironment();

		// the input is only used to construct a chained operator, and they will not be used in the real tests.
		DataStream<Integer> input = chainEnv.fromElements(1, 2, 3);

		if (withLazyFunction) {
			input = AsyncDataStream.orderedWait(
				input,
				new LazyAsyncFunction(),
				TIMEOUT,
				TimeUnit.MILLISECONDS,
				6);
		}
		else {
			input = AsyncDataStream.orderedWait(
				input,
				new MyAsyncFunction(),
				TIMEOUT,
				TimeUnit.MILLISECONDS,
				6);
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

		input = AsyncDataStream.unorderedWait(
			input,
			new MyAsyncFunction(),
			TIMEOUT,
			TimeUnit.MILLISECONDS,
			3);

		input.map(new MapFunction<Integer, Integer>() {
			private static final long serialVersionUID = 5162085254238405527L;

			@Override
			public Integer map(Integer value) throws Exception {
				return value;
			}
		}).startNewChain().addSink(new DiscardingSink<Integer>());

		// be build our own OperatorChain
		final JobGraph jobGraph = chainEnv.getStreamGraph().getJobGraph();

		Assert.assertTrue(jobGraph.getVerticesSortedTopologicallyFromSources().size() == 3);

		return jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
	}

	@Test
	public void testStateSnapshotAndRestore() throws Exception {
		final OneInputStreamTask<Integer, Integer> task = new OneInputStreamTask<>();
		final OneInputStreamTaskTestHarness<Integer, Integer> testHarness =
				new OneInputStreamTaskTestHarness<>(task, 1, 1, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
		testHarness.setupOutputForSingletonOperatorChain();

		AsyncWaitOperator<Integer, Integer> operator = new AsyncWaitOperator<>(
			new LazyAsyncFunction(),
			TIMEOUT,
			3,
			AsyncDataStream.OutputMode.ORDERED);

		final StreamConfig streamConfig = testHarness.getStreamConfig();
		OperatorID operatorID = new OperatorID(42L, 4711L);
		streamConfig.setStreamOperator(operator);
		streamConfig.setOperatorID(operatorID);

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

		task.triggerCheckpoint(checkpointMetaData, CheckpointOptions.forCheckpoint());

		env.getCheckpointLatch().await();

		assertEquals(checkpointId, env.getCheckpointId());

		LazyAsyncFunction.countDown();

		testHarness.endInput();
		testHarness.waitForTaskCompletion();

		// set the operator state from previous attempt into the restored one
		final OneInputStreamTask<Integer, Integer> restoredTask = new OneInputStreamTask<>();
		TaskStateSnapshot subtaskStates = env.getCheckpointStateHandles();
		restoredTask.setInitialState(subtaskStates);

		final OneInputStreamTaskTestHarness<Integer, Integer> restoredTaskHarness =
				new OneInputStreamTaskTestHarness<>(restoredTask, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
		restoredTaskHarness.setupOutputForSingletonOperatorChain();

		AsyncWaitOperator<Integer, Integer> restoredOperator = new AsyncWaitOperator<>(
			new MyAsyncFunction(),
			TIMEOUT,
			6,
			AsyncDataStream.OutputMode.ORDERED);

		restoredTaskHarness.getStreamConfig().setStreamOperator(restoredOperator);
		restoredTaskHarness.getStreamConfig().setOperatorID(operatorID);

		restoredTaskHarness.invoke();
		restoredTaskHarness.waitForTaskRunning();

		restoredTaskHarness.processElement(new StreamRecord<>(5, initialTime + 5));
		restoredTaskHarness.processElement(new StreamRecord<>(6, initialTime + 6));
		restoredTaskHarness.processElement(new StreamRecord<>(7, initialTime + 7));

		// trigger the checkpoint while processing stream elements
		restoredTask.triggerCheckpoint(new CheckpointMetaData(checkpointId, checkpointTimestamp), CheckpointOptions.forCheckpoint());

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

	@Test
	public void testAsyncTimeout() throws Exception {
		final long timeout = 10L;

		final AsyncWaitOperator<Integer, Integer> operator = new AsyncWaitOperator<>(
			new LazyAsyncFunction(),
			timeout,
			2,
			AsyncDataStream.OutputMode.ORDERED);

		final Environment mockEnvironment = createMockEnvironment();

		final OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
			new OneInputStreamOperatorTestHarness<>(operator, IntSerializer.INSTANCE, mockEnvironment);

		final long initialTime = 0L;
		final ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		testHarness.setProcessingTime(initialTime);

		synchronized (testHarness.getCheckpointLock()) {
			testHarness.processElement(new StreamRecord<>(1, initialTime));
			testHarness.setProcessingTime(initialTime + 5L);
			testHarness.processElement(new StreamRecord<>(2, initialTime + 5L));
		}

		// trigger the timeout of the first stream record
		testHarness.setProcessingTime(initialTime + timeout + 1L);

		// allow the second async stream record to be processed
		LazyAsyncFunction.countDown();

		// wait until all async collectors in the buffer have been emitted out.
		synchronized (testHarness.getCheckpointLock()) {
			testHarness.close();
		}

		expectedOutput.add(new StreamRecord<>(2, initialTime + 5L));

		TestHarnessUtil.assertOutputEquals("Output with watermark was not correct.", expectedOutput, testHarness.getOutput());

		ArgumentCaptor<Throwable> argumentCaptor = ArgumentCaptor.forClass(Throwable.class);

		verify(mockEnvironment).failExternally(argumentCaptor.capture());

		Throwable failureCause = argumentCaptor.getValue();

		Assert.assertNotNull(failureCause.getCause());
		Assert.assertTrue(failureCause.getCause() instanceof ExecutionException);

		Assert.assertNotNull(failureCause.getCause().getCause());
		Assert.assertTrue(failureCause.getCause().getCause() instanceof TimeoutException);
	}

	@Nonnull
	private Environment createMockEnvironment() {
		final Environment mockEnvironment = mock(Environment.class);

		final Configuration taskConfiguration = new Configuration();
		final ExecutionConfig executionConfig = new ExecutionConfig();
		final TaskMetricGroup metricGroup = new UnregisteredTaskMetricsGroup();
		final TaskManagerRuntimeInfo taskManagerRuntimeInfo = new TestingTaskManagerRuntimeInfo();
		final TaskInfo taskInfo = new TaskInfo("foobarTask", 1, 0, 1, 1);

		when(mockEnvironment.getTaskConfiguration()).thenReturn(taskConfiguration);
		when(mockEnvironment.getExecutionConfig()).thenReturn(executionConfig);
		when(mockEnvironment.getMetricGroup()).thenReturn(metricGroup);
		when(mockEnvironment.getTaskManagerInfo()).thenReturn(taskManagerRuntimeInfo);
		when(mockEnvironment.getTaskInfo()).thenReturn(taskInfo);
		when(mockEnvironment.getUserClassLoader()).thenReturn(Thread.currentThread().getContextClassLoader());
		return mockEnvironment;
	}

	/**
	 * Test case for FLINK-5638: Tests that the async wait operator can be closed even if the
	 * emitter is currently waiting on the checkpoint lock (e.g. in the case of two chained async
	 * wait operators where the latter operator's queue is currently full).
	 *
	 * <p>Note that this test does not enforce the exact strict ordering because with the fix it is no
	 * longer possible. However, it provokes the described situation without the fix.
	 */
	@Test(timeout = 10000L)
	public void testClosingWithBlockedEmitter() throws Exception {
		final Object lock = new Object();

		ArgumentCaptor<Throwable> failureReason = ArgumentCaptor.forClass(Throwable.class);

		Environment environment = createMockEnvironment();
		doNothing().when(environment).failExternally(failureReason.capture());

		StreamTask<?, ?> containingTask = mock(StreamTask.class);
		when(containingTask.getEnvironment()).thenReturn(environment);
		when(containingTask.getCheckpointLock()).thenReturn(lock);
		when(containingTask.getProcessingTimeService()).thenReturn(new TestProcessingTimeService());

		StreamConfig streamConfig = mock(StreamConfig.class);
		doReturn(IntSerializer.INSTANCE).when(streamConfig).getTypeSerializerIn1(any(ClassLoader.class));

		final OneShotLatch closingLatch = new OneShotLatch();
		final OneShotLatch outputLatch = new OneShotLatch();

		Output<StreamRecord<Integer>> output = mock(Output.class);
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				assertTrue("Output should happen under the checkpoint lock.", Thread.currentThread().holdsLock(lock));

				outputLatch.trigger();

				// wait until we're in the closing method of the operator
				while (!closingLatch.isTriggered()) {
					lock.wait();
				}

				return null;
			}
		}).when(output).collect(any(StreamRecord.class));

		AsyncWaitOperator<Integer, Integer> operator = new TestAsyncWaitOperator<>(
			new MyAsyncFunction(),
			1000L,
			1,
			AsyncDataStream.OutputMode.ORDERED,
			closingLatch);

		operator.setup(
			containingTask,
			streamConfig,
			output);

		operator.open();

		synchronized (lock) {
			operator.processElement(new StreamRecord<>(42));
		}

		outputLatch.await();

		synchronized (lock) {
			operator.close();
		}

		// check that no concurrent exception has occurred
		try {
			verify(environment, never()).failExternally(any(Throwable.class));
		} catch (Error e) {
			// add the exception occurring in the emitter thread (root cause) as a suppressed
			// exception
			e.addSuppressed(failureReason.getValue());
			throw e;
		}
	}

	/**
	 * Testing async wait operator which introduces a latch to synchronize the execution with the
	 * emitter.
	 */
	private static final class TestAsyncWaitOperator<IN, OUT> extends AsyncWaitOperator<IN, OUT> {

		private static final long serialVersionUID = -8528791694746625560L;

		private final transient OneShotLatch closingLatch;

		public TestAsyncWaitOperator(
				AsyncFunction<IN, OUT> asyncFunction,
				long timeout,
				int capacity,
				AsyncDataStream.OutputMode outputMode,
				OneShotLatch closingLatch) {
			super(asyncFunction, timeout, capacity, outputMode);

			this.closingLatch = Preconditions.checkNotNull(closingLatch);
		}

		@Override
		public void close() throws Exception {
			closingLatch.trigger();
			checkpointingLock.notifyAll();
			super.close();
		}
	}

	/**
	 * FLINK-5652
	 * Tests that registered timers are properly canceled upon completion of a
	 * {@link StreamRecordQueueEntry} in order to avoid resource leaks because TriggerTasks hold
	 * a reference on the StreamRecordQueueEntry.
	 */
	@Test
	public void testTimeoutCleanup() throws Exception {
		final Object lock = new Object();

		final long timeout = 100000L;
		final long timestamp = 1L;

		Environment environment = createMockEnvironment();

		ScheduledFuture<?> scheduledFuture = mock(ScheduledFuture.class);

		ProcessingTimeService processingTimeService = mock(ProcessingTimeService.class);
		when(processingTimeService.getCurrentProcessingTime()).thenReturn(timestamp);
		doReturn(scheduledFuture).when(processingTimeService).registerTimer(anyLong(), any(ProcessingTimeCallback.class));

		StreamTask<?, ?> containingTask = mock(StreamTask.class);
		when(containingTask.getEnvironment()).thenReturn(environment);
		when(containingTask.getCheckpointLock()).thenReturn(lock);
		when(containingTask.getProcessingTimeService()).thenReturn(processingTimeService);

		StreamConfig streamConfig = mock(StreamConfig.class);
		doReturn(IntSerializer.INSTANCE).when(streamConfig).getTypeSerializerIn1(any(ClassLoader.class));

		Output<StreamRecord<Integer>> output = mock(Output.class);

		AsyncWaitOperator<Integer, Integer> operator = new AsyncWaitOperator<>(
			new AsyncFunction<Integer, Integer>() {
				private static final long serialVersionUID = -3718276118074877073L;

				@Override
				public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture) throws Exception {
					resultFuture.complete(Collections.singletonList(input));
				}
			},
			timeout,
			1,
			AsyncDataStream.OutputMode.UNORDERED);

		operator.setup(
			containingTask,
			streamConfig,
			output);

		operator.open();

		final StreamRecord<Integer> streamRecord = new StreamRecord<>(42, timestamp);

		synchronized (lock) {
			// processing an element will register a timeout
			operator.processElement(streamRecord);
		}

		synchronized (lock) {
			// closing the operator waits until all inputs have been processed
			operator.close();
		}

		// check that we actually outputted the result of the single input
		verify(output).collect(eq(streamRecord));
		verify(processingTimeService).registerTimer(eq(processingTimeService.getCurrentProcessingTime() + timeout), any(ProcessingTimeCallback.class));

		// check that we have cancelled our registered timeout
		verify(scheduledFuture).cancel(eq(true));
	}

	/**
	 * FLINK-6435
	 *
	 * <p>Tests that a user exception triggers the completion of a StreamElementQueueEntry and does not wait to until
	 * another StreamElementQueueEntry is properly completed before it is collected.
	 */
	@Test(timeout = 2000)
	public void testOrderedWaitUserExceptionHandling() throws Exception {
		testUserExceptionHandling(AsyncDataStream.OutputMode.ORDERED);
	}

	/**
	 * FLINK-6435
	 *
	 * <p>Tests that a user exception triggers the completion of a StreamElementQueueEntry and does not wait to until
	 * another StreamElementQueueEntry is properly completed before it is collected.
	 */
	@Test(timeout = 2000)
	public void testUnorderedWaitUserExceptionHandling() throws Exception {
		testUserExceptionHandling(AsyncDataStream.OutputMode.UNORDERED);
	}

	private void testUserExceptionHandling(AsyncDataStream.OutputMode outputMode) throws Exception {
		UserExceptionAsyncFunction asyncWaitFunction = new UserExceptionAsyncFunction();
		long timeout = 2000L;

		AsyncWaitOperator<Integer, Integer> asyncWaitOperator = new AsyncWaitOperator<>(
			asyncWaitFunction,
			TIMEOUT,
			2,
			outputMode);

		final Environment mockEnvironment = createMockEnvironment();

		OneInputStreamOperatorTestHarness<Integer, Integer> harness = new OneInputStreamOperatorTestHarness<>(
			asyncWaitOperator,
			IntSerializer.INSTANCE,
			mockEnvironment);

		harness.open();

		synchronized (harness.getCheckpointLock()) {
			harness.processElement(1, 1L);
		}

		verify(harness.getEnvironment(), timeout(timeout)).failExternally(any(Exception.class));

		synchronized (harness.getCheckpointLock()) {
			harness.close();
		}
	}

	/**
	 * AsyncFunction which completes the result with an {@link Exception}.
	 */
	private static class UserExceptionAsyncFunction implements AsyncFunction<Integer, Integer> {

		private static final long serialVersionUID = 6326568632967110990L;

		@Override
		public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture) throws Exception {
			resultFuture.completeExceptionally(new Exception("Test exception"));
		}
	}

	/**
	 * FLINK-6435
	 *
	 * <p>Tests that timeout exceptions are properly handled in ordered output mode. The proper handling means that
	 * a StreamElementQueueEntry is completed in case of a timeout exception.
	 */
	@Test
	public void testOrderedWaitTimeoutHandling() throws Exception {
		testTimeoutExceptionHandling(AsyncDataStream.OutputMode.ORDERED);
	}

	/**
	 * FLINK-6435
	 *
	 * <p>Tests that timeout exceptions are properly handled in ordered output mode. The proper handling means that
	 * a StreamElementQueueEntry is completed in case of a timeout exception.
	 */
	@Test
	public void testUnorderedWaitTimeoutHandling() throws Exception {
		testTimeoutExceptionHandling(AsyncDataStream.OutputMode.UNORDERED);
	}

	private void testTimeoutExceptionHandling(AsyncDataStream.OutputMode outputMode) throws Exception {
		AsyncFunction<Integer, Integer> asyncFunction = new NoOpAsyncFunction<>();
		long timeout = 10L; // 1 milli second

		AsyncWaitOperator<Integer, Integer> asyncWaitOperator = new AsyncWaitOperator<>(
			asyncFunction,
			timeout,
			2,
			outputMode);

		final Environment mockenvironment = createMockEnvironment();

		OneInputStreamOperatorTestHarness<Integer, Integer> harness = new OneInputStreamOperatorTestHarness<>(
			asyncWaitOperator,
			IntSerializer.INSTANCE,
			mockenvironment);

		harness.open();

		synchronized (harness.getCheckpointLock()) {
			harness.processElement(1, 1L);
		}

		harness.setProcessingTime(10L);

		verify(harness.getEnvironment(), timeout(100L * timeout)).failExternally(any(Exception.class));

		synchronized (harness.getCheckpointLock()) {
			harness.close();
		}
	}

	/**
	 * Tests that the AysncWaitOperator can restart if checkpointed queue was full.
	 *
	 * <p>See FLINK-7949
	 */
	@Test(timeout = 10000)
	public void testRestartWithFullQueue() throws Exception {
		int capacity = 10;

		// 1. create the snapshot which contains capacity + 1 elements
		final CompletableFuture<Void> trigger = new CompletableFuture<>();
		final ControllableAsyncFunction<Integer> controllableAsyncFunction = new ControllableAsyncFunction<>(trigger);

		final OneInputStreamOperatorTestHarness<Integer, Integer> snapshotHarness = new OneInputStreamOperatorTestHarness<>(
			new AsyncWaitOperator<>(
				controllableAsyncFunction, // the NoOpAsyncFunction is like a blocking function
				1000L,
				capacity,
				AsyncDataStream.OutputMode.ORDERED),
			IntSerializer.INSTANCE);

		snapshotHarness.open();

		final OperatorStateHandles snapshot;

		final ArrayList<Integer> expectedOutput = new ArrayList<>(capacity + 1);

		try {
			synchronized (snapshotHarness.getCheckpointLock()) {
				for (int i = 0; i < capacity; i++) {
					snapshotHarness.processElement(i, 0L);
					expectedOutput.add(i);
				}
			}

			expectedOutput.add(capacity);

			final OneShotLatch lastElement = new OneShotLatch();

			final CheckedThread lastElementWriter = new CheckedThread() {
				@Override
				public void go() throws Exception {
					synchronized (snapshotHarness.getCheckpointLock()) {
						lastElement.trigger();
						snapshotHarness.processElement(capacity, 0L);
					}
				}
			};

			lastElementWriter.start();

			lastElement.await();

			synchronized (snapshotHarness.getCheckpointLock()) {
				// execute the snapshot within the checkpoint lock, because then it is guaranteed
				// that the lastElementWriter has written the exceeding element
				snapshot = snapshotHarness.snapshot(0L, 0L);
			}

			// trigger the computation to make the close call finish
			trigger.complete(null);
		} finally {
			synchronized (snapshotHarness.getCheckpointLock()) {
				snapshotHarness.close();
			}
		}

		// 2. restore the snapshot and check that we complete
		final OneInputStreamOperatorTestHarness<Integer, Integer> recoverHarness = new OneInputStreamOperatorTestHarness<>(
			new AsyncWaitOperator<>(
				new ControllableAsyncFunction<>(CompletableFuture.completedFuture(null)),
				1000L,
				capacity,
				AsyncDataStream.OutputMode.ORDERED),
			IntSerializer.INSTANCE);

		recoverHarness.initializeState(snapshot);

		synchronized (recoverHarness.getCheckpointLock()) {
			recoverHarness.open();
		}

		synchronized (recoverHarness.getCheckpointLock()) {
			recoverHarness.close();
		}

		final ConcurrentLinkedQueue<Object> output = recoverHarness.getOutput();

		assertThat(output.size(), Matchers.equalTo(capacity + 1));

		final ArrayList<Integer> outputElements = new ArrayList<>(capacity + 1);

		for (int i = 0; i < capacity + 1; i++) {
			StreamRecord<Integer> streamRecord = ((StreamRecord<Integer>) output.poll());
			outputElements.add(streamRecord.getValue());
		}

		assertThat(outputElements, Matchers.equalTo(expectedOutput));
	}

	private static class ControllableAsyncFunction<IN> implements AsyncFunction<IN, IN> {

		private static final long serialVersionUID = -4214078239267288636L;

		private transient CompletableFuture<Void> trigger;

		private ControllableAsyncFunction(CompletableFuture<Void> trigger) {
			this.trigger = Preconditions.checkNotNull(trigger);
		}

		@Override
		public void asyncInvoke(IN input, ResultFuture<IN> resultFuture) throws Exception {
			trigger.thenAccept(v -> resultFuture.complete(Collections.singleton(input)));
		}
	}

	private static class NoOpAsyncFunction<IN, OUT> implements AsyncFunction<IN, OUT> {
		private static final long serialVersionUID = -3060481953330480694L;

		@Override
		public void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception {
			// no op
		}
	}
}
