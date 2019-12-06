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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertTrue;

/**
 * These tests verify that the RichFunction methods are called (in correct order). And that
 * checkpointing/element emission don't occur concurrently.
 */
public class SourceStreamTaskTest {

	/**
	 * This test verifies that open() and close() are correctly called by the StreamTask.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testOpenClose() throws Exception {
		final StreamTaskTestHarness<String> testHarness = new StreamTaskTestHarness<>(
				SourceStreamTask::new, BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.setupOutputForSingletonOperatorChain();

		StreamConfig streamConfig = testHarness.getStreamConfig();
		StreamSource<String, ?> sourceOperator = new StreamSource<>(new OpenCloseTestSource());
		streamConfig.setStreamOperator(sourceOperator);
		streamConfig.setOperatorID(new OperatorID());

		testHarness.invoke();
		testHarness.waitForTaskCompletion();

		assertTrue("RichFunction methods where not called.", OpenCloseTestSource.closeCalled);

		List<String> resultElements = TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
		Assert.assertEquals(10, resultElements.size());
	}

	/**
	 * This test ensures that the SourceStreamTask properly serializes checkpointing
	 * and element emission. This also verifies that there are no concurrent invocations
	 * of the checkpoint method on the source operator.
	 *
	 * <p>The source emits elements and performs checkpoints. We have several checkpointer threads
	 * that fire checkpoint requests at the source task.
	 *
	 * <p>If element emission and checkpointing are not in series the count of elements at the
	 * beginning of a checkpoint and at the end of a checkpoint are not the same because the
	 * source kept emitting elements while the checkpoint was ongoing.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testCheckpointing() throws Exception {
		final int numElements = 100;
		final int numCheckpoints = 100;
		final int numCheckpointers = 1;
		final int checkpointInterval = 5; // in ms
		final int sourceCheckpointDelay = 1000; // how many random values we sum up in storeCheckpoint
		final int sourceReadDelay = 1; // in ms

		ExecutorService executor = Executors.newFixedThreadPool(10);
		try {
			final TupleTypeInfo<Tuple2<Long, Integer>> typeInfo = new TupleTypeInfo<>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

			final StreamTaskTestHarness<Tuple2<Long, Integer>> testHarness = new StreamTaskTestHarness<>(
					SourceStreamTask::new, typeInfo);
			testHarness.setupOutputForSingletonOperatorChain();

			StreamConfig streamConfig = testHarness.getStreamConfig();
			StreamSource<Tuple2<Long, Integer>, ?> sourceOperator = new StreamSource<>(new MockSource(numElements, sourceCheckpointDelay, sourceReadDelay));
			streamConfig.setStreamOperator(sourceOperator);
			streamConfig.setOperatorID(new OperatorID());

			// prepare the

			Future<Boolean>[] checkpointerResults = new Future[numCheckpointers];

			// invoke this first, so the tasks are actually running when the checkpoints are scheduled
			testHarness.invoke();
			testHarness.waitForTaskRunning();

			final StreamTask<Tuple2<Long, Integer>, ?> sourceTask = testHarness.getTask();

			for (int i = 0; i < numCheckpointers; i++) {
				checkpointerResults[i] = executor.submit(new Checkpointer(numCheckpoints, checkpointInterval, sourceTask));
			}

			testHarness.waitForTaskCompletion();

			// Get the result from the checkpointers, if these threw an exception it
			// will be rethrown here
			for (int i = 0; i < numCheckpointers; i++) {
				if (!checkpointerResults[i].isDone()) {
					checkpointerResults[i].cancel(true);
				}
				if (!checkpointerResults[i].isCancelled()) {
					checkpointerResults[i].get();
				}
			}

			List<Tuple2<Long, Integer>> resultElements = TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
			Assert.assertEquals(numElements, resultElements.size());
		}
		finally {
			executor.shutdown();
		}
	}

	@Test
	public void testMarkingEndOfInput() throws Exception {
		final StreamTaskTestHarness<String> testHarness = new StreamTaskTestHarness<>(
			SourceStreamTask::new,
			BasicTypeInfo.STRING_TYPE_INFO);

		testHarness
			.setupOperatorChain(
				new OperatorID(),
				new StreamSource<>(new FromElementsFunction<>(
					BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()), "Hello")))
			.chain(
				new OperatorID(),
				new TestBoundedOneInputStreamOperator("Operator1"),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
			.finish();

		StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.invoke();
		testHarness.waitForTaskCompletion();

		expectedOutput.add(new StreamRecord<>("Hello"));
		expectedOutput.add(new StreamRecord<>("[Operator1]: Bye"));

		TestHarnessUtil.assertOutputEquals("Output was not correct.",
			expectedOutput,
			testHarness.getOutput());
	}

	@Test
	public void testNotMarkingEndOfInputWhenTaskCancelled () throws Exception {
		final StreamTaskTestHarness<String> testHarness = new StreamTaskTestHarness<>(
			SourceStreamTask::new,
			BasicTypeInfo.STRING_TYPE_INFO);

		testHarness
			.setupOperatorChain(
				new OperatorID(),
				new StreamSource<>(new CancelTestSource(
					BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()), "Hello")))
			.chain(
				new OperatorID(),
				new TestBoundedOneInputStreamOperator("Operator1"),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
			.finish();

		StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.invoke();
		CancelTestSource.getDataProcessing().get();
		testHarness.getTask().cancel();

		try {
			testHarness.waitForTaskCompletion();
		} catch (Throwable t) {
			assertTrue(ExceptionUtils.findThrowable(t, CancelTaskException.class).isPresent());
		}

		expectedOutput.add(new StreamRecord<>("Hello"));

		TestHarnessUtil.assertOutputEquals("Output was not correct.",
			expectedOutput,
			testHarness.getOutput());
	}

	@Test
	public void testCancellationWithSourceBlockedOnLock() throws Exception {
		testCancellationWithSourceBlockedOnLock(false);
	}

	@Test
	public void testCancellationWithSourceBlockedOnLockAndThrowingOnError() throws Exception {
		testCancellationWithSourceBlockedOnLock(true);
	}

	/**
	 * Note that this test is testing also for the shared cancellation logic inside {@link StreamTask}
	 * which, as of the time this test is being written, is not tested anywhere else
	 * (like {@link StreamTaskTest} or {@link OneInputStreamTaskTest}).
	 */
	public void testCancellationWithSourceBlockedOnLock(boolean throwInCancel) throws Exception {
		final StreamTaskTestHarness<String> testHarness = new StreamTaskTestHarness<>(
			SourceStreamTask::new,
			BasicTypeInfo.STRING_TYPE_INFO);

		CancelLockingSource.reset();
		testHarness
			.setupOperatorChain(
				new OperatorID(),
				new StreamSource<>(new CancelLockingSource(throwInCancel)))
			.chain(
				new OperatorID(),
				new TestBoundedOneInputStreamOperator("Operator1"),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
			.finish();

		StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		testHarness.invoke();
		CancelLockingSource.awaitRunning();

		try {
			testHarness.getTask().cancel();
		}
		catch (ExpectedTestException e) {
			checkState(throwInCancel);
		}

		try {
			testHarness.waitForTaskCompletion();
		} catch (Throwable t) {
			if (!ExceptionUtils.findThrowable(t, CancelTaskException.class).isPresent()) {
				throw t;
			}
		}
	}

	/**
	 * A source that locks if cancellation attempts to cleanly shut down.
	 */
	public static class CancelLockingSource implements SourceFunction<String> {
		private static final long serialVersionUID = 8713065281092996042L;

		private static CompletableFuture<Void> isRunning = new CompletableFuture<>();

		private final boolean throwOnCancel;

		private volatile boolean cancelled = false;

		public CancelLockingSource(boolean throwOnCancel) {
			this.throwOnCancel = throwOnCancel;
		}

		public static void reset() {
			isRunning = new CompletableFuture<>();
		}

		public static void awaitRunning() throws ExecutionException, InterruptedException {
			isRunning.get();
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			synchronized (ctx.getCheckpointLock()) {
				while (!cancelled) {
					isRunning.complete(null);

					if (throwOnCancel) {
						Thread.sleep(1000000000);
					}
					else {
						try {
							//noinspection SleepWhileHoldingLock
							Thread.sleep(1000000000);
						} catch (InterruptedException ignored) {
						}
					}
				}
			}
		}

		@Override
		public void cancel() {
			if (throwOnCancel) {
				throw new ExpectedTestException();
			}
			cancelled = true;
		}
	}

	@Test
	public void testInterruptedNotSwallowed() throws Exception {
		final StreamTaskTestHarness<String> testHarness = new StreamTaskTestHarness<>(
			SourceStreamTask::new,
			BasicTypeInfo.STRING_TYPE_INFO);

		CancelLockingSource.reset();
		testHarness
			.setupOperatorChain(
				new OperatorID(),
				new StreamSource<>(new InterruptedSource()))
			.chain(
				new OperatorID(),
				new TestBoundedOneInputStreamOperator("Operator1"),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()))
			.finish();

		StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		testHarness.invoke();
		try {
			testHarness.waitForTaskCompletion();
		} catch (Exception e) {
			if (!(ExceptionUtils.findThrowable(e, InterruptedException.class).isPresent())) {
				throw e;
			}
		}
	}

	/**
	 * A source that locks if cancellation attempts to cleanly shut down.
	 */
	public static class InterruptedSource implements SourceFunction<String> {
		private static final long serialVersionUID = 8713065281092996042L;

		private static CompletableFuture<Void> isRunning = new CompletableFuture<>();

		public static void reset() {
			isRunning = new CompletableFuture<>();
		}

		public static void awaitRunning() throws ExecutionException, InterruptedException {
			isRunning.get();
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			synchronized (ctx.getCheckpointLock()) {
				isRunning.complete(null);
				Thread.currentThread().interrupt();
				throw new InterruptedException();
			}
		}

		@Override
		public void cancel() {
		}
	}

	/**
	 * Cancelling should not swallow exceptions in the Invokable. They will eventually be ignored
	 * because the bubble up into the Task thread, where they go nowhere.
	 */
	@Test
	public void cancellingForwardsExceptions() throws Exception {
		final StreamTaskTestHarness<String> testHarness = new StreamTaskTestHarness<>(
				SourceStreamTask::new,
				BasicTypeInfo.STRING_TYPE_INFO);

		final CompletableFuture<Void> operatorRunningWaitingFuture = new CompletableFuture<>();
		ExceptionThrowingSource.setIsInRunLoopFuture(operatorRunningWaitingFuture);

		testHarness.setupOutputForSingletonOperatorChain();
		StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setStreamOperator(new StreamSource<>(new ExceptionThrowingSource()));
		streamConfig.setOperatorID(new OperatorID());

		testHarness.invoke();
		operatorRunningWaitingFuture.get();
		testHarness.getTask().cancel();

		Optional<ExceptionThrowingSource.TestException> testException = Optional.empty();
		try {
			testHarness.waitForTaskCompletion();
		} catch (Throwable t) {
			if (!ExceptionUtils.findThrowable(t, CancelTaskException.class).isPresent()) {
				throw t;
			}
		}
	}

	/**
	 * If finishing a task doesn't swallow exceptions this test would fail with an exception.
	 */
	@Test
	public void finishingIgnoresExceptions() throws Exception {
		final StreamTaskTestHarness<String> testHarness = new StreamTaskTestHarness<>(
				SourceStreamTask::new,
				BasicTypeInfo.STRING_TYPE_INFO);

		final CompletableFuture<Void> operatorRunningWaitingFuture = new CompletableFuture<>();
		ExceptionThrowingSource.setIsInRunLoopFuture(operatorRunningWaitingFuture);

		testHarness.setupOutputForSingletonOperatorChain();
		StreamConfig streamConfig = testHarness.getStreamConfig();
		streamConfig.setStreamOperator(new StreamSource<>(new ExceptionThrowingSource()));
		streamConfig.setOperatorID(new OperatorID());

		testHarness.invoke();
		operatorRunningWaitingFuture.get();
		testHarness.getTask().finishTask();

		testHarness.waitForTaskCompletion();
	}

	private static class MockSource implements SourceFunction<Tuple2<Long, Integer>>, ListCheckpointed<Serializable> {
		private static final long serialVersionUID = 1;

		private int maxElements;
		private int checkpointDelay;
		private int readDelay;

		private volatile int count;
		private volatile long lastCheckpointId = -1;

		private Semaphore semaphore;

		private volatile boolean isRunning = true;

		public MockSource(int maxElements, int checkpointDelay, int readDelay) {
			this.maxElements = maxElements;
			this.checkpointDelay = checkpointDelay;
			this.readDelay = readDelay;
			this.count = 0;
			semaphore = new Semaphore(1);
		}

		@Override
		public void run(SourceContext<Tuple2<Long, Integer>> ctx) {
			final Object lockObject = ctx.getCheckpointLock();
			while (isRunning && count < maxElements) {
				// simulate some work
				try {
					Thread.sleep(readDelay);
				}
				catch (InterruptedException e) {
					// ignore and reset interruption state
					Thread.currentThread().interrupt();
				}

				synchronized (lockObject) {
					ctx.collect(new Tuple2<Long, Integer>(lastCheckpointId, count));
					count++;
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public List<Serializable> snapshotState(long checkpointId, long timestamp) throws Exception {
			if (!semaphore.tryAcquire()) {
				Assert.fail("Concurrent invocation of snapshotState.");
			}
			int startCount = count;
			lastCheckpointId = checkpointId;

			long sum = 0;
			for (int i = 0; i < checkpointDelay; i++) {
				sum += new Random().nextLong();
			}

			if (startCount != count) {
				semaphore.release();
				// This means that next() was invoked while the snapshot was ongoing
				Assert.fail("Count is different at start end end of snapshot.");
			}
			semaphore.release();
			return Collections.<Serializable>singletonList(sum);
		}

		@Override
		public void restoreState(List<Serializable> state) throws Exception {
		}
	}

	/**
	 * This calls triggerInterrupt on the given task with the given interval.
	 */
	private static class Checkpointer implements Callable<Boolean> {
		private final int numCheckpoints;
		private final int checkpointInterval;
		private final AtomicLong checkpointId;
		private final StreamTask<Tuple2<Long, Integer>, ?> sourceTask;

		public Checkpointer(int numCheckpoints, int checkpointInterval, StreamTask<Tuple2<Long, Integer>, ?> task) {
			this.numCheckpoints = numCheckpoints;
			checkpointId = new AtomicLong(0);
			sourceTask = task;
			this.checkpointInterval = checkpointInterval;
		}

		@Override
		public Boolean call() throws Exception {
			for (int i = 0; i < numCheckpoints; i++) {
				long currentCheckpointId = checkpointId.getAndIncrement();
				CheckpointMetaData checkpointMetaData = new CheckpointMetaData(currentCheckpointId, 0L);
				sourceTask.triggerCheckpoint(checkpointMetaData, CheckpointOptions.forCheckpointWithDefaultLocation(), false);
				Thread.sleep(checkpointInterval);
			}
			return true;
		}
	}

	private static class OpenCloseTestSource extends RichSourceFunction<String> {
		private static final long serialVersionUID = 1L;

		public static boolean openCalled = false;
		public static boolean closeCalled = false;

		OpenCloseTestSource() {
			openCalled = false;
			closeCalled = false;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			if (closeCalled) {
				Assert.fail("Close called before open.");
			}
			openCalled = true;
		}

		@Override
		public void close() throws Exception {
			super.close();
			if (!openCalled) {
				Assert.fail("Open was not called before close.");
			}
			closeCalled = true;
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			if (!openCalled) {
				Assert.fail("Open was not called before run.");
			}
			for (int i = 0; i < 10; i++) {
				ctx.collect("Hello" + i);
			}
		}

		@Override
		public void cancel() {}
	}

	private static class CancelTestSource extends FromElementsFunction<String> {
		private static final long serialVersionUID = 8713065281092996067L;

		private static CompletableFuture<Void> dataProcessing = new CompletableFuture<>();

		private static CompletableFuture<Void> cancellationWaiting = new CompletableFuture<>();

		public CancelTestSource(TypeSerializer<String> serializer, String... elements) throws IOException {
			super(serializer, elements);
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			super.run(ctx);

			dataProcessing.complete(null);
			cancellationWaiting.get();
		}

		@Override
		public void cancel() {
			super.cancel();

			cancellationWaiting.complete(null);
		}

		public static CompletableFuture<Void> getDataProcessing() {
			return dataProcessing;
		}
	}

	/**
	 * A {@link SourceFunction} that throws an exception from {@link #run(SourceContext)} when it is
	 * cancelled via {@link #cancel()}.
	 */
	private static class ExceptionThrowingSource implements SourceFunction<String> {

		private static volatile CompletableFuture<Void> isInRunLoop;

		private volatile boolean running = true;

		public static class TestException extends RuntimeException {
			public TestException(String message) {
				super(message);
			}
		}

		public static void setIsInRunLoopFuture(@Nonnull final CompletableFuture<Void> waitingLatch) {
			ExceptionThrowingSource.isInRunLoop = waitingLatch;
		}

		@Override
		public void run(SourceContext<String> ctx) throws TestException {
			checkState(isInRunLoop != null && !isInRunLoop.isDone());

			while (running) {
				if (!isInRunLoop.isDone()) {
					isInRunLoop.complete(null);
				}
				ctx.collect("hello");
			}

			throw new TestException("Oh no, we're failing.");
		}

		@Override
		public void cancel() {
			running = false;
		}
	}
}

