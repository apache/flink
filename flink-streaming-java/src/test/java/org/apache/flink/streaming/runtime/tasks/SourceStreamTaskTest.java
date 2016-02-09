/**
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

import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StoppableStreamSource;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

/**
 * These tests verify that the RichFunction methods are called (in correct order). And that
 * checkpointing/element emission don't occur concurrently.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ResultPartitionWriter.class})
public class SourceStreamTaskTest {


	/**
	 * This test verifies that open() and close() are correctly called by the StreamTask.
	 */
	@Test
	public void testOpenClose() throws Exception {
		final SourceStreamTask<String> sourceTask = new SourceStreamTask<String>();
		final StreamTaskTestHarness<String> testHarness = new StreamTaskTestHarness<String>(sourceTask, BasicTypeInfo.STRING_TYPE_INFO);

		StreamConfig streamConfig = testHarness.getStreamConfig();
		StreamSource<String> sourceOperator = new StreamSource<String>(new OpenCloseTestSource());
		streamConfig.setStreamOperator(sourceOperator);

		testHarness.invoke();
		testHarness.waitForTaskCompletion();

		Assert.assertTrue("RichFunction methods where not called.", OpenCloseTestSource.closeCalled);

		List<String> resultElements = TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
		Assert.assertEquals(10, resultElements.size());
	}

	// test flag for testStop()
	static boolean stopped = false;

	@Test
	public void testStop() {
		final StoppableSourceStreamTask<Object> sourceTask = new StoppableSourceStreamTask<Object>();
		sourceTask.headOperator = new StoppableStreamSource<Object>(new StoppableSource());

		sourceTask.stop();

		Assert.assertTrue(stopped);
	}

	/**
	 * This test ensures that the SourceStreamTask properly serializes checkpointing
	 * and element emission. This also verifies that there are no concurrent invocations
	 * of the checkpoint method on the source operator.
	 *
	 * The source emits elements and performs checkpoints. We have several checkpointer threads
	 * that fire checkpoint requests at the source task.
	 *
	 * If element emission and checkpointing are not in series the count of elements at the
	 * beginning of a checkpoint and at the end of a checkpoint are not the same because the
	 * source kept emitting elements while the checkpoint was ongoing.
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testCheckpointing() throws Exception {
		final int NUM_ELEMENTS = 100;
		final int NUM_CHECKPOINTS = 100;
		final int NUM_CHECKPOINTERS = 1;
		final int CHECKPOINT_INTERVAL = 5; // in ms
		final int SOURCE_CHECKPOINT_DELAY = 1000; // how many random values we sum up in storeCheckpoint
		final int SOURCE_READ_DELAY = 1; // in ms

		ExecutorService executor = Executors.newFixedThreadPool(10);
		try {
			final TupleTypeInfo<Tuple2<Long, Integer>> typeInfo = new TupleTypeInfo<Tuple2<Long, Integer>>(BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);
			final SourceStreamTask<Tuple2<Long, Integer>> sourceTask = new SourceStreamTask<Tuple2<Long, Integer>>();
			final StreamTaskTestHarness<Tuple2<Long, Integer>> testHarness = new StreamTaskTestHarness<Tuple2<Long, Integer>>(sourceTask, typeInfo);

			StreamConfig streamConfig = testHarness.getStreamConfig();
			StreamSource<Tuple2<Long, Integer>> sourceOperator = new StreamSource<Tuple2<Long, Integer>>(new MockSource(NUM_ELEMENTS, SOURCE_CHECKPOINT_DELAY, SOURCE_READ_DELAY));
			streamConfig.setStreamOperator(sourceOperator);

			// prepare the

			Future<Boolean>[] checkpointerResults = new Future[NUM_CHECKPOINTERS];

			// invoke this first, so the tasks are actually running when the checkpoints are scheduled
			testHarness.invoke();

			for (int i = 0; i < NUM_CHECKPOINTERS; i++) {
				checkpointerResults[i] = executor.submit(new Checkpointer(NUM_CHECKPOINTS, CHECKPOINT_INTERVAL, sourceTask));
			}

			testHarness.waitForTaskCompletion();

			// Get the result from the checkpointers, if these threw an exception it
			// will be rethrown here
			for (int i = 0; i < NUM_CHECKPOINTERS; i++) {
				if (!checkpointerResults[i].isDone()) {
					checkpointerResults[i].cancel(true);
				}
				if (!checkpointerResults[i].isCancelled()) {
					checkpointerResults[i].get();
				}
			}

			List<Tuple2<Long, Integer>> resultElements = TestHarnessUtil.getRawElementsFromOutput(testHarness.getOutput());
			Assert.assertEquals(NUM_ELEMENTS, resultElements.size());
		}
		finally {
			executor.shutdown();
		}
	}

	private static class StoppableSource extends RichSourceFunction<Object> implements StoppableFunction {
		private static final long serialVersionUID = 728864804042338806L;

		@Override
		public void run(org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<Object> ctx)
				throws Exception {
		}

		@Override
		public void cancel() {}

		@Override
		public void stop() {
			stopped = true;
		}
	}

	private static class MockSource implements SourceFunction<Tuple2<Long, Integer>>, Checkpointed {
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
				} catch (InterruptedException e) {
					e.printStackTrace();
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
		public Serializable snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
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
			return sum;
		}

		@Override
		public void restoreState(Serializable state) {

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
				sourceTask.triggerCheckpoint(currentCheckpointId, 0L);
				Thread.sleep(checkpointInterval);
			}
			return true;
		}
	}

	public static class OpenCloseTestSource extends RichSourceFunction<String> {
		private static final long serialVersionUID = 1L;

		public static boolean openCalled = false;
		public static boolean closeCalled = false;

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
}

