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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichEventTimeSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.*;

/**
 * This verfies that checkpointing works correctly with event time windows.
 *
 * <p>
 * This is a version of {@link EventTimeWindowCheckpointingITCase} for All-Windows.
 */
@SuppressWarnings("serial")
public class EventTimeAllWindowCheckpointingITCase extends TestLogger {

	private static final int PARALLELISM = 4;

	private static ForkableFlinkMiniCluster cluster;


	@BeforeClass
	public static void startTestCluster() {
		Configuration config = new Configuration();
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 2);
		config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, PARALLELISM / 2);
		config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 48);
		config.setString(ConfigConstants.DEFAULT_EXECUTION_RETRY_DELAY_KEY, "0 ms");

		cluster = new ForkableFlinkMiniCluster(config, false);
		cluster.start();
	}

	@AfterClass
	public static void stopTestCluster() {
		if (cluster != null) {
			cluster.stop();
		}
	}

	// ------------------------------------------------------------------------

	@Test
	public void testTumblingTimeWindow() {
		final int NUM_ELEMENTS_PER_KEY = 3000;
		final int WINDOW_SIZE = 100;
		final int NUM_KEYS = 1;
		FailingSource.reset();
		
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
					"localhost", cluster.getLeaderRPCPort());
			
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setNumberOfExecutionRetries(3);
			env.getConfig().disableSysoutLogging();

			env
					.addSource(new FailingSource(NUM_KEYS,
							NUM_ELEMENTS_PER_KEY,
							NUM_ELEMENTS_PER_KEY / 3))
					.rebalance()
					.timeWindowAll(Time.of(WINDOW_SIZE, MILLISECONDS))
					.apply(new RichAllWindowFunction<Tuple2<Long, IntType>, Tuple4<Long, Long, Long, IntType>, TimeWindow>() {

						private boolean open = false;

						@Override
						public void open(Configuration parameters) {
							assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());
							open = true;
						}

						@Override
						public void apply(
								TimeWindow window,
								Iterable<Tuple2<Long, IntType>> values,
								Collector<Tuple4<Long, Long, Long, IntType>> out) {

							// validate that the function has been opened properly
							assertTrue(open);

							int sum = 0;
							long key = -1;

							for (Tuple2<Long, IntType> value : values) {
								sum += value.f1.value;
								key = value.f0;
							}
							out.collect(new Tuple4<>(key, window.getStart(), window.getEnd(), new IntType(sum)));
						}
					})
					.addSink(new ValidatingSink(NUM_KEYS, NUM_ELEMENTS_PER_KEY / WINDOW_SIZE)).setParallelism(1);


			tryExecute(env, "Tumbling Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSlidingTimeWindow() {
		final int NUM_ELEMENTS_PER_KEY = 3000;
		final int WINDOW_SIZE = 1000;
		final int WINDOW_SLIDE = 100;
		final int NUM_KEYS = 1;
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
					"localhost", cluster.getLeaderRPCPort());

			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setNumberOfExecutionRetries(3);
			env.getConfig().disableSysoutLogging();

			env
					.addSource(new FailingSource(NUM_KEYS, NUM_ELEMENTS_PER_KEY, NUM_ELEMENTS_PER_KEY / 3))
					.rebalance()
					.timeWindowAll(Time.of(WINDOW_SIZE, MILLISECONDS), Time.of(WINDOW_SLIDE, MILLISECONDS))
					.apply(new RichAllWindowFunction<Tuple2<Long, IntType>, Tuple4<Long, Long, Long, IntType>, TimeWindow>() {

						private boolean open = false;

						@Override
						public void open(Configuration parameters) {
							assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());
							open = true;
						}

						@Override
						public void apply(
								TimeWindow window,
								Iterable<Tuple2<Long, IntType>> values,
								Collector<Tuple4<Long, Long, Long, IntType>> out) {

							// validate that the function has been opened properly
							assertTrue(open);

							int sum = 0;
							long key = -1;

							for (Tuple2<Long, IntType> value : values) {
								sum += value.f1.value;
								key = value.f0;
							}
							out.collect(new Tuple4<>(key, window.getStart(), window.getEnd(), new IntType(sum)));
						}
					})
					.addSink(new ValidatingSink(NUM_KEYS, NUM_ELEMENTS_PER_KEY / WINDOW_SLIDE)).setParallelism(1);


			tryExecute(env, "Tumbling Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPreAggregatedTumblingTimeWindow() {
		final int NUM_ELEMENTS_PER_KEY = 3000;
		final int WINDOW_SIZE = 100;
		final int NUM_KEYS = 1;
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
					"localhost", cluster.getLeaderRPCPort());

			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setNumberOfExecutionRetries(3);
			env.getConfig().disableSysoutLogging();

			env
					.addSource(new FailingSource(NUM_KEYS,
							NUM_ELEMENTS_PER_KEY,
							NUM_ELEMENTS_PER_KEY / 3))
					.rebalance()
					.timeWindowAll(Time.of(WINDOW_SIZE, MILLISECONDS))
					.apply(
							new RichReduceFunction<Tuple2<Long, IntType>>() {

								private boolean open = false;

								@Override
								public void open(Configuration parameters) {
									assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());
									open = true;
								}

								@Override
								public Tuple2<Long, IntType> reduce(
										Tuple2<Long, IntType> a,
										Tuple2<Long, IntType> b) {

									// validate that the function has been opened properly
									assertTrue(open);
									return new Tuple2<>(a.f0, new IntType(a.f1.value + b.f1.value));
								}
							},
							new RichAllWindowFunction<Tuple2<Long, IntType>, Tuple4<Long, Long, Long, IntType>, TimeWindow>() {

						private boolean open = false;

						@Override
						public void open(Configuration parameters) {
							assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());
							open = true;
						}

						@Override
						public void apply(
								TimeWindow window,
								Iterable<Tuple2<Long, IntType>> values,
								Collector<Tuple4<Long, Long, Long, IntType>> out) {

							// validate that the function has been opened properly
							assertTrue(open);

							int sum = 0;
							long key = -1;

							for (Tuple2<Long, IntType> value : values) {
								sum += value.f1.value;
								key = value.f0;
							}
							out.collect(new Tuple4<>(key, window.getStart(), window.getEnd(), new IntType(sum)));
						}
					})
					.addSink(new ValidatingSink(NUM_KEYS, NUM_ELEMENTS_PER_KEY / WINDOW_SIZE)).setParallelism(1);


			tryExecute(env, "Tumbling Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPreAggregatedSlidingTimeWindow() {
		final int NUM_ELEMENTS_PER_KEY = 3000;
		final int WINDOW_SIZE = 1000;
		final int WINDOW_SLIDE = 100;
		final int NUM_KEYS = 1;
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
					"localhost", cluster.getLeaderRPCPort());

			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setNumberOfExecutionRetries(3);
			env.getConfig().disableSysoutLogging();

			env
					.addSource(new FailingSource(NUM_KEYS,
							NUM_ELEMENTS_PER_KEY,
							NUM_ELEMENTS_PER_KEY / 3))
					.rebalance()
					.timeWindowAll(Time.of(WINDOW_SIZE, MILLISECONDS),
							Time.of(WINDOW_SLIDE, MILLISECONDS))
					.apply(
							new RichReduceFunction<Tuple2<Long, IntType>>() {

								private boolean open = false;

								@Override
								public void open(Configuration parameters) {
									assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());
									open = true;
								}

								@Override
								public Tuple2<Long, IntType> reduce(
										Tuple2<Long, IntType> a,
										Tuple2<Long, IntType> b) {

									// validate that the function has been opened properly
									assertTrue(open);
									return new Tuple2<>(a.f0, new IntType(a.f1.value + b.f1.value));
								}
							},
							new RichAllWindowFunction<Tuple2<Long, IntType>, Tuple4<Long, Long, Long, IntType>, TimeWindow>() {

						private boolean open = false;

						@Override
						public void open(Configuration parameters) {
							assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());
							open = true;
						}

						@Override
						public void apply(
								TimeWindow window,
								Iterable<Tuple2<Long, IntType>> values,
								Collector<Tuple4<Long, Long, Long, IntType>> out) {

							// validate that the function has been opened properly
							assertTrue(open);

							int sum = 0;
							long key = -1;

							for (Tuple2<Long, IntType> value : values) {
								sum += value.f1.value;
								key = value.f0;
							}
							out.collect(new Tuple4<>(key, window.getStart(), window.getEnd(), new IntType(sum)));
						}
					})
					.addSink(new ValidatingSink(NUM_KEYS, NUM_ELEMENTS_PER_KEY / WINDOW_SLIDE)).setParallelism(1);


			tryExecute(env, "Tumbling Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}


	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static class FailingSource extends RichEventTimeSourceFunction<Tuple2<Long, IntType>>
			implements Checkpointed<Integer>, CheckpointNotifier
	{
		private static volatile boolean failedBefore = false;

		private final int numKeys;
		private final int numElementsToEmit;
		private final int failureAfterNumElements;

		private volatile int numElementsEmitted;
		private volatile int numSuccessfulCheckpoints;
		private volatile boolean running = true;

		private FailingSource(int numKeys, int numElementsToEmitPerKey, int failureAfterNumElements) {
			this.numKeys = numKeys;
			this.numElementsToEmit = numElementsToEmitPerKey;
			this.failureAfterNumElements = failureAfterNumElements;
		}

		@Override
		public void open(Configuration parameters) {
			// non-parallel source
			assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());
		}

		@Override
		public void run(SourceContext<Tuple2<Long, IntType>> ctx) throws Exception {
			// we loop longer than we have elements, to permit delayed checkpoints
			// to still cause a failure
			while (running) {

				if (!failedBefore) {
					// delay a bit, if we have not failed before
					Thread.sleep(1);
					if (numSuccessfulCheckpoints >= 2 && numElementsEmitted >= failureAfterNumElements) {
						// cause a failure if we have not failed before and have reached
						// enough completed checkpoints and elements
						failedBefore = true;
						throw new Exception("Artificial Failure");
					}
				}

				if (numElementsEmitted < numElementsToEmit &&
						(failedBefore || numElementsEmitted <= failureAfterNumElements))
				{
					// the function failed before, or we are in the elements before the failure
					synchronized (ctx.getCheckpointLock()) {
						int next = numElementsEmitted++;
						for (long i = 0; i < numKeys; i++) {
							ctx.collectWithTimestamp(new Tuple2<>(i, new IntType(next)), next);
						}
						ctx.emitWatermark(new Watermark(next));
					}
				}
				else {
					// if our work is done, delay a bit to prevent busy waiting
					Thread.sleep(1);
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			numSuccessfulCheckpoints++;
		}

		@Override
		public Integer snapshotState(long checkpointId, long checkpointTimestamp) {
			return numElementsEmitted;
		}

		@Override
		public void restoreState(Integer state) {
			numElementsEmitted = state;
		}

		public static void reset() {
			failedBefore = false;
		}
	}

	private static class ValidatingSink extends RichSinkFunction<Tuple4<Long, Long, Long, IntType>>
			implements Checkpointed<HashMap<Long, Integer>> {

		private final HashMap<Long, Integer> windowCounts = new HashMap<>();

		private final int numKeys;
		private final int numWindowsExpected;

		private ValidatingSink(int numKeys, int numWindowsExpected) {
			this.numKeys = numKeys;
			this.numWindowsExpected = numWindowsExpected;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			// this sink can only work with DOP 1
			assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());

			// it can happen that a checkpoint happens when the complete success state is
			// already set. In that case we restart with the final state and would never
			// finish because no more elements arrive.
			if (windowCounts.size() == numKeys) {
				boolean seenAll = true;
				for (Integer windowCount: windowCounts.values()) {
					if (windowCount != numWindowsExpected) {
						seenAll = false;
						break;
					}
				}
				if (seenAll) {
					throw new SuccessException();
				}
			}
		}

		@Override
		public void close() throws Exception {
			boolean seenAll = true;
			if (windowCounts.size() == numKeys) {
				for (Integer windowCount: windowCounts.values()) {
					if (windowCount != numWindowsExpected) {
						seenAll = false;
						break;
					}
				}
			}
			assertTrue("The source must see all expected windows.", seenAll);
		}

		@Override
		public void invoke(Tuple4<Long, Long, Long, IntType> value) throws Exception {

			// verify the contents of that window, Tuple4.f1 and .f2 are the window start/end
			// the sum should be "sum (start .. end-1)"

			int expectedSum = 0;
			for (long i = value.f1; i < value.f2; i++) {
				// only sum up positive vals, to filter out the negative start of the
				// first sliding windows
				if (i > 0) {
					expectedSum += i;
				}
			}

			assertEquals("Window start: " + value.f1 + " end: " + value.f2, expectedSum, value.f3.value);


			Integer curr = windowCounts.get(value.f0);
			if (curr != null) {
				windowCounts.put(value.f0, curr + 1);
			}
			else {
				windowCounts.put(value.f0, 1);
			}

			boolean seenAll = true;
			if (windowCounts.size() == numKeys) {
				for (Integer windowCount: windowCounts.values()) {
					if (windowCount < numWindowsExpected) {
						seenAll = false;
						break;
					} else if (windowCount > numWindowsExpected) {
						fail("Window count to high: " + windowCount);
					}
				}

				if (seenAll) {
					// exit
					throw new SuccessException();
				}

			}
		}

		@Override
		public HashMap<Long, Integer> snapshotState(long checkpointId, long checkpointTimestamp) {
			return this.windowCounts;
		}

		@Override
		public void restoreState(HashMap<Long, Integer> state) {
			this.windowCounts.putAll(state);
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	public static void tryExecute(StreamExecutionEnvironment env, String jobName) throws Exception {
		try {
			env.execute(jobName);
		}
		catch (ProgramInvocationException | JobExecutionException root) {
			Throwable cause = root.getCause();

			// search for nested SuccessExceptions
			int depth = 0;
			while (!(cause instanceof SuccessException)) {
				if (cause == null || depth++ == 20) {
					root.printStackTrace();
					fail("Test failed: " + root.getMessage());
				}
				else {
					cause = cause.getCause();
				}
			}
		}
	}

	public static class IntType {

		public int value;

		public IntType() {}

		public IntType(int value) { this.value = value; }
	}

	static final class SuccessException extends Exception {
		private static final long serialVersionUID = -9218191172606739598L;
	}
}
