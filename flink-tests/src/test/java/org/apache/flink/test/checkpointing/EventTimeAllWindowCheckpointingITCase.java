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

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.test.util.TestUtils.tryExecute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This verifies that checkpointing works correctly with event time windows.
 *
 * <p>This is a version of {@link AbstractEventTimeWindowCheckpointingITCase} for All-Windows.
 */
@SuppressWarnings("serial")
public class EventTimeAllWindowCheckpointingITCase extends TestLogger {

	private static final int PARALLELISM = 4;

	@ClassRule
	public static final MiniClusterResource MINI_CLUSTER_RESOURCE = new MiniClusterResource(
		new MiniClusterResource.MiniClusterResourceConfiguration(
			getConfiguration(),
			2,
			PARALLELISM / 2));

	private static Configuration getConfiguration() {
		Configuration config = new Configuration();
		config.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 48L);
		config.setString(AkkaOptions.LOOKUP_TIMEOUT, "60 s");
		config.setString(AkkaOptions.ASK_TIMEOUT, "60 s");
		return config;
	}

	// ------------------------------------------------------------------------

	@Test
	public void testTumblingTimeWindow() {
		final int numElementsPerKey = 3000;
		final int windowSize = 100;
		final int numKeys = 1;
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
			env.getConfig().disableSysoutLogging();

			env
					.addSource(new FailingSource(numKeys,
							numElementsPerKey,
							numElementsPerKey / 3))
					.rebalance()
					.timeWindowAll(Time.of(windowSize, MILLISECONDS))
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
					.addSink(new ValidatingSink(numKeys, numElementsPerKey / windowSize)).setParallelism(1);

			tryExecute(env, "Tumbling Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSlidingTimeWindow() {
		final int numElementsPerKey = 3000;
		final int windowSize = 1000;
		final int windowSlide = 100;
		final int numKeys = 1;
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
			env.getConfig().disableSysoutLogging();

			env
					.addSource(new FailingSource(numKeys, numElementsPerKey, numElementsPerKey / 3))
					.rebalance()
					.timeWindowAll(Time.of(windowSize, MILLISECONDS), Time.of(windowSlide, MILLISECONDS))
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
					.addSink(new ValidatingSink(numKeys, numElementsPerKey / windowSlide)).setParallelism(1);

			tryExecute(env, "Sliding Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPreAggregatedTumblingTimeWindow() {
		final int numElementsPerKey = 3000;
		final int windowSize = 100;
		final int numKeys = 1;
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
			env.getConfig().disableSysoutLogging();

			env
					.addSource(new FailingSource(numKeys,
							numElementsPerKey,
							numElementsPerKey / 3))
					.rebalance()
					.timeWindowAll(Time.of(windowSize, MILLISECONDS))
					.reduce(
							new ReduceFunction<Tuple2<Long, IntType>>() {

								@Override
								public Tuple2<Long, IntType> reduce(
										Tuple2<Long, IntType> a,
										Tuple2<Long, IntType> b) {

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
								Iterable<Tuple2<Long, IntType>> input,
								Collector<Tuple4<Long, Long, Long, IntType>> out) {

							// validate that the function has been opened properly
							assertTrue(open);

							for (Tuple2<Long, IntType> in: input) {
								out.collect(new Tuple4<>(in.f0,
										window.getStart(),
										window.getEnd(),
										in.f1));
							}
						}
					})
					.addSink(new ValidatingSink(numKeys, numElementsPerKey / windowSize)).setParallelism(1);

			tryExecute(env, "Tumbling Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPreAggregatedFoldingTumblingTimeWindow() {
		final int numElementsPerKey = 3000;
		final int windowSize = 100;
		final int numKeys = 1;
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
			env.getConfig().disableSysoutLogging();

			env
					.addSource(new FailingSource(numKeys,
							numElementsPerKey,
							numElementsPerKey / 3))
					.rebalance()
					.timeWindowAll(Time.of(windowSize, MILLISECONDS))
					.fold(new Tuple4<>(0L, 0L, 0L, new IntType(0)),
							new FoldFunction<Tuple2<Long, IntType>, Tuple4<Long, Long, Long, IntType>>() {
								@Override
								public Tuple4<Long, Long, Long, IntType> fold(Tuple4<Long, Long, Long, IntType> accumulator,
										Tuple2<Long, IntType> value) throws Exception {
									accumulator.f0 = value.f0;
									accumulator.f3 = new IntType(accumulator.f3.value + value.f1.value);
									return accumulator;
								}
							},
							new RichAllWindowFunction<Tuple4<Long, Long, Long, IntType>, Tuple4<Long, Long, Long, IntType>, TimeWindow>() {

								private boolean open = false;

								@Override
								public void open(Configuration parameters) {
									assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());
									open = true;
								}

								@Override
								public void apply(
										TimeWindow window,
										Iterable<Tuple4<Long, Long, Long, IntType>> input,
										Collector<Tuple4<Long, Long, Long, IntType>> out) {

									// validate that the function has been opened properly
									assertTrue(open);

									for (Tuple4<Long, Long, Long, IntType> in: input) {
										out.collect(new Tuple4<>(in.f0,
												window.getStart(),
												window.getEnd(),
												in.f3));
									}
								}
							})
					.addSink(new ValidatingSink(numKeys, numElementsPerKey / windowSize)).setParallelism(1);

			tryExecute(env, "Tumbling Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPreAggregatedSlidingTimeWindow() {
		final int numElementsPerKey = 3000;
		final int windowSize = 1000;
		final int windowSlide = 100;
		final int numKeys = 1;
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
			env.getConfig().disableSysoutLogging();

			env
					.addSource(new FailingSource(numKeys,
							numElementsPerKey,
							numElementsPerKey / 3))
					.rebalance()
					.timeWindowAll(Time.of(windowSize, MILLISECONDS),
							Time.of(windowSlide, MILLISECONDS))
					.reduce(
							new ReduceFunction<Tuple2<Long, IntType>>() {

								@Override
								public Tuple2<Long, IntType> reduce(
										Tuple2<Long, IntType> a,
										Tuple2<Long, IntType> b) {

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
								Iterable<Tuple2<Long, IntType>> input,
								Collector<Tuple4<Long, Long, Long, IntType>> out) {

							// validate that the function has been opened properly
							assertTrue(open);

							for (Tuple2<Long, IntType> in: input) {
								out.collect(new Tuple4<>(in.f0,
										window.getStart(),
										window.getEnd(),
										in.f1));
							}
						}
					})
					.addSink(new ValidatingSink(numKeys, numElementsPerKey / windowSlide)).setParallelism(1);

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

	private static class FailingSource extends RichSourceFunction<Tuple2<Long, IntType>>
			implements ListCheckpointed<Integer>, CheckpointListener {
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
						(failedBefore || numElementsEmitted <= failureAfterNumElements)) {
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

		public static void reset() {
			failedBefore = false;
		}

		@Override
		public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(this.numElementsEmitted);
		}

		@Override
		public void restoreState(List<Integer> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}
			this.numElementsEmitted = state.get(0);
		}
	}

	private static class ValidatingSink extends RichSinkFunction<Tuple4<Long, Long, Long, IntType>>
			implements ListCheckpointed<HashMap<Long, Integer>> {

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
		public List<HashMap<Long, Integer>> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(this.windowCounts);
		}

		@Override
		public void restoreState(List<HashMap<Long, Integer>> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}
			this.windowCounts.putAll(state.get(0));
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Custom boxed integer type.
	 */
	public static class IntType {

		public int value;

		public IntType() {}

		public IntType(int value) {
			this.value = value;
		}
	}
}
