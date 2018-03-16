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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.test.util.TestUtils.tryExecute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test uses a custom non-serializable data type to to ensure that state
 * serializability is handled correctly.
 */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class WindowCheckpointingITCase extends TestLogger {

	private TimeCharacteristic timeCharacteristic;

	public WindowCheckpointingITCase(TimeCharacteristic timeCharacteristic) {
		this.timeCharacteristic = timeCharacteristic;
	}

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
		return config;
	}

	// ------------------------------------------------------------------------

	@Test
	public void testTumblingProcessingTimeWindow() {
		final int numElements = 3000;
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(timeCharacteristic);
			env.getConfig().setAutoWatermarkInterval(10);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
			env.getConfig().disableSysoutLogging();

			env
					.addSource(new FailingSource(numElements, numElements / 3))
					.rebalance()
					.keyBy(0)
					.timeWindow(Time.of(100, MILLISECONDS))
					.apply(new RichWindowFunction<Tuple2<Long, IntType>, Tuple2<Long, IntType>, Tuple, TimeWindow>() {

						private boolean open = false;

						@Override
						public void open(Configuration parameters) {
							assertEquals(PARALLELISM, getRuntimeContext().getNumberOfParallelSubtasks());
							open = true;
						}

						@Override
						public void apply(
								Tuple tuple,
								TimeWindow window,
								Iterable<Tuple2<Long, IntType>> values,
								Collector<Tuple2<Long, IntType>> out) {

							// validate that the function has been opened properly
							assertTrue(open);

							for (Tuple2<Long, IntType> value : values) {
								assertEquals(value.f0.intValue(), value.f1.value);
								out.collect(new Tuple2<Long, IntType>(value.f0, new IntType(1)));
							}
						}
					})
					.addSink(new ValidatingSink(numElements, 1)).setParallelism(1);

			tryExecute(env, "Tumbling Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSlidingProcessingTimeWindow() {
		final int numElements = 3000;
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(timeCharacteristic);
			env.getConfig().setAutoWatermarkInterval(10);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
			env.getConfig().disableSysoutLogging();

			env
					.addSource(new FailingSource(numElements, numElements / 3))
					.rebalance()
					.keyBy(0)
					.timeWindow(Time.of(150, MILLISECONDS), Time.of(50, MILLISECONDS))
					.apply(new RichWindowFunction<Tuple2<Long, IntType>, Tuple2<Long, IntType>, Tuple, TimeWindow>() {

						private boolean open = false;

						@Override
						public void open(Configuration parameters) {
							assertEquals(PARALLELISM, getRuntimeContext().getNumberOfParallelSubtasks());
							open = true;
						}

						@Override
						public void apply(
								Tuple tuple,
								TimeWindow window,
								Iterable<Tuple2<Long, IntType>> values,
								Collector<Tuple2<Long, IntType>> out) {

							// validate that the function has been opened properly
							assertTrue(open);

							for (Tuple2<Long, IntType> value : values) {
								assertEquals(value.f0.intValue(), value.f1.value);
								out.collect(new Tuple2<Long, IntType>(value.f0, new IntType(1)));
							}
						}
					})
					.addSink(new ValidatingSink(numElements, 3)).setParallelism(1);

			tryExecute(env, "Tumbling Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testAggregatingTumblingProcessingTimeWindow() {
		final int numElements = 3000;
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(timeCharacteristic);
			env.getConfig().setAutoWatermarkInterval(10);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
			env.getConfig().disableSysoutLogging();

			env
					.addSource(new FailingSource(numElements, numElements / 3))
					.map(new MapFunction<Tuple2<Long, IntType>, Tuple2<Long, IntType>>() {
						@Override
						public Tuple2<Long, IntType> map(Tuple2<Long, IntType> value) {
							value.f1.value = 1;
							return value;
						}
					})
					.rebalance()
					.keyBy(0)
					.timeWindow(Time.of(100, MILLISECONDS))
					.reduce(new ReduceFunction<Tuple2<Long, IntType>>() {

						@Override
						public Tuple2<Long, IntType> reduce(
								Tuple2<Long, IntType> a,
								Tuple2<Long, IntType> b) {
							return new Tuple2<>(a.f0, new IntType(1));
						}
					})
					.addSink(new ValidatingSink(numElements, 1)).setParallelism(1);

			tryExecute(env, "Tumbling Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testAggregatingSlidingProcessingTimeWindow() {
		final int numElements = 3000;
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(timeCharacteristic);
			env.getConfig().setAutoWatermarkInterval(10);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
			env.getConfig().disableSysoutLogging();

			env
					.addSource(new FailingSource(numElements, numElements / 3))
					.map(new MapFunction<Tuple2<Long, IntType>, Tuple2<Long, IntType>>() {
						@Override
						public Tuple2<Long, IntType> map(Tuple2<Long, IntType> value) {
							value.f1.value = 1;
							return value;
						}
					})
					.rebalance()
					.keyBy(0)
					.timeWindow(Time.of(150, MILLISECONDS), Time.of(50, MILLISECONDS))
					.reduce(new ReduceFunction<Tuple2<Long, IntType>>() {
						@Override
						public Tuple2<Long, IntType> reduce(
								Tuple2<Long, IntType> a,
								Tuple2<Long, IntType> b) {
							return new Tuple2<>(a.f0, new IntType(1));
						}
					})
					.addSink(new ValidatingSink(numElements, 3)).setParallelism(1);

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

		private final int numElementsToEmit;
		private final int failureAfterNumElements;

		private volatile int numElementsEmitted;
		private volatile int numSuccessfulCheckpoints;
		private volatile boolean running = true;

		private FailingSource(int numElementsToEmit, int failureAfterNumElements) {
			this.numElementsToEmit = numElementsToEmit;
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
						ctx.collect(new Tuple2<Long, IntType>((long) next, new IntType(next)));
					}
				} else {
					// if our work is done, delay a bit to prevent busy waiting
					Thread.sleep(10);
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

		public static void reset() {
			failedBefore = false;
		}
	}

	private static class ValidatingSink extends RichSinkFunction<Tuple2<Long, IntType>>
			implements ListCheckpointed<HashMap<Long, Integer>> {

		private final HashMap<Long, Integer> counts = new HashMap<>();

		private final int elementCountExpected;
		private final int countPerElementExpected;

		private int aggCount;

		private ValidatingSink(int elementCountExpected, int countPerElementExpected) {
			this.elementCountExpected = elementCountExpected;
			this.countPerElementExpected = countPerElementExpected;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			// this sink can only work with DOP 1
			assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());
			checkSuccess();
		}

		@Override
		public void invoke(Tuple2<Long, IntType> value) throws Exception {
			Integer curr = counts.get(value.f0);
			if (curr != null) {
				counts.put(value.f0, curr + value.f1.value);
			}
			else {
				counts.put(value.f0, value.f1.value);
			}

			// check if we have seen all we expect
			aggCount += value.f1.value;
			checkSuccess();
		}

		private void checkSuccess() throws SuccessException {
			if (aggCount >= elementCountExpected * countPerElementExpected) {
				// we are done. validate
				assertEquals(elementCountExpected, counts.size());

				for (Integer i : counts.values()) {
					assertEquals(countPerElementExpected, i.intValue());
				}

				// exit
				throw new SuccessException();
			}
		}

		@Override
		public List<HashMap<Long, Integer>> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(this.counts);
		}

		@Override
		public void restoreState(List<HashMap<Long, Integer>> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}
			this.counts.putAll(state.get(0));

			for (Integer i : state.get(0).values()) {
				this.aggCount += i;
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Parametrization for testing different time characteristics
	// ------------------------------------------------------------------------

	@Parameterized.Parameters(name = "TimeCharacteristic = {0}")
	@SuppressWarnings("unchecked,rawtypes")
	public static Collection<TimeCharacteristic[]> timeCharacteristic(){
		return Arrays.asList(new TimeCharacteristic[]{TimeCharacteristic.ProcessingTime},
				new TimeCharacteristic[]{TimeCharacteristic.IngestionTime}
		);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * POJO with int value.
	 */
	public static class IntType {

		public int value;

		public IntType() {}

		public IntType(int value) {
			this.value = value;
		}
	}
}
