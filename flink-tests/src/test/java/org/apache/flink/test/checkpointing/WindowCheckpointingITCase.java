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
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.checkpointing.utils.FailingSource;
import org.apache.flink.test.checkpointing.utils.IntType;
import org.apache.flink.test.checkpointing.utils.ValidatingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

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
	public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE = new MiniClusterWithClientResource(
		new MiniClusterResourceConfiguration.Builder()
			.setConfiguration(getConfiguration())
			.setNumberTaskManagers(2)
			.setNumberSlotsPerTaskManager(PARALLELISM / 2)
			.build());

	private static Configuration getConfiguration() {
		Configuration config = new Configuration();
		config.set(TaskManagerOptions.MANAGED_MEMORY_SIZE, MemorySize.parse("48m"));
		return config;
	}

	// ------------------------------------------------------------------------

	@Test
	public void testTumblingProcessingTimeWindow() {
		final int numElements = 3000;

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(timeCharacteristic);
			env.getConfig().setAutoWatermarkInterval(10);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));

			SinkValidatorUpdaterAndChecker updaterAndChecker =
				new SinkValidatorUpdaterAndChecker(numElements, 1);

			env
					.addSource(new FailingSource(new Generator(), numElements, timeCharacteristic))
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
								out.collect(new Tuple2<>(value.f0, new IntType(1)));
							}
						}
					})
				.addSink(new ValidatingSink<>(updaterAndChecker, updaterAndChecker, timeCharacteristic))
				.setParallelism(1);

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

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(timeCharacteristic);
			env.getConfig().setAutoWatermarkInterval(10);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
						SinkValidatorUpdaterAndChecker updaterAndChecker =
				new SinkValidatorUpdaterAndChecker(numElements, 3);
			env
					.addSource(new FailingSource(new Generator(), numElements, timeCharacteristic))
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
								out.collect(new Tuple2<>(value.f0, new IntType(1)));
							}
						}
					})
				.addSink(new ValidatingSink<>(updaterAndChecker, updaterAndChecker, timeCharacteristic))
				.setParallelism(1);

			tryExecute(env, "Sliding Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testAggregatingTumblingProcessingTimeWindow() {
		final int numElements = 3000;

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(timeCharacteristic);
			env.getConfig().setAutoWatermarkInterval(10);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
						SinkValidatorUpdaterAndChecker updaterAndChecker =
				new SinkValidatorUpdaterAndChecker(numElements, 1);
			env
					.addSource(new FailingSource(new Generator(), numElements, timeCharacteristic))
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
				.addSink(new ValidatingSink<>(updaterAndChecker, updaterAndChecker, timeCharacteristic))
				.setParallelism(1);

			tryExecute(env, "Aggregating Tumbling Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testAggregatingSlidingProcessingTimeWindow() {
		final int numElements = 3000;

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(timeCharacteristic);
			env.getConfig().setAutoWatermarkInterval(10);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
						SinkValidatorUpdaterAndChecker updaterAndChecker =
				new SinkValidatorUpdaterAndChecker(numElements, 3);
			env
					.addSource(new FailingSource(new Generator(), numElements, timeCharacteristic))
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
				.addSink(new ValidatingSink<>(updaterAndChecker, updaterAndChecker, timeCharacteristic))
				.setParallelism(1);

			tryExecute(env, "Aggregating Sliding Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	static class Generator implements FailingSource.EventEmittingGenerator {

		@Override
		public void emitEvent(SourceFunction.SourceContext<Tuple2<Long, IntType>> ctx, int eventSequenceNo) {
			ctx.collect(new Tuple2<>((long) eventSequenceNo, new IntType(eventSequenceNo)));
		}
	}

	static class SinkValidatorUpdaterAndChecker
		implements ValidatingSink.CountUpdater<Tuple2<Long, IntType>>, ValidatingSink.ResultChecker {

		private final int elementCountExpected;
		private final int countPerElementExpected;

		SinkValidatorUpdaterAndChecker(int elementCountExpected, int countPerElementExpected) {
			this.elementCountExpected = elementCountExpected;
			this.countPerElementExpected = countPerElementExpected;
		}

		@Override
		public void updateCount(Tuple2<Long, IntType> value, Map<Long, Integer> windowCounts) {
			windowCounts.merge(value.f0, value.f1.value, (a, b) -> a + b);
		}

		@Override
		public boolean checkResult(Map<Long, Integer> windowCounts) {
			int aggCount = 0;

			for (Integer i : windowCounts.values()) {
				aggCount += i;
			}

			if (aggCount < elementCountExpected * countPerElementExpected
				|| elementCountExpected != windowCounts.size()) {
				return false;
			}

			for (int i : windowCounts.values()) {
				if (countPerElementExpected != i) {
					return false;
				}
			}

			return true;
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
}
