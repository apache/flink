/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing.utils;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * This verifies that we can restore a complete job from a Flink 1.2 savepoint.
 *
 * <p>The test for checkpointed (legacy state) was removed from this test for Flink 1.4 because compatibility with
 * Flink 1.1 is removed. The legacy state in the binary savepoints is ignored by the tests now.
 *
 * <p>The tests will time out if they don't see the required number of successful checks within
 * a time limit.
 *
 *
 */
public class StatefulJobSavepointFrom12MigrationITCase extends SavepointMigrationTestBase {
	private static final int NUM_SOURCE_ELEMENTS = 4;

	/**
	 * This has to be manually executed to create the savepoint on Flink 1.2.
	 */
	@Test
	@Ignore
	public void testCreateSavepointOnFlink12() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setStateBackend(new MemoryStateBackend());
		env.enableCheckpointing(500);
		env.setParallelism(4);
		env.setMaxParallelism(4);

		env
				.addSource(new LegacyCheckpointedSource(NUM_SOURCE_ELEMENTS)).setMaxParallelism(1).uid("LegacyCheckpointedSource")
				.flatMap(new LegacyCheckpointedFlatMap()).startNewChain().uid("LegacyCheckpointedFlatMap")
				.keyBy(0)
				.flatMap(new LegacyCheckpointedFlatMapWithKeyedState()).startNewChain().uid("LegacyCheckpointedFlatMapWithKeyedState")
				.keyBy(0)
				.flatMap(new KeyedStateSettingFlatMap()).startNewChain().uid("KeyedStateSettingFlatMap")
				.keyBy(0)
				.transform(
						"custom_operator",
						new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
						new CheckpointedUdfOperator(new LegacyCheckpointedFlatMapWithKeyedState())).uid("LegacyCheckpointedOperator")
				.keyBy(0)
				.transform(
						"timely_stateful_operator",
						new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
						new TimelyStatefulOperator()).uid("TimelyStatefulOperator")
				.addSink(new AccumulatorCountingSink<Tuple2<Long, Long>>());

		executeAndSavepoint(
				env,
				"src/test/resources/" + getSavepointPath(),
				new Tuple2<>(AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR, NUM_SOURCE_ELEMENTS));
	}

	/**
	 * This has to be manually executed to create the savepoint on Flink 1.2.
	 */
	@Test
	@Ignore
	public void testCreateSavepointOnFlink12WithRocksDB() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		RocksDBStateBackend rocksBackend =
				new RocksDBStateBackend(new MemoryStateBackend());
		env.setStateBackend(rocksBackend);
		env.enableCheckpointing(500);
		env.setParallelism(4);
		env.setMaxParallelism(4);

		env
				.addSource(new LegacyCheckpointedSource(NUM_SOURCE_ELEMENTS)).setMaxParallelism(1).uid("LegacyCheckpointedSource")
				.flatMap(new LegacyCheckpointedFlatMap()).startNewChain().uid("LegacyCheckpointedFlatMap")
				.keyBy(0)
				.flatMap(new LegacyCheckpointedFlatMapWithKeyedState()).startNewChain().uid("LegacyCheckpointedFlatMapWithKeyedState")
				.keyBy(0)
				.flatMap(new KeyedStateSettingFlatMap()).startNewChain().uid("KeyedStateSettingFlatMap")
				.keyBy(0)
				.transform(
						"custom_operator",
						new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
						new CheckpointedUdfOperator(new LegacyCheckpointedFlatMapWithKeyedState())).uid("LegacyCheckpointedOperator")
				.keyBy(0)
				.transform(
						"timely_stateful_operator",
						new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
						new TimelyStatefulOperator()).uid("TimelyStatefulOperator")
				.addSink(new AccumulatorCountingSink<Tuple2<Long, Long>>());

		executeAndSavepoint(
				env,
				"src/test/resources/" + getRocksDBSavepointPath(),
				new Tuple2<>(AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR, NUM_SOURCE_ELEMENTS));
	}

	@Test
	public void testSavepointRestoreFromFlink12() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setStateBackend(new MemoryStateBackend());
		env.enableCheckpointing(500);
		env.setParallelism(4);
		env.setMaxParallelism(4);

		env
				.addSource(new CheckingRestoringSource(NUM_SOURCE_ELEMENTS)).setMaxParallelism(1).uid("LegacyCheckpointedSource")
				.flatMap(new CheckingRestoringFlatMap()).startNewChain().uid("LegacyCheckpointedFlatMap")
				.keyBy(0)
				.flatMap(new CheckingRestoringFlatMapWithKeyedState()).startNewChain().uid("LegacyCheckpointedFlatMapWithKeyedState")
				.keyBy(0)
				.flatMap(new CheckingKeyedStateFlatMap()).startNewChain().uid("KeyedStateSettingFlatMap")
				.keyBy(0)
				.transform(
						"custom_operator",
						new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
						new CheckingRestoringUdfOperator(new CheckingRestoringFlatMapWithKeyedStateInOperator())).uid("LegacyCheckpointedOperator")
				.keyBy(0)
				.transform(
						"timely_stateful_operator",
						new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
						new CheckingTimelyStatefulOperator()).uid("TimelyStatefulOperator")
				.addSink(new AccumulatorCountingSink<Tuple2<Long, Long>>());

		restoreAndExecute(
				env,
				getResourceFilename(getSavepointPath()),
				new Tuple2<>(CheckingRestoringSource.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, 1),
				new Tuple2<>(CheckingRestoringFlatMap.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(CheckingRestoringFlatMapWithKeyedState.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(CheckingKeyedStateFlatMap.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(CheckingRestoringUdfOperator.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(CheckingRestoringFlatMapWithKeyedStateInOperator.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(CheckingTimelyStatefulOperator.SUCCESSFUL_PROCESS_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(CheckingTimelyStatefulOperator.SUCCESSFUL_EVENT_TIME_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(CheckingTimelyStatefulOperator.SUCCESSFUL_PROCESSING_TIME_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR, NUM_SOURCE_ELEMENTS));
	}

	@Test
	public void testSavepointRestoreFromFlink12FromRocksDB() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setStateBackend(new RocksDBStateBackend(new MemoryStateBackend()));
		env.enableCheckpointing(500);
		env.setParallelism(4);
		env.setMaxParallelism(4);

		env
				.addSource(new CheckingRestoringSource(NUM_SOURCE_ELEMENTS)).setMaxParallelism(1).uid("LegacyCheckpointedSource")
				.flatMap(new CheckingRestoringFlatMap()).startNewChain().uid("LegacyCheckpointedFlatMap")
				.keyBy(0)
				.flatMap(new CheckingRestoringFlatMapWithKeyedState()).startNewChain().uid("LegacyCheckpointedFlatMapWithKeyedState")
				.keyBy(0)
				.flatMap(new CheckingKeyedStateFlatMap()).startNewChain().uid("KeyedStateSettingFlatMap")
				.keyBy(0)
				.transform(
						"custom_operator",
						new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
						new CheckingRestoringUdfOperator(new CheckingRestoringFlatMapWithKeyedStateInOperator())).uid("LegacyCheckpointedOperator")
				.keyBy(0)
				.transform(
						"timely_stateful_operator",
						new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
						new CheckingTimelyStatefulOperator()).uid("TimelyStatefulOperator")
				.addSink(new AccumulatorCountingSink<Tuple2<Long, Long>>());

		restoreAndExecute(
				env,
				getResourceFilename(getRocksDBSavepointPath()),
				new Tuple2<>(CheckingRestoringSource.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, 1),
				new Tuple2<>(CheckingRestoringFlatMap.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(CheckingRestoringFlatMapWithKeyedState.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(CheckingKeyedStateFlatMap.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(CheckingRestoringUdfOperator.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(CheckingRestoringFlatMapWithKeyedStateInOperator.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(CheckingTimelyStatefulOperator.SUCCESSFUL_PROCESS_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(CheckingTimelyStatefulOperator.SUCCESSFUL_EVENT_TIME_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(CheckingTimelyStatefulOperator.SUCCESSFUL_PROCESSING_TIME_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR, NUM_SOURCE_ELEMENTS));
	}

	protected String getSavepointPath() {
		return "stateful-udf-migration-itcase-flink1.2-savepoint";
	}

	protected String getRocksDBSavepointPath() {
		return "stateful-udf-migration-itcase-flink1.2-rocksdb-savepoint";
	}

	private static class LegacyCheckpointedSource
			implements SourceFunction<Tuple2<Long, Long>> {

		public static String checkpointedString = "Here be dragons!";

		private static final long serialVersionUID = 1L;

		private volatile boolean isRunning = true;

		private final int numElements;

		public LegacyCheckpointedSource(int numElements) {
			this.numElements = numElements;
		}

		@Override
		public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {

			ctx.emitWatermark(new Watermark(0));

			synchronized (ctx.getCheckpointLock()) {
				for (long i = 0; i < numElements; i++) {
					ctx.collect(new Tuple2<>(i, i));
				}
			}

			// don't emit a final watermark so that we don't trigger the registered event-time
			// timers
			while (isRunning) {
				Thread.sleep(20);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	private static class CheckingRestoringSource
			extends RichSourceFunction<Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		public static final String SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR = CheckingRestoringSource.class + "_RESTORE_CHECK";

		private volatile boolean isRunning = true;

		private final int numElements;

		private String restoredState;

		public CheckingRestoringSource(int numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			getRuntimeContext().addAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, new IntCounter());
		}

		@Override
		public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
			getRuntimeContext().getAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR).add(1);

			// immediately trigger any set timers
			ctx.emitWatermark(new Watermark(1000));

			synchronized (ctx.getCheckpointLock()) {
				for (long i = 0; i < numElements; i++) {
					ctx.collect(new Tuple2<>(i, i));
				}
			}

			while (isRunning) {
				Thread.sleep(20);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	private static class LegacyCheckpointedFlatMap extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		public static Tuple2<String, Long> checkpointedTuple =
				new Tuple2<>("hello", 42L);

		@Override
		public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);
		}
	}

	private static class CheckingRestoringFlatMap extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		public static final String SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR = CheckingRestoringFlatMap.class + "_RESTORE_CHECK";

		private transient Tuple2<String, Long> restoredState;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			getRuntimeContext().addAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, new IntCounter());
		}

		@Override
		public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);

			getRuntimeContext().getAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR).add(1);

		}

	}

	private static class LegacyCheckpointedFlatMapWithKeyedState
			extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		public static Tuple2<String, Long> checkpointedTuple =
				new Tuple2<>("hello", 42L);

		private final ValueStateDescriptor<Long> stateDescriptor =
				new ValueStateDescriptor<Long>("state-name", LongSerializer.INSTANCE);

		@Override
		public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);

			getRuntimeContext().getState(stateDescriptor).update(value.f1);

			assertEquals(value.f1, getRuntimeContext().getState(stateDescriptor).value());
		}
	}

	private static class CheckingRestoringFlatMapWithKeyedState
		extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		public static final String SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR = CheckingRestoringFlatMapWithKeyedState.class + "_RESTORE_CHECK";

		private transient Tuple2<String, Long> restoredState;

		private final ValueStateDescriptor<Long> stateDescriptor =
				new ValueStateDescriptor<Long>("state-name", LongSerializer.INSTANCE);

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			getRuntimeContext().addAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, new IntCounter());
		}

		@Override
		public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);

			ValueState<Long> state = getRuntimeContext().getState(stateDescriptor);
			if (state == null) {
				throw new RuntimeException("Missing key value state for " + value);
			}

			assertEquals(value.f1, state.value());
			getRuntimeContext().getAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR).add(1);
		}
	}

	private static class CheckingRestoringFlatMapWithKeyedStateInOperator
		extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		public static final String SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR = CheckingRestoringFlatMapWithKeyedStateInOperator.class + "_RESTORE_CHECK";

		private transient Tuple2<String, Long> restoredState;

		private final ValueStateDescriptor<Long> stateDescriptor =
				new ValueStateDescriptor<Long>("state-name", LongSerializer.INSTANCE);

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			getRuntimeContext().addAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, new IntCounter());
		}

		@Override
		public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);

			ValueState<Long> state = getRuntimeContext().getState(stateDescriptor);
			if (state == null) {
				throw new RuntimeException("Missing key value state for " + value);
			}

			assertEquals(value.f1, state.value());
			getRuntimeContext().getAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR).add(1);
		}
	}

	private static class KeyedStateSettingFlatMap extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		private final ValueStateDescriptor<Long> stateDescriptor =
				new ValueStateDescriptor<Long>("state-name", LongSerializer.INSTANCE);

		@Override
		public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);

			getRuntimeContext().getState(stateDescriptor).update(value.f1);
		}
	}

	private static class CheckingKeyedStateFlatMap extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		public static final String SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR = CheckingKeyedStateFlatMap.class + "_RESTORE_CHECK";

		private final ValueStateDescriptor<Long> stateDescriptor =
				new ValueStateDescriptor<Long>("state-name", LongSerializer.INSTANCE);

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			getRuntimeContext().addAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, new IntCounter());
		}

		@Override
		public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);

			ValueState<Long> state = getRuntimeContext().getState(stateDescriptor);
			if (state == null) {
				throw new RuntimeException("Missing key value state for " + value);
			}

			assertEquals(value.f1, state.value());
			getRuntimeContext().getAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR).add(1);
		}
	}

	private static class CheckpointedUdfOperator
			extends AbstractUdfStreamOperator<Tuple2<Long, Long>, FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>>
			implements OneInputStreamOperator<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		private static final long serialVersionUID = 1L;

		private static final String CHECKPOINTED_STRING = "Oh my, that's nice!";

		public CheckpointedUdfOperator(FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> userFunction) {
			super(userFunction);
		}

		@Override
		public void processElement(StreamRecord<Tuple2<Long, Long>> element) throws Exception {
			userFunction.flatMap(element.getValue(), new TimestampedCollector<>(output));
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			output.emitWatermark(mark);
		}
	}

	private static class CheckingRestoringUdfOperator
			extends AbstractUdfStreamOperator<Tuple2<Long, Long>, FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>>
			implements OneInputStreamOperator<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		public static final String SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR = CheckingRestoringUdfOperator.class + "_RESTORE_CHECK";

		private String restoredState;

		public CheckingRestoringUdfOperator(FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> userFunction) {
			super(userFunction);
		}

		@Override
		public void open() throws Exception {
			super.open();

			getRuntimeContext().addAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, new IntCounter());
		}

		@Override
		public void processElement(StreamRecord<Tuple2<Long, Long>> element) throws Exception {
			userFunction.flatMap(element.getValue(), new TimestampedCollector<>(output));
			getRuntimeContext().getAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR).add(1);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			output.emitWatermark(mark);
		}
	}

	private static class TimelyStatefulOperator
			extends AbstractStreamOperator<Tuple2<Long, Long>>
			implements OneInputStreamOperator<Tuple2<Long, Long>, Tuple2<Long, Long>>, Triggerable<Long, Long> {
		private static final long serialVersionUID = 1L;

		private final ValueStateDescriptor<Long> stateDescriptor =
				new ValueStateDescriptor<Long>("state-name", LongSerializer.INSTANCE);

		private transient InternalTimerService<Long> timerService;

		@Override
		public void open() throws Exception {
			super.open();

			timerService = getInternalTimerService(
					"timer",
					LongSerializer.INSTANCE,
					this);

		}

		@Override
		public void processElement(StreamRecord<Tuple2<Long, Long>> element) throws Exception {
			ValueState<Long> state = getKeyedStateBackend().getPartitionedState(
					element.getValue().f0,
					LongSerializer.INSTANCE,
					stateDescriptor);

			state.update(element.getValue().f1);

			timerService.registerEventTimeTimer(element.getValue().f0, timerService.currentWatermark() + 10);
			timerService.registerProcessingTimeTimer(element.getValue().f0, timerService.currentProcessingTime() + 30_000);

			output.collect(element);
		}

		@Override
		public void onEventTime(InternalTimer<Long, Long> timer) throws Exception {

		}

		@Override
		public void onProcessingTime(InternalTimer<Long, Long> timer) throws Exception {

		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			output.emitWatermark(mark);
		}
	}

	private static class CheckingTimelyStatefulOperator
			extends AbstractStreamOperator<Tuple2<Long, Long>>
			implements OneInputStreamOperator<Tuple2<Long, Long>, Tuple2<Long, Long>>, Triggerable<Long, Long> {
		private static final long serialVersionUID = 1L;

		public static final String SUCCESSFUL_PROCESS_CHECK_ACCUMULATOR = CheckingTimelyStatefulOperator.class + "_PROCESS_CHECKS";
		public static final String SUCCESSFUL_EVENT_TIME_CHECK_ACCUMULATOR = CheckingTimelyStatefulOperator.class + "_ET_CHECKS";
		public static final String SUCCESSFUL_PROCESSING_TIME_CHECK_ACCUMULATOR = CheckingTimelyStatefulOperator.class + "_PT_CHECKS";

		private final ValueStateDescriptor<Long> stateDescriptor =
				new ValueStateDescriptor<Long>("state-name", LongSerializer.INSTANCE);

		private transient InternalTimerService<Long> timerService;

		@Override
		public void open() throws Exception {
			super.open();

			timerService = getInternalTimerService(
					"timer",
					LongSerializer.INSTANCE,
					this);

			getRuntimeContext().addAccumulator(SUCCESSFUL_PROCESS_CHECK_ACCUMULATOR, new IntCounter());
			getRuntimeContext().addAccumulator(SUCCESSFUL_EVENT_TIME_CHECK_ACCUMULATOR, new IntCounter());
			getRuntimeContext().addAccumulator(SUCCESSFUL_PROCESSING_TIME_CHECK_ACCUMULATOR, new IntCounter());
		}

		@Override
		public void processElement(StreamRecord<Tuple2<Long, Long>> element) throws Exception {
			ValueState<Long> state = getKeyedStateBackend().getPartitionedState(
					element.getValue().f0,
					LongSerializer.INSTANCE,
					stateDescriptor);

			assertEquals(state.value(), element.getValue().f1);
			getRuntimeContext().getAccumulator(SUCCESSFUL_PROCESS_CHECK_ACCUMULATOR).add(1);

			output.collect(element);
		}

		@Override
		public void onEventTime(InternalTimer<Long, Long> timer) throws Exception {
			ValueState<Long> state = getKeyedStateBackend().getPartitionedState(
					timer.getNamespace(),
					LongSerializer.INSTANCE,
					stateDescriptor);

			assertEquals(state.value(), timer.getNamespace());
			getRuntimeContext().getAccumulator(SUCCESSFUL_EVENT_TIME_CHECK_ACCUMULATOR).add(1);
		}

		@Override
		public void onProcessingTime(InternalTimer<Long, Long> timer) throws Exception {
			ValueState<Long> state = getKeyedStateBackend().getPartitionedState(
					timer.getNamespace(),
					LongSerializer.INSTANCE,
					stateDescriptor);

			assertEquals(state.value(), timer.getNamespace());
			getRuntimeContext().getAccumulator(SUCCESSFUL_PROCESSING_TIME_CHECK_ACCUMULATOR).add(1);
		}
	}

	private static class AccumulatorCountingSink<T> extends RichSinkFunction<T> {
		private static final long serialVersionUID = 1L;

		public static final String NUM_ELEMENTS_ACCUMULATOR = AccumulatorCountingSink.class + "_NUM_ELEMENTS";

		int count = 0;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			getRuntimeContext().addAccumulator(NUM_ELEMENTS_ACCUMULATOR, new IntCounter());
		}

		@Override
		public void invoke(T value) throws Exception {
			count++;
			getRuntimeContext().getAccumulator(NUM_ELEMENTS_ACCUMULATOR).add(1);
		}
	}
}
