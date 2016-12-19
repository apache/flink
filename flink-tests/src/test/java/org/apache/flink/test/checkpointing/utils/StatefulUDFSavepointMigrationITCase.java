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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.checkpoint.CheckpointedRestoring;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * This verifies that we can restore a complete job from a Flink 1.1 savepoint.
 *
 * <p>The test pipeline contains both "Checkpointed" state and keyed user state.
 */
public class StatefulUDFSavepointMigrationITCase extends SavepointMigrationTestBase {
	private static final int NUM_SOURCE_ELEMENTS = 4;
	private static final String EXPECTED_ELEMENTS_ACCUMULATOR = "NUM_EXPECTED_ELEMENTS";
	private static final String SUCCESSFUL_CHECK_ACCUMULATOR = "SUCCESSFUL_CHECKS";

	/**
	 * This has to be manually executed to create the savepoint on Flink 1.1.
	 */
	@Test
	@Ignore
	public void testCreateSavepointOnFlink11() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// we only test memory state backend yet
		env.setStateBackend(new MemoryStateBackend());
		env.enableCheckpointing(500);
		env.setParallelism(4);
		env.setMaxParallelism(4);

		// create source
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
				.addSink(new AccumulatorCountingSink<Tuple2<Long, Long>>(EXPECTED_ELEMENTS_ACCUMULATOR));

		executeAndSavepoint(
				env,
				"src/test/resources/stateful-udf-migration-itcase-flink1.1-savepoint",
				new Tuple2<>(EXPECTED_ELEMENTS_ACCUMULATOR, NUM_SOURCE_ELEMENTS));
	}

	/**
	 * This has to be manually executed to create the savepoint on Flink 1.1.
	 */
	@Test
	@Ignore
	public void testCreateSavepointOnFlink11WithRocksDB() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		RocksDBStateBackend rocksBackend =
				new RocksDBStateBackend(new MemoryStateBackend());
//		rocksBackend.enableFullyAsyncSnapshots();
		env.setStateBackend(rocksBackend);
		env.enableCheckpointing(500);
		env.setParallelism(4);
		env.setMaxParallelism(4);

		// create source
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
				.addSink(new AccumulatorCountingSink<Tuple2<Long, Long>>(EXPECTED_ELEMENTS_ACCUMULATOR));

		executeAndSavepoint(
				env,
				"src/test/resources/stateful-udf-migration-itcase-flink1.1-savepoint-rocksdb",
				new Tuple2<>(EXPECTED_ELEMENTS_ACCUMULATOR, NUM_SOURCE_ELEMENTS));
	}


	@Test
	public void testSavepointRestoreFromFlink11() throws Exception {

		final int EXPECTED_SUCCESSFUL_CHECKS = 21;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// we only test memory state backend yet
		env.setStateBackend(new MemoryStateBackend());
		env.enableCheckpointing(500);
		env.setParallelism(4);
		env.setMaxParallelism(4);

		// create source
		env
				.addSource(new RestoringCheckingSource(NUM_SOURCE_ELEMENTS)).setMaxParallelism(1).uid("LegacyCheckpointedSource")
				.flatMap(new RestoringCheckingFlatMap()).startNewChain().uid("LegacyCheckpointedFlatMap")
				.keyBy(0)
				.flatMap(new RestoringCheckingFlatMapWithKeyedState()).startNewChain().uid("LegacyCheckpointedFlatMapWithKeyedState")
				.keyBy(0)
				.flatMap(new KeyedStateCheckingFlatMap()).startNewChain().uid("KeyedStateSettingFlatMap")
				.keyBy(0)
				.transform(
						"custom_operator",
						new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
						new RestoringCheckingUdfOperator(new RestoringCheckingFlatMapWithKeyedState())).uid("LegacyCheckpointedOperator")
				.addSink(new AccumulatorCountingSink<Tuple2<Long, Long>>(EXPECTED_ELEMENTS_ACCUMULATOR));

		restoreAndExecute(
				env,
				getResourceFilename("stateful-udf-migration-itcase-flink1.1-savepoint"),
				new Tuple2<>(SUCCESSFUL_CHECK_ACCUMULATOR, EXPECTED_SUCCESSFUL_CHECKS));
	}

	@Test
	public void testSavepointRestoreFromFlink11FromRocksDB() throws Exception {

		final int EXPECTED_SUCCESSFUL_CHECKS = 21;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// we only test memory state backend yet
		env.setStateBackend(new RocksDBStateBackend(new MemoryStateBackend()));
		env.enableCheckpointing(500);
		env.setParallelism(4);
		env.setMaxParallelism(4);

		// create source
		env
				.addSource(new RestoringCheckingSource(NUM_SOURCE_ELEMENTS)).setMaxParallelism(1).uid("LegacyCheckpointedSource")
				.flatMap(new RestoringCheckingFlatMap()).startNewChain().uid("LegacyCheckpointedFlatMap")
				.keyBy(0)
				.flatMap(new RestoringCheckingFlatMapWithKeyedState()).startNewChain().uid("LegacyCheckpointedFlatMapWithKeyedState")
				.keyBy(0)
				.flatMap(new KeyedStateCheckingFlatMap()).startNewChain().uid("KeyedStateSettingFlatMap")
				.keyBy(0)
				.transform(
						"custom_operator",
						new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
						new RestoringCheckingUdfOperator(new RestoringCheckingFlatMapWithKeyedState())).uid("LegacyCheckpointedOperator")
				.addSink(new AccumulatorCountingSink<Tuple2<Long, Long>>(EXPECTED_ELEMENTS_ACCUMULATOR));

		restoreAndExecute(
				env,
				getResourceFilename("stateful-udf-migration-itcase-flink1.1-savepoint-rocksdb"),
				new Tuple2<>(SUCCESSFUL_CHECK_ACCUMULATOR, EXPECTED_SUCCESSFUL_CHECKS));
	}

	private static class LegacyCheckpointedSource
			implements SourceFunction<Tuple2<Long, Long>>, Checkpointed<String> {

		public static String CHECKPOINTED_STRING = "Here be dragons!";

		private static final long serialVersionUID = 1L;

		private volatile boolean isRunning = true;

		private final int numElements;

		public LegacyCheckpointedSource(int numElements) {
			this.numElements = numElements;
		}

		@Override
		public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {

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

		@Override
		public void restoreState(String state) throws Exception {
			assertEquals(CHECKPOINTED_STRING, state);
		}

		@Override
		public String snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return CHECKPOINTED_STRING;
		}
	}

	private static class RestoringCheckingSource
			extends RichSourceFunction<Tuple2<Long, Long>>
			implements CheckpointedRestoring<String> {

		private static final long serialVersionUID = 1L;

		private volatile boolean isRunning = true;

		private final int numElements;

		private String restoredState;

		public RestoringCheckingSource(int numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			getRuntimeContext().addAccumulator(SUCCESSFUL_CHECK_ACCUMULATOR, new IntCounter());
		}

		@Override
		public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
			assertEquals(LegacyCheckpointedSource.CHECKPOINTED_STRING, restoredState);
			getRuntimeContext().getAccumulator(SUCCESSFUL_CHECK_ACCUMULATOR).add(1);

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

		@Override
		public void restoreState(String state) throws Exception {
			restoredState = state;
		}
	}

	public static class LegacyCheckpointedFlatMap extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>
			implements Checkpointed<Tuple2<String, Long>> {

		private static final long serialVersionUID = 1L;

		public static Tuple2<String, Long> CHECKPOINTED_TUPLE =
				new Tuple2<>("hello", 42L);

		@Override
		public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);
		}

		@Override
		public void restoreState(Tuple2<String, Long> state) throws Exception {
		}

		@Override
		public Tuple2<String, Long> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return CHECKPOINTED_TUPLE;
		}
	}

	public static class RestoringCheckingFlatMap extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>
			implements CheckpointedRestoring<Tuple2<String, Long>> {

		private static final long serialVersionUID = 1L;

		private transient Tuple2<String, Long> restoredState;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			getRuntimeContext().addAccumulator(SUCCESSFUL_CHECK_ACCUMULATOR, new IntCounter());
		}

		@Override
		public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);

			assertEquals(LegacyCheckpointedFlatMap.CHECKPOINTED_TUPLE, restoredState);
			getRuntimeContext().getAccumulator(SUCCESSFUL_CHECK_ACCUMULATOR).add(1);

		}

		@Override
		public void restoreState(Tuple2<String, Long> state) throws Exception {
			restoredState = state;
		}
	}

	public static class LegacyCheckpointedFlatMapWithKeyedState
			extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>
			implements Checkpointed<Tuple2<String, Long>> {

		private static final long serialVersionUID = 1L;

		public static Tuple2<String, Long> CHECKPOINTED_TUPLE =
				new Tuple2<>("hello", 42L);

		private final ValueStateDescriptor<Long> stateDescriptor =
				new ValueStateDescriptor<Long>("state-name", LongSerializer.INSTANCE, null);

		@Override
		public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);

			getRuntimeContext().getState(stateDescriptor).update(value.f1);
		}

		@Override
		public void restoreState(Tuple2<String, Long> state) throws Exception {
		}

		@Override
		public Tuple2<String, Long> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return CHECKPOINTED_TUPLE;
		}
	}

	public static class RestoringCheckingFlatMapWithKeyedState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>
			implements CheckpointedRestoring<Tuple2<String, Long>> {

		private static final long serialVersionUID = 1L;

		private transient Tuple2<String, Long> restoredState;

		private final ValueStateDescriptor<Long> stateDescriptor =
				new ValueStateDescriptor<Long>("state-name", LongSerializer.INSTANCE, null);

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			getRuntimeContext().addAccumulator(SUCCESSFUL_CHECK_ACCUMULATOR, new IntCounter());
		}

		@Override
		public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);

			ValueState<Long> state = getRuntimeContext().getState(stateDescriptor);
			if (state == null) {
				throw new RuntimeException("Missing key value state for " + value);
			}

			assertEquals(value.f1, state.value());
			assertEquals(LegacyCheckpointedFlatMap.CHECKPOINTED_TUPLE, restoredState);
			getRuntimeContext().getAccumulator(SUCCESSFUL_CHECK_ACCUMULATOR).add(1);
		}

		@Override
		public void restoreState(Tuple2<String, Long> state) throws Exception {
			restoredState = state;
		}
	}

	public static class KeyedStateSettingFlatMap extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		private final ValueStateDescriptor<Long> stateDescriptor =
				new ValueStateDescriptor<Long>("state-name", LongSerializer.INSTANCE, null);

		@Override
		public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);

			getRuntimeContext().getState(stateDescriptor).update(value.f1);
		}
	}

	public static class KeyedStateCheckingFlatMap extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		private final ValueStateDescriptor<Long> stateDescriptor =
				new ValueStateDescriptor<Long>("state-name", LongSerializer.INSTANCE, null);

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			getRuntimeContext().addAccumulator(SUCCESSFUL_CHECK_ACCUMULATOR, new IntCounter());
		}

		@Override
		public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);

			ValueState<Long> state = getRuntimeContext().getState(stateDescriptor);
			if (state == null) {
				throw new RuntimeException("Missing key value state for " + value);
			}

			assertEquals(value.f1, state.value());
			getRuntimeContext().getAccumulator(SUCCESSFUL_CHECK_ACCUMULATOR).add(1);
		}
	}

	public static class CheckpointedUdfOperator
			extends AbstractUdfStreamOperator<Tuple2<Long, Long>, FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>>
			implements OneInputStreamOperator<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		private static final long serialVersionUID = 1L;

		private static final String CHECKPOINTED_STRING = "Oh my, that's nice!";

		public CheckpointedUdfOperator(FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> userFunction) {
			super(userFunction);
		}

		@Override
		public void processElement(StreamRecord<Tuple2<Long, Long>> element) throws Exception {
			output.collect(element);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			output.emitWatermark(mark);
		}

		// Flink 1.1
//		@Override
//		public StreamTaskState snapshotOperatorState(
//				long checkpointId, long timestamp) throws Exception {
//			StreamTaskState result = super.snapshotOperatorState(checkpointId, timestamp);
//
//			AbstractStateBackend.CheckpointStateOutputView out = getStateBackend().createCheckpointStateOutputView(
//					checkpointId,
//					timestamp);
//
//			out.writeUTF(CHECKPOINTED_STRING);
//
//			result.setOperatorState(out.closeAndGetHandle());
//
//			return result;
//		}
	}

	public static class RestoringCheckingUdfOperator
			extends AbstractUdfStreamOperator<Tuple2<Long, Long>, FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>>
			implements OneInputStreamOperator<Tuple2<Long, Long>, Tuple2<Long, Long>> {
		private static final long serialVersionUID = 1L;

		private String restoredState;

		public RestoringCheckingUdfOperator(FlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> userFunction) {
			super(userFunction);
		}

		@Override
		public void open() throws Exception {
			super.open();
		}

		@Override
		public void processElement(StreamRecord<Tuple2<Long, Long>> element) throws Exception {
			userFunction.flatMap(element.getValue(), new TimestampedCollector<>(output));

			assertEquals(CheckpointedUdfOperator.CHECKPOINTED_STRING, restoredState);
			getRuntimeContext().getAccumulator(SUCCESSFUL_CHECK_ACCUMULATOR).add(1);
		}

		@Override
		public void processWatermark(Watermark mark) throws Exception {
			output.emitWatermark(mark);
		}

		@Override
		public void restoreState(FSDataInputStream in) throws Exception {
			super.restoreState(in);

			DataInputViewStreamWrapper streamWrapper = new DataInputViewStreamWrapper(in);

			restoredState = streamWrapper.readUTF();
		}
	}

	public static class AccumulatorCountingSink<T> extends RichSinkFunction<T> {
		private static final long serialVersionUID = 1L;

		private final String accumulatorName;

		int count = 0;

		public AccumulatorCountingSink(String accumulatorName) {
			this.accumulatorName = accumulatorName;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			getRuntimeContext().addAccumulator(accumulatorName, new IntCounter());
		}

		@Override
		public void invoke(T value) throws Exception {
			count++;
			getRuntimeContext().getAccumulator(accumulatorName).add(1);
		}
	}
}
