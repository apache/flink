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
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.migration.MigrationVersion;
import org.apache.flink.util.Collector;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Migration ITCases for a stateful job. The tests are parameterized to cover
 * migrating for multiple previous Flink versions, as well as for different state backends.
 */
@RunWith(Parameterized.class)
public class StatefulJobSavepointMigrationITCase extends SavepointMigrationTestBase {

	private static final int NUM_SOURCE_ELEMENTS = 4;

	@Parameterized.Parameters(name = "Migrate Savepoint / Backend: {0}")
	public static Collection<Tuple2<MigrationVersion, String>> parameters () {
		return Arrays.asList(
			Tuple2.of(MigrationVersion.v1_4, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
			Tuple2.of(MigrationVersion.v1_4, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME));
	}

	/**
	 * TODO to generate savepoints for a specific Flink version / backend type,
	 * TODO change these values accordingly, e.g. to generate for 1.4 with RocksDB,
	 * TODO set as (MigrationVersion.v1_4, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME)
	 */
	private final MigrationVersion flinkGenerateSavepointVersion = MigrationVersion.v1_4;
	private final String flinkGenerateSavepointBackendType = StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME;

	private final MigrationVersion testMigrateVersion;
	private final String testStateBackend;

	public StatefulJobSavepointMigrationITCase(Tuple2<MigrationVersion, String> testMigrateVersionAndBackend) {
		this.testMigrateVersion = testMigrateVersionAndBackend.f0;
		this.testStateBackend = testMigrateVersionAndBackend.f1;
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Test
	@Ignore
	public void writeSavepoint() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		switch (flinkGenerateSavepointBackendType) {
			case StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME:
				env.setStateBackend(new RocksDBStateBackend(new MemoryStateBackend()));
				break;
			case StateBackendLoader.MEMORY_STATE_BACKEND_NAME:
				env.setStateBackend(new MemoryStateBackend());
				break;
			default:
				throw new UnsupportedOperationException();
		}

		env.enableCheckpointing(500);
		env.setParallelism(4);
		env.setMaxParallelism(4);

		env
			.addSource(new CheckpointingNonParallelSourceWithListState(NUM_SOURCE_ELEMENTS)).uid("CheckpointingSource1")
			.keyBy(0)
			.flatMap(new CheckpointingKeyedStateFlatMap()).startNewChain().uid("CheckpointingKeyedStateFlatMap1")
			.keyBy(0)
			.transform(
				"timely_stateful_operator",
				new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
				new CheckpointingTimelyStatefulOperator()).uid("CheckpointingTimelyStatefulOperator1")
			.addSink(new AccumulatorCountingSink<>());

		env
			.addSource(new CheckpointingParallelSourceWithUnionListState(NUM_SOURCE_ELEMENTS)).uid("CheckpointingSource2")
			.keyBy(0)
			.flatMap(new CheckpointingKeyedStateFlatMap()).startNewChain().uid("CheckpointingKeyedStateFlatMap2")
			.keyBy(0)
			.transform(
				"timely_stateful_operator",
				new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
				new CheckpointingTimelyStatefulOperator()).uid("CheckpointingTimelyStatefulOperator2")
			.addSink(new AccumulatorCountingSink<>());

		executeAndSavepoint(
			env,
			"src/test/resources/" + getSavepointPath(flinkGenerateSavepointVersion, flinkGenerateSavepointBackendType),
			new Tuple2<>(AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR, NUM_SOURCE_ELEMENTS * 2));
	}

	@Test
	public void testSavepointRestore() throws Exception {

		final int parallelism = 4;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		switch (testStateBackend) {
			case StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME:
				env.setStateBackend(new RocksDBStateBackend(new MemoryStateBackend()));
				break;
			case StateBackendLoader.MEMORY_STATE_BACKEND_NAME:
				env.setStateBackend(new MemoryStateBackend());
				break;
			default:
				throw new UnsupportedOperationException();
		}

		env.enableCheckpointing(500);
		env.setParallelism(parallelism);
		env.setMaxParallelism(parallelism);

		env
			.addSource(new CheckingNonParallelSourceWithListState(NUM_SOURCE_ELEMENTS)).uid("CheckpointingSource1")
			.keyBy(0)
			.flatMap(new CheckingKeyedStateFlatMap()).startNewChain().uid("CheckpointingKeyedStateFlatMap1")
			.keyBy(0)
			.transform(
				"timely_stateful_operator",
				new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
				new CheckingTimelyStatefulOperator()).uid("CheckpointingTimelyStatefulOperator1")
			.addSink(new AccumulatorCountingSink<>());

		env
			.addSource(new CheckingRestoringParallelSourceWithUnionListState(NUM_SOURCE_ELEMENTS)).uid("CheckpointingSource2")
			.keyBy(0)
			.flatMap(new CheckingKeyedStateFlatMap()).startNewChain().uid("CheckpointingKeyedStateFlatMap2")
			.keyBy(0)
			.transform(
				"timely_stateful_operator",
				new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
				new CheckingTimelyStatefulOperator()).uid("CheckpointingTimelyStatefulOperator2")
			.addSink(new AccumulatorCountingSink<>());

		restoreAndExecute(
			env,
			getResourceFilename(getSavepointPath(testMigrateVersion, testStateBackend)),
			new Tuple2<>(CheckingNonParallelSourceWithListState.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, 1),
			new Tuple2<>(CheckingRestoringParallelSourceWithUnionListState.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, parallelism),
			new Tuple2<>(CheckingKeyedStateFlatMap.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS * 2),
			new Tuple2<>(CheckingTimelyStatefulOperator.SUCCESSFUL_PROCESS_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS * 2),
			new Tuple2<>(CheckingTimelyStatefulOperator.SUCCESSFUL_EVENT_TIME_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS * 2),
			new Tuple2<>(CheckingTimelyStatefulOperator.SUCCESSFUL_PROCESSING_TIME_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS * 2),
			new Tuple2<>(AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR, NUM_SOURCE_ELEMENTS * 2));
	}

	private String getSavepointPath(MigrationVersion savepointVersion, String backendType) {
		switch (backendType) {
			case StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME:
				return "new-stateful-udf-migration-itcase-flink" + savepointVersion + "-rocksdb-savepoint";
			case StateBackendLoader.MEMORY_STATE_BACKEND_NAME:
				return "new-stateful-udf-migration-itcase-flink" + savepointVersion + "-savepoint";
			default:
				throw new UnsupportedOperationException();
		}
	}

	private static class CheckpointingNonParallelSourceWithListState
		implements SourceFunction<Tuple2<Long, Long>>, CheckpointedFunction {

		static final ListStateDescriptor<String> stateDescriptor =
			new ListStateDescriptor<>("source-state", StringSerializer.INSTANCE);

		static final String checkpointedString = "Here be dragons!";
		static final String checkpointedString1 = "Here be more dragons!";
		static final String checkpointedString2 = "Here be yet more dragons!";
		static final String checkpointedString3 = "Here be the mostest dragons!";

		private static final long serialVersionUID = 1L;

		private volatile boolean isRunning = true;

		private final int numElements;

		private transient ListState<String> unionListState;

		public CheckpointingNonParallelSourceWithListState(int numElements) {
			this.numElements = numElements;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			unionListState.clear();
			unionListState.add(checkpointedString);
			unionListState.add(checkpointedString1);
			unionListState.add(checkpointedString2);
			unionListState.add(checkpointedString3);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			unionListState = context.getOperatorStateStore().getListState(
				stateDescriptor);
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

	private static class CheckingNonParallelSourceWithListState
		extends RichSourceFunction<Tuple2<Long, Long>> implements CheckpointedFunction {

		private static final long serialVersionUID = 1L;

		public static final String SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR = CheckingNonParallelSourceWithListState.class + "_RESTORE_CHECK";

		private volatile boolean isRunning = true;

		private final int numElements;

		public CheckingNonParallelSourceWithListState(int numElements) {
			this.numElements = numElements;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {

		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			ListState<String> unionListState = context.getOperatorStateStore().getListState(
				CheckpointingNonParallelSourceWithListState.stateDescriptor);

			if (context.isRestored()) {
				assertThat(unionListState.get(),
					containsInAnyOrder(
						CheckpointingNonParallelSourceWithListState.checkpointedString,
						CheckpointingNonParallelSourceWithListState.checkpointedString1,
						CheckpointingNonParallelSourceWithListState.checkpointedString2,
						CheckpointingNonParallelSourceWithListState.checkpointedString3));

				getRuntimeContext().addAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, new IntCounter());
				getRuntimeContext().getAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR).add(1);
			} else {
				throw new RuntimeException(
					"This source should always be restored because it's only used when restoring from a savepoint.");
			}
		}

		@Override
		public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {

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

	private static class CheckpointingParallelSourceWithUnionListState
		extends RichSourceFunction<Tuple2<Long, Long>> implements CheckpointedFunction {

		static final ListStateDescriptor<String> stateDescriptor =
			new ListStateDescriptor<>("source-state", StringSerializer.INSTANCE);

		static final String[] checkpointedStrings = {
			"Here be dragons!",
			"Here be more dragons!",
			"Here be yet more dragons!",
			"Here be the mostest dragons!" };

		private static final long serialVersionUID = 1L;

		private volatile boolean isRunning = true;

		private final int numElements;

		private transient ListState<String> unionListState;

		public CheckpointingParallelSourceWithUnionListState(int numElements) {
			this.numElements = numElements;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			unionListState.clear();

			for (String s : checkpointedStrings) {
				if (s.hashCode() % getRuntimeContext().getNumberOfParallelSubtasks() == getRuntimeContext().getIndexOfThisSubtask()) {
					unionListState.add(s);
				}
			}
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			unionListState = context.getOperatorStateStore().getUnionListState(
				stateDescriptor);
		}

		@Override
		public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {

			ctx.emitWatermark(new Watermark(0));

			synchronized (ctx.getCheckpointLock()) {
				for (long i = 0; i < numElements; i++) {
					if (i % getRuntimeContext().getNumberOfParallelSubtasks() == getRuntimeContext().getIndexOfThisSubtask()) {
						ctx.collect(new Tuple2<>(i, i));
					}
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

	private static class CheckingRestoringParallelSourceWithUnionListState
		extends RichParallelSourceFunction<Tuple2<Long, Long>> implements CheckpointedFunction {

		private static final long serialVersionUID = 1L;

		public static final String SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR = CheckingRestoringParallelSourceWithUnionListState.class + "_RESTORE_CHECK";

		private volatile boolean isRunning = true;

		private final int numElements;

		public CheckingRestoringParallelSourceWithUnionListState(int numElements) {
			this.numElements = numElements;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {

		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			ListState<String> unionListState = context.getOperatorStateStore().getUnionListState(
				CheckpointingNonParallelSourceWithListState.stateDescriptor);

			if (context.isRestored()) {
				assertThat(unionListState.get(),
					containsInAnyOrder(CheckpointingParallelSourceWithUnionListState.checkpointedStrings));

				getRuntimeContext().addAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, new IntCounter());
				getRuntimeContext().getAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR).add(1);
			} else {
				throw new RuntimeException(
					"This source should always be restored because it's only used when restoring from a savepoint.");
			}
		}

		@Override
		public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {

			// immediately trigger any set timers
			ctx.emitWatermark(new Watermark(1000));

			synchronized (ctx.getCheckpointLock()) {
				for (long i = 0; i < numElements; i++) {
					if (i % getRuntimeContext().getNumberOfParallelSubtasks() == getRuntimeContext().getIndexOfThisSubtask()) {
						ctx.collect(new Tuple2<>(i, i));
					}
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

	private static class CheckpointingKeyedStateFlatMap extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

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

	private static class CheckpointingTimelyStatefulOperator
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

		@Override
		public void open() throws Exception {
			super.open();

			// have to re-register to ensure that our onEventTime() is called
			getInternalTimerService(
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
