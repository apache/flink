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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.testutils.migration.MigrationVersion;
import org.apache.flink.util.Collector;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * Migration ITCases for a stateful job. The tests are parameterized to cover
 * migrating for multiple previous Flink versions, as well as for different state backends.
 */
@RunWith(Parameterized.class)
public class StatefulJobSavepointMigrationITCase extends SavepointMigrationTestBase {

	private static final int NUM_SOURCE_ELEMENTS = 4;

	/**
	 * This test runs in either of two modes: 1) we want to generate the binary savepoint, i.e.
	 * we have to run the checkpointing functions 2) we want to verify restoring, so we have to run
	 * the checking functions.
	 */
	public enum ExecutionMode {
		PERFORM_SAVEPOINT,
		VERIFY_SAVEPOINT
	}

	// TODO change this to PERFORM_SAVEPOINT to regenerate binary savepoints
	// TODO Note: You should generate the savepoint based on the release branch instead of the master.
	private final ExecutionMode executionMode = ExecutionMode.VERIFY_SAVEPOINT;

	@Parameterized.Parameters(name = "Migrate Savepoint / Backend: {0}")
	public static Collection<Tuple2<MigrationVersion, String>> parameters () {
		return Arrays.asList(
			Tuple2.of(MigrationVersion.v1_4, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
			Tuple2.of(MigrationVersion.v1_4, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME),
			Tuple2.of(MigrationVersion.v1_5, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
			Tuple2.of(MigrationVersion.v1_5, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME),
			Tuple2.of(MigrationVersion.v1_6, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
			Tuple2.of(MigrationVersion.v1_6, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME),
			Tuple2.of(MigrationVersion.v1_7, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
			Tuple2.of(MigrationVersion.v1_7, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME),
			Tuple2.of(MigrationVersion.v1_8, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
			Tuple2.of(MigrationVersion.v1_8, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME),
			Tuple2.of(MigrationVersion.v1_9, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
			Tuple2.of(MigrationVersion.v1_9, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME));
	}

	private final MigrationVersion testMigrateVersion;
	private final String testStateBackend;

	public StatefulJobSavepointMigrationITCase(Tuple2<MigrationVersion, String> testMigrateVersionAndBackend) throws Exception {
		this.testMigrateVersion = testMigrateVersionAndBackend.f0;
		this.testStateBackend = testMigrateVersionAndBackend.f1;
	}

	@Test
	public void testSavepoint() throws Exception {

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

		SourceFunction<Tuple2<Long, Long>> nonParallelSource;
		SourceFunction<Tuple2<Long, Long>> parallelSource;
		RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> flatMap;
		OneInputStreamOperator<Tuple2<Long, Long>, Tuple2<Long, Long>> timelyOperator;

		if (executionMode == ExecutionMode.PERFORM_SAVEPOINT) {
			nonParallelSource = new MigrationTestUtils.CheckpointingNonParallelSourceWithListState(NUM_SOURCE_ELEMENTS);
			parallelSource = new MigrationTestUtils.CheckpointingParallelSourceWithUnionListState(NUM_SOURCE_ELEMENTS);
			flatMap = new CheckpointingKeyedStateFlatMap();
			timelyOperator = new CheckpointingTimelyStatefulOperator();
		} else if (executionMode == ExecutionMode.VERIFY_SAVEPOINT) {
			nonParallelSource = new MigrationTestUtils.CheckingNonParallelSourceWithListState(NUM_SOURCE_ELEMENTS);
			parallelSource = new MigrationTestUtils.CheckingParallelSourceWithUnionListState(NUM_SOURCE_ELEMENTS);
			flatMap = new CheckingKeyedStateFlatMap();
			timelyOperator = new CheckingTimelyStatefulOperator();
		} else {
			throw new IllegalStateException("Unknown ExecutionMode " + executionMode);
		}

		env
			.addSource(nonParallelSource).uid("CheckpointingSource1")
			.keyBy(0)
			.flatMap(flatMap).startNewChain().uid("CheckpointingKeyedStateFlatMap1")
			.keyBy(0)
			.transform(
				"timely_stateful_operator",
				new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
				timelyOperator).uid("CheckpointingTimelyStatefulOperator1")
			.addSink(new MigrationTestUtils.AccumulatorCountingSink<>());

		env
			.addSource(parallelSource).uid("CheckpointingSource2")
			.keyBy(0)
			.flatMap(flatMap).startNewChain().uid("CheckpointingKeyedStateFlatMap2")
			.keyBy(0)
			.transform(
				"timely_stateful_operator",
				new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
				timelyOperator).uid("CheckpointingTimelyStatefulOperator2")
			.addSink(new MigrationTestUtils.AccumulatorCountingSink<>());

		if (executionMode == ExecutionMode.PERFORM_SAVEPOINT) {
			executeAndSavepoint(
				env,
				"src/test/resources/" + getSavepointPath(testMigrateVersion, testStateBackend),
				new Tuple2<>(MigrationTestUtils.AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR, NUM_SOURCE_ELEMENTS * 2));
		} else {
			restoreAndExecute(
				env,
				getResourceFilename(getSavepointPath(testMigrateVersion, testStateBackend)),
				new Tuple2<>(MigrationTestUtils.CheckingNonParallelSourceWithListState.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, 1),
				new Tuple2<>(MigrationTestUtils.CheckingParallelSourceWithUnionListState.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, parallelism),
				new Tuple2<>(CheckingKeyedStateFlatMap.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS * 2),
				new Tuple2<>(CheckingTimelyStatefulOperator.SUCCESSFUL_PROCESS_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS * 2),
				new Tuple2<>(CheckingTimelyStatefulOperator.SUCCESSFUL_EVENT_TIME_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS * 2),
				new Tuple2<>(CheckingTimelyStatefulOperator.SUCCESSFUL_PROCESSING_TIME_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS * 2),
				new Tuple2<>(MigrationTestUtils.AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR, NUM_SOURCE_ELEMENTS * 2));
		}
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

	private static class CheckpointingKeyedStateFlatMap extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		private final ValueStateDescriptor<Long> stateDescriptor =
			new ValueStateDescriptor<>("state-name", LongSerializer.INSTANCE);

		@Override
		public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);

			getRuntimeContext().getState(stateDescriptor).update(value.f1);
		}
	}

	private static class CheckingKeyedStateFlatMap extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1L;

		static final String SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR = CheckingKeyedStateFlatMap.class + "_RESTORE_CHECK";

		private final ValueStateDescriptor<Long> stateDescriptor =
			new ValueStateDescriptor<>("state-name", LongSerializer.INSTANCE);

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
			new ValueStateDescriptor<>("state-name", LongSerializer.INSTANCE);

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
		public void onEventTime(InternalTimer<Long, Long> timer) {

		}

		@Override
		public void onProcessingTime(InternalTimer<Long, Long> timer) {

		}

		@Override
		public void processWatermark(Watermark mark) {
			output.emitWatermark(mark);
		}
	}

	private static class CheckingTimelyStatefulOperator
		extends AbstractStreamOperator<Tuple2<Long, Long>>
		implements OneInputStreamOperator<Tuple2<Long, Long>, Tuple2<Long, Long>>, Triggerable<Long, Long> {
		private static final long serialVersionUID = 1L;

		static final String SUCCESSFUL_PROCESS_CHECK_ACCUMULATOR = CheckingTimelyStatefulOperator.class + "_PROCESS_CHECKS";
		static final String SUCCESSFUL_EVENT_TIME_CHECK_ACCUMULATOR = CheckingTimelyStatefulOperator.class + "_ET_CHECKS";
		static final String SUCCESSFUL_PROCESSING_TIME_CHECK_ACCUMULATOR = CheckingTimelyStatefulOperator.class + "_PT_CHECKS";

		private final ValueStateDescriptor<Long> stateDescriptor =
			new ValueStateDescriptor<>("state-name", LongSerializer.INSTANCE);

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
}
