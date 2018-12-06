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

package org.apache.flink.test.checkpointing.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.migration.MigrationVersion;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Migration ITCases for a stateful job with broadcast state. The tests are parameterized to (potentially)
 * cover migrating for multiple previous Flink versions, as well as for different state backends.
 */
@RunWith(Parameterized.class)
public class StatefulJobWBroadcastStateMigrationITCase extends SavepointMigrationTestBase {

	private static final int NUM_SOURCE_ELEMENTS = 4;

	// TODO change this to PERFORM_SAVEPOINT to regenerate binary savepoints
	private final StatefulJobSavepointMigrationITCase.ExecutionMode executionMode =
			StatefulJobSavepointMigrationITCase.ExecutionMode.VERIFY_SAVEPOINT;

	@Parameterized.Parameters(name = "Migrate Savepoint / Backend: {0}")
	public static Collection<Tuple2<MigrationVersion, String>> parameters () {
		return Arrays.asList(
				Tuple2.of(MigrationVersion.v1_5, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
				Tuple2.of(MigrationVersion.v1_5, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME),
				Tuple2.of(MigrationVersion.v1_6, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
				Tuple2.of(MigrationVersion.v1_6, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME));
	}

	private final MigrationVersion testMigrateVersion;
	private final String testStateBackend;

	public StatefulJobWBroadcastStateMigrationITCase(Tuple2<MigrationVersion, String> testMigrateVersionAndBackend) throws Exception {
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
		SourceFunction<Tuple2<Long, Long>> nonParallelSourceB;
		SourceFunction<Tuple2<Long, Long>> parallelSource;
		SourceFunction<Tuple2<Long, Long>> parallelSourceB;
		KeyedBroadcastProcessFunction<Long, Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> firstBroadcastFunction;
		KeyedBroadcastProcessFunction<Long, Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> secondBroadcastFunction;

		final Map<Long, Long> expectedFirstState = new HashMap<>();
		expectedFirstState.put(0L, 0L);
		expectedFirstState.put(1L, 1L);
		expectedFirstState.put(2L, 2L);
		expectedFirstState.put(3L, 3L);

		final Map<String, Long> expectedSecondState = new HashMap<>();
		expectedSecondState.put("0", 0L);
		expectedSecondState.put("1", 1L);
		expectedSecondState.put("2", 2L);
		expectedSecondState.put("3", 3L);

		final Map<Long, String> expectedThirdState = new HashMap<>();
		expectedThirdState.put(0L, "0");
		expectedThirdState.put(1L, "1");
		expectedThirdState.put(2L, "2");
		expectedThirdState.put(3L, "3");

		if (executionMode == StatefulJobSavepointMigrationITCase.ExecutionMode.PERFORM_SAVEPOINT) {
			nonParallelSource = new MigrationTestUtils.CheckpointingNonParallelSourceWithListState(NUM_SOURCE_ELEMENTS);
			nonParallelSourceB = new MigrationTestUtils.CheckpointingNonParallelSourceWithListState(NUM_SOURCE_ELEMENTS);
			parallelSource = new MigrationTestUtils.CheckpointingParallelSourceWithUnionListState(NUM_SOURCE_ELEMENTS);
			parallelSourceB = new MigrationTestUtils.CheckpointingParallelSourceWithUnionListState(NUM_SOURCE_ELEMENTS);
			firstBroadcastFunction = new CheckpointingKeyedBroadcastFunction();
			secondBroadcastFunction = new CheckpointingKeyedSingleBroadcastFunction();
		} else if (executionMode == StatefulJobSavepointMigrationITCase.ExecutionMode.VERIFY_SAVEPOINT) {
			nonParallelSource = new MigrationTestUtils.CheckingNonParallelSourceWithListState(NUM_SOURCE_ELEMENTS);
			nonParallelSourceB = new MigrationTestUtils.CheckingNonParallelSourceWithListState(NUM_SOURCE_ELEMENTS);
			parallelSource = new MigrationTestUtils.CheckingParallelSourceWithUnionListState(NUM_SOURCE_ELEMENTS);
			parallelSourceB = new MigrationTestUtils.CheckingParallelSourceWithUnionListState(NUM_SOURCE_ELEMENTS);
			firstBroadcastFunction = new CheckingKeyedBroadcastFunction(expectedFirstState, expectedSecondState);
			secondBroadcastFunction = new CheckingKeyedSingleBroadcastFunction(expectedThirdState);
		} else {
			throw new IllegalStateException("Unknown ExecutionMode " + executionMode);
		}

		KeyedStream<Tuple2<Long, Long>, Long> npStream = env
				.addSource(nonParallelSource).uid("CheckpointingSource1")
				.keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {

					private static final long serialVersionUID = -4514793867774977152L;

					@Override
					public Long getKey(Tuple2<Long, Long> value) throws Exception {
						return value.f0;
					}
				});

		KeyedStream<Tuple2<Long, Long>, Long> pStream = env
				.addSource(parallelSource).uid("CheckpointingSource2")
				.keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {

					private static final long serialVersionUID = 4940496713319948104L;

					@Override
					public Long getKey(Tuple2<Long, Long> value) throws Exception {
						return value.f0;
					}
				});

		final MapStateDescriptor<Long, Long> firstBroadcastStateDesc = new MapStateDescriptor<>(
				"broadcast-state-1", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO
		);

		final MapStateDescriptor<String, Long> secondBroadcastStateDesc = new MapStateDescriptor<>(
				"broadcast-state-2", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO
		);

		final MapStateDescriptor<Long, String> thirdBroadcastStateDesc = new MapStateDescriptor<>(
				"broadcast-state-3", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
		);

		BroadcastStream<Tuple2<Long, Long>> npBroadcastStream = env
				.addSource(nonParallelSourceB).uid("BrCheckpointingSource1")
				.broadcast(firstBroadcastStateDesc, secondBroadcastStateDesc);

		BroadcastStream<Tuple2<Long, Long>> pBroadcastStream = env
				.addSource(parallelSourceB).uid("BrCheckpointingSource2")
				.broadcast(thirdBroadcastStateDesc);

		npStream
				.connect(npBroadcastStream)
				.process(firstBroadcastFunction).uid("BrProcess1")
				.addSink(new MigrationTestUtils.AccumulatorCountingSink<>());

		pStream
				.connect(pBroadcastStream)
				.process(secondBroadcastFunction).uid("BrProcess2")
				.addSink(new MigrationTestUtils.AccumulatorCountingSink<>());

		if (executionMode == StatefulJobSavepointMigrationITCase.ExecutionMode.PERFORM_SAVEPOINT) {
			executeAndSavepoint(
					env,
					"src/test/resources/" + getBroadcastSavepointPath(testMigrateVersion, testStateBackend),
					new Tuple2<>(MigrationTestUtils.AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR, 2 * NUM_SOURCE_ELEMENTS));
		} else {
			restoreAndExecute(
					env,
					getResourceFilename(getBroadcastSavepointPath(testMigrateVersion, testStateBackend)),
					new Tuple2<>(MigrationTestUtils.CheckingNonParallelSourceWithListState.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, 2),					// we have 2 sources
					new Tuple2<>(MigrationTestUtils.CheckingParallelSourceWithUnionListState.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, 2 * parallelism),	// we have 2 sources
					new Tuple2<>(MigrationTestUtils.AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR, NUM_SOURCE_ELEMENTS * 2)
			);
		}
	}

	private String getBroadcastSavepointPath(MigrationVersion savepointVersion, String backendType) {
		switch (backendType) {
			case StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME:
				return "new-stateful-broadcast-udf-migration-itcase-flink" + savepointVersion + "-rocksdb-savepoint";
			case StateBackendLoader.MEMORY_STATE_BACKEND_NAME:
				return "new-stateful-broadcast-udf-migration-itcase-flink" + savepointVersion + "-savepoint";
			default:
				throw new UnsupportedOperationException();
		}
	}

	/**
	 * A simple {@link KeyedBroadcastProcessFunction} that puts everything on the broadcast side in the state.
	 */
	private static class CheckpointingKeyedBroadcastFunction
			extends KeyedBroadcastProcessFunction<Long, Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1333992081671604521L;

		private MapStateDescriptor<Long, Long> firstStateDesc;

		private MapStateDescriptor<String, Long> secondStateDesc;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			firstStateDesc = new MapStateDescriptor<>(
					"broadcast-state-1", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO
			);

			secondStateDesc = new MapStateDescriptor<>(
					"broadcast-state-2", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO
			);
		}

		@Override
		public void processElement(Tuple2<Long, Long> value, ReadOnlyContext ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);
		}

		@Override
		public void processBroadcastElement(Tuple2<Long, Long> value, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
			ctx.getBroadcastState(firstStateDesc).put(value.f0, value.f1);
			ctx.getBroadcastState(secondStateDesc).put(Long.toString(value.f0), value.f1);
		}
	}

	/**
	 * A simple {@link KeyedBroadcastProcessFunction} that puts everything on the broadcast side in the state.
	 */
	private static class CheckpointingKeyedSingleBroadcastFunction
			extends KeyedBroadcastProcessFunction<Long, Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1333992081671604521L;

		private MapStateDescriptor<Long, String> stateDesc;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			stateDesc = new MapStateDescriptor<>(
					"broadcast-state-3", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
			);
		}

		@Override
		public void processElement(Tuple2<Long, Long> value, ReadOnlyContext ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);
		}

		@Override
		public void processBroadcastElement(Tuple2<Long, Long> value, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
			ctx.getBroadcastState(stateDesc).put(value.f0, Long.toString(value.f1));
		}
	}

	/**
	 * A simple {@link KeyedBroadcastProcessFunction} that verifies the contents of the broadcast state after recovery.
	 */
	private static class CheckingKeyedBroadcastFunction
			extends KeyedBroadcastProcessFunction<Long, Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1333992081671604521L;

		private final Map<Long, Long> expectedFirstState;

		private final Map<String, Long> expectedSecondState;

		private MapStateDescriptor<Long, Long> firstStateDesc;

		private MapStateDescriptor<String, Long> secondStateDesc;

		CheckingKeyedBroadcastFunction(Map<Long, Long> firstState, Map<String, Long> secondState) {
			this.expectedFirstState = firstState;
			this.expectedSecondState = secondState;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			firstStateDesc = new MapStateDescriptor<>(
					"broadcast-state-1", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO
			);

			secondStateDesc = new MapStateDescriptor<>(
					"broadcast-state-2", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO
			);
		}

		@Override
		public void processElement(Tuple2<Long, Long> value, ReadOnlyContext ctx, Collector<Tuple2<Long, Long>> out) throws Exception {

			final Map<Long, Long> actualFirstState = new HashMap<>();
			for (Map.Entry<Long, Long> entry: ctx.getBroadcastState(firstStateDesc).immutableEntries()) {
				actualFirstState.put(entry.getKey(), entry.getValue());
			}
			Assert.assertEquals(expectedFirstState, actualFirstState);

			final Map<String, Long> actualSecondState = new HashMap<>();
			for (Map.Entry<String, Long> entry: ctx.getBroadcastState(secondStateDesc).immutableEntries()) {
				actualSecondState.put(entry.getKey(), entry.getValue());
			}
			Assert.assertEquals(expectedSecondState, actualSecondState);

			out.collect(value);
		}

		@Override
		public void processBroadcastElement(Tuple2<Long, Long> value, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
			// now we do nothing as we just want to verify the contents of the broadcast state.
		}
	}

	/**
	 * A simple {@link KeyedBroadcastProcessFunction} that verifies the contents of the broadcast state after recovery.
	 */
	private static class CheckingKeyedSingleBroadcastFunction
			extends KeyedBroadcastProcessFunction<Long, Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1333992081671604521L;

		private final Map<Long, String> expectedState;

		private MapStateDescriptor<Long, String> stateDesc;

		CheckingKeyedSingleBroadcastFunction(Map<Long, String> state) {
			this.expectedState = state;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			stateDesc = new MapStateDescriptor<>(
					"broadcast-state-3", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
			);
		}

		@Override
		public void processElement(Tuple2<Long, Long> value, ReadOnlyContext ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
			final Map<Long, String> actualState = new HashMap<>();
			for (Map.Entry<Long, String> entry: ctx.getBroadcastState(stateDesc).immutableEntries()) {
				actualState.put(entry.getKey(), entry.getValue());
			}
			Assert.assertEquals(expectedState, actualState);

			out.collect(value);
		}

		@Override
		public void processBroadcastElement(Tuple2<Long, Long> value, Context ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
			// now we do nothing as we just want to verify the contents of the broadcast state.
		}
	}
}
