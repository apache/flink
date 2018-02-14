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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
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
import org.apache.flink.streaming.util.migration.MigrationVersion;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Migration ITCases for a stateful job. The tests are parameterized to cover
 * migrating for different state backends.
 */
@RunWith(Parameterized.class)
public class BroadcastStateJobMigrationITCase extends SavepointMigrationTestBase {

	private static final int NUM_SOURCE_ELEMENTS = 4;

	@Parameterized.Parameters(name = "Migrate Savepoint / Backend: {0}")
	public static Collection<Tuple2<MigrationVersion, String>> parameters () {
		return Arrays.asList(
				Tuple2.of(MigrationVersion.v1_5, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
				Tuple2.of(MigrationVersion.v1_5, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME));
	}

	/**
	 * TODO to generate savepoints for a specific Flink version / backend type,
	 * TODO change these values accordingly, e.g. to generate for 1.3 with RocksDB,
	 * TODO set as (MigrationVersion.v1_3, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME)
	 */
	private final MigrationVersion flinkGenerateSavepointVersion = MigrationVersion.v1_5;
	private final String flinkGenerateSavepointBackendType = StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME;

	private final MigrationVersion testMigrateVersion;
	private final String testStateBackend;

	public BroadcastStateJobMigrationITCase(Tuple2<MigrationVersion, String> testMigrateVersionAndBackend) {
		this.testMigrateVersion = testMigrateVersionAndBackend.f0;
		this.testStateBackend = testMigrateVersionAndBackend.f1;
	}

	/**
	 * Manually run this to write binary snapshot data.
	 */
	@Test
	@Ignore
	public void writeSavepointWithBroadcast() throws Exception {

		final MapStateDescriptor<Long, Long> firstBroadcastStateDesc = new MapStateDescriptor<>(
				"broadcast-state-1", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO
		);

		final MapStateDescriptor<String, String> secondBroadcastStateDesc = new MapStateDescriptor<>(
				"broadcast-state-2", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
		);

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

		KeyedStream<Tuple2<Long, Long>, Long> stream = env
				.addSource(new StatefulJobSavepointMigrationITCase.LegacyCheckpointedSource(NUM_SOURCE_ELEMENTS)).setMaxParallelism(1).uid("LegacyCheckpointedSource")
				.flatMap(new StatefulJobSavepointMigrationITCase.LegacyCheckpointedFlatMap()).startNewChain().uid("LegacyCheckpointedFlatMap")
				.keyBy(0)
				.flatMap(new StatefulJobSavepointMigrationITCase.LegacyCheckpointedFlatMapWithKeyedState()).startNewChain().uid("LegacyCheckpointedFlatMapWithKeyedState")
				.keyBy(0)
				.flatMap(new StatefulJobSavepointMigrationITCase.KeyedStateSettingFlatMap()).startNewChain().uid("KeyedStateSettingFlatMap")
				.keyBy(0)
				.transform(
						"custom_operator",
						new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
						new StatefulJobSavepointMigrationITCase.CheckpointedUdfOperator(new StatefulJobSavepointMigrationITCase.LegacyCheckpointedFlatMapWithKeyedState())).uid("LegacyCheckpointedOperator")
				.keyBy(0)
				.transform(
						"timely_stateful_operator",
						new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
						new StatefulJobSavepointMigrationITCase.TimelyStatefulOperator()).uid("TimelyStatefulOperator")
				.keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {

					private static final long serialVersionUID = -4514793867774977152L;

					@Override
					public Long getKey(Tuple2<Long, Long> value) throws Exception {
						return value.f0;
					}
				});

		BroadcastStream<Tuple2<Long, Long>> broadcastStream = env
				.addSource(new StatefulJobSavepointMigrationITCase.LegacyCheckpointedSource(NUM_SOURCE_ELEMENTS)).setMaxParallelism(1).uid("LegacyCheckpointedSourceBroadcastSide")
				.broadcast(firstBroadcastStateDesc, secondBroadcastStateDesc);

		stream
				.connect(broadcastStream)
				.process(new KeyedBroadcastFunction())
				.addSink(new StatefulJobSavepointMigrationITCase.AccumulatorCountingSink<>());

		executeAndSavepoint(
				env,
				"src/test/resources/" + getBroadcastSavepointPath(flinkGenerateSavepointVersion, flinkGenerateSavepointBackendType),
				new Tuple2<>(StatefulJobSavepointMigrationITCase.AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR, NUM_SOURCE_ELEMENTS));
	}

	@Test
	public void testSavepointRestoreWithBroadcast() throws Exception {

		final MapStateDescriptor<Long, Long> firstBroadcastStateDesc = new MapStateDescriptor<>(
				"broadcast-state-1", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO
		);

		final MapStateDescriptor<String, String> secondBroadcastStateDesc = new MapStateDescriptor<>(
				"broadcast-state-2", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
		);

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
		env.setParallelism(4);
		env.setMaxParallelism(4);

		KeyedStream<Tuple2<Long, Long>, Long> stream = env
				.addSource(new StatefulJobSavepointMigrationITCase.CheckingRestoringSource(NUM_SOURCE_ELEMENTS)).setMaxParallelism(1).uid("LegacyCheckpointedSource")
				.flatMap(new StatefulJobSavepointMigrationITCase.CheckingRestoringFlatMap()).startNewChain().uid("LegacyCheckpointedFlatMap")
				.keyBy(0)
				.flatMap(new StatefulJobSavepointMigrationITCase.CheckingRestoringFlatMapWithKeyedState()).startNewChain().uid("LegacyCheckpointedFlatMapWithKeyedState")
				.keyBy(0)
				.flatMap(new StatefulJobSavepointMigrationITCase.CheckingKeyedStateFlatMap()).startNewChain().uid("KeyedStateSettingFlatMap")
				.keyBy(0)
				.transform(
						"custom_operator",
						new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
						new StatefulJobSavepointMigrationITCase.CheckingRestoringUdfOperator(new StatefulJobSavepointMigrationITCase.CheckingRestoringFlatMapWithKeyedStateInOperator())).uid("LegacyCheckpointedOperator")
				.keyBy(0)
				.transform(
						"timely_stateful_operator",
						new TypeHint<Tuple2<Long, Long>>() {}.getTypeInfo(),
						new StatefulJobSavepointMigrationITCase.CheckingTimelyStatefulOperator()).uid("TimelyStatefulOperator")
				.keyBy(new KeySelector<Tuple2<Long, Long>, Long>() {

					private static final long serialVersionUID = -4514793867774977152L;

					@Override
					public Long getKey(Tuple2<Long, Long> value) throws Exception {
						return value.f0;
					}
				});

		BroadcastStream<Tuple2<Long, Long>> broadcastStream = env
				.addSource(new StatefulJobSavepointMigrationITCase.CheckingRestoringSource(NUM_SOURCE_ELEMENTS)).setMaxParallelism(1).uid("LegacyCheckpointedSourceBroadcastSide")
				.broadcast(firstBroadcastStateDesc, secondBroadcastStateDesc);

		final Map<Long, Long> expectedFirstState = new HashMap<>();
		expectedFirstState.put(0L, 0L);
		expectedFirstState.put(1L, 1L);
		expectedFirstState.put(2L, 2L);
		expectedFirstState.put(3L, 3L);

		final Map<String, String> expectedSecondState = new HashMap<>();
		expectedSecondState.put("0", "0");
		expectedSecondState.put("1", "1");
		expectedSecondState.put("2", "2");
		expectedSecondState.put("3", "3");

		stream
				.connect(broadcastStream)
				.process(new CheckingKeyedBroadcastFunction(expectedFirstState, expectedSecondState))
				.addSink(new StatefulJobSavepointMigrationITCase.AccumulatorCountingSink<>());

		restoreAndExecute(
				env,
				getResourceFilename(getBroadcastSavepointPath(testMigrateVersion, testStateBackend)),
				new Tuple2<>(StatefulJobSavepointMigrationITCase.CheckingRestoringSource.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, 2), // we have 2 sources now.
				new Tuple2<>(StatefulJobSavepointMigrationITCase.CheckingRestoringFlatMap.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(StatefulJobSavepointMigrationITCase.CheckingRestoringFlatMapWithKeyedState.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(StatefulJobSavepointMigrationITCase.CheckingKeyedStateFlatMap.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(StatefulJobSavepointMigrationITCase.CheckingRestoringUdfOperator.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(StatefulJobSavepointMigrationITCase.CheckingRestoringFlatMapWithKeyedStateInOperator.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(StatefulJobSavepointMigrationITCase.CheckingTimelyStatefulOperator.SUCCESSFUL_PROCESS_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(StatefulJobSavepointMigrationITCase.CheckingTimelyStatefulOperator.SUCCESSFUL_EVENT_TIME_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(StatefulJobSavepointMigrationITCase.CheckingTimelyStatefulOperator.SUCCESSFUL_PROCESSING_TIME_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(StatefulJobSavepointMigrationITCase.AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR, NUM_SOURCE_ELEMENTS));
	}

	private String getBroadcastSavepointPath(MigrationVersion savepointVersion, String backendType) {
		switch (backendType) {
			case StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME:
				return "stateful-broadcast-udf-migration-itcase-flink" + savepointVersion + "-rocksdb-savepoint";
			case StateBackendLoader.MEMORY_STATE_BACKEND_NAME:
				return "stateful-broadcast-udf-migration-itcase-flink" + savepointVersion + "-savepoint";
			default:
				throw new UnsupportedOperationException();
		}
	}

	/**
	 * A simple{@link KeyedBroadcastProcessFunction} that puts everything on the broadcast side in the state.
	 */
	private static class KeyedBroadcastFunction
			extends KeyedBroadcastProcessFunction<Long, Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1333992081671604521L;

		private MapStateDescriptor<Long, Long> firstStateDesc;

		private MapStateDescriptor<String, String> secondStateDesc;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			firstStateDesc = new MapStateDescriptor<>(
					"broadcast-state-1", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO
			);

			secondStateDesc = new MapStateDescriptor<>(
					"broadcast-state-2", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
			);
		}

		@Override
		public void processElement(Tuple2<Long, Long> value, KeyedReadOnlyContext ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
			out.collect(value);
		}

		@Override
		public void processBroadcastElement(Tuple2<Long, Long> value, KeyedContext ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
			ctx.getBroadcastState(firstStateDesc).put(value.f0, value.f1);
			ctx.getBroadcastState(secondStateDesc).put(Long.toString(value.f0), Long.toString(value.f1));
		}
	}

	/**
	 * A simple{@link KeyedBroadcastProcessFunction} that verifies the contents of the broadcast state after recovery.
	 */
	private static class CheckingKeyedBroadcastFunction
			extends KeyedBroadcastProcessFunction<Long, Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>> {

		private static final long serialVersionUID = 1333992081671604521L;

		private final Map<Long, Long> expectedFirstState;

		private final Map<String, String> expectedSecondState;

		private MapStateDescriptor<Long, Long> firstStateDesc;

		private MapStateDescriptor<String, String> secondStateDesc;

		CheckingKeyedBroadcastFunction(Map<Long, Long> firstState, Map<String, String> secondState) {
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
					"broadcast-state-2", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO
			);
		}

		@Override
		public void processElement(Tuple2<Long, Long> value, KeyedReadOnlyContext ctx, Collector<Tuple2<Long, Long>> out) throws Exception {

			final Map<Long, Long> actualFirstState = new HashMap<>();
			for (Map.Entry<Long, Long> entry: ctx.getBroadcastState(firstStateDesc).immutableEntries()) {
				actualFirstState.put(entry.getKey(), entry.getValue());
			}
			Assert.assertEquals(expectedFirstState, actualFirstState);

			final Map<String, String> actualSecondState = new HashMap<>();
			for (Map.Entry<String, String> entry: ctx.getBroadcastState(secondStateDesc).immutableEntries()) {
				actualSecondState.put(entry.getKey(), entry.getValue());
			}
			Assert.assertEquals(expectedSecondState, actualSecondState);

			out.collect(value);
		}

		@Override
		public void processBroadcastElement(Tuple2<Long, Long> value, KeyedContext ctx, Collector<Tuple2<Long, Long>> out) throws Exception {
			// now we do nothing as we just want to verify the contents of the broadcast state.
		}
	}
}
