/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.protobuf;

import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.formats.protobuf.generated.UserProtobuf;
import org.apache.flink.formats.protobuf.typeutils.ProtobufSerializer;
import org.apache.flink.formats.protobuf.utils.TestDataGenerator;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.checkpointing.utils.MigrationTestUtils;
import org.apache.flink.test.checkpointing.utils.SavepointMigrationTestBase;
import org.apache.flink.testutils.migration.MigrationVersion;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

/**
 * Migration ITCases for a stateful job with customized Protobuf. The tests are parameterized to cover
 * migrating from pre Flink-1.8 to using built-in {@link ProtobufSerializer}, as well as for different state backends.
 */
@RunWith(Parameterized.class)
public class ProtobufSavepointMigrationITCase extends SavepointMigrationTestBase {

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
			Tuple2.of(MigrationVersion.v1_7, StateBackendLoader.MEMORY_STATE_BACKEND_NAME),
			Tuple2.of(MigrationVersion.v1_7, StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME));
	}

	private static final UserProtobuf.User[] PREDEFINED_DATA = TestDataGenerator.getPredefinedData();

	private static final int NUM_SOURCE_ELEMENTS = PREDEFINED_DATA.length;

	private final MigrationVersion testMigrateVersion;
	private final String testStateBackend;

	public ProtobufSavepointMigrationITCase(Tuple2<MigrationVersion, String> testMigrateVersionAndBackend) throws Exception {
		this.testMigrateVersion = testMigrateVersionAndBackend.f0;
		this.testStateBackend = testMigrateVersionAndBackend.f1;
	}

	@Test
	public void testSavepoint() throws Exception {
		final int parallelism = NUM_SOURCE_ELEMENTS;

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

		env.enableCheckpointing(1000);
		env.setParallelism(parallelism);
		env.setMaxParallelism(parallelism);

		SourceFunction<Tuple2<Integer, Long>> source = new SimpleSource(parallelism);
		RichMapFunction<Tuple2<Integer, Long>, Tuple2<Integer, UserProtobuf.User>> mapFunction;

		if (executionMode == ExecutionMode.PERFORM_SAVEPOINT) {
			mapFunction = new CheckpointingMapFunction();
		} else if (executionMode == ExecutionMode.VERIFY_SAVEPOINT) {
			mapFunction = new CheckingMapFunction();
		} else {
			throw new IllegalStateException("Unknown ExecutionMode " + executionMode);
		}

		env.addSource(source).uid("SimpleSource")
			.keyBy(0)
			.map(mapFunction).startNewChain().uid("MapFunction")
			.addSink(new MigrationTestUtils.AccumulatorCountingSink<>());

		if (executionMode == ExecutionMode.PERFORM_SAVEPOINT) {
			// TODO this configuration below only works before Flink-1.8
			env.getConfig().registerTypeWithKryoSerializer(UserProtobuf.User.class, com.twitter.chill.protobuf.ProtobufSerializer.class);
			executeAndSavepoint(
				env,
				"src/test/resources/" + getSavepointPath(testMigrateVersion, testStateBackend),
				new Tuple2<>(MigrationTestUtils.AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR, NUM_SOURCE_ELEMENTS));
		} else {
			restoreAndExecute(
				env,
				getResourceFilename(getSavepointPath(testMigrateVersion, testStateBackend)),
				new Tuple2<>(CheckingMapFunction.SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, NUM_SOURCE_ELEMENTS),
				new Tuple2<>(MigrationTestUtils.AccumulatorCountingSink.NUM_ELEMENTS_ACCUMULATOR, NUM_SOURCE_ELEMENTS));

		}

	}

	private String getSavepointPath(MigrationVersion savepointVersion, String backendType) {
		switch (backendType) {
			case StateBackendLoader.ROCKSDB_STATE_BACKEND_NAME:
				return "new-protobuf-migration-itcase-flink" + savepointVersion + "-rocksdb-savepoint";
			case StateBackendLoader.MEMORY_STATE_BACKEND_NAME:
				return "new-protobuf-migration-itcase-flink" + savepointVersion + "-savepoint";
			default:
				throw new UnsupportedOperationException();
		}
	}

	private static class SimpleSource extends RichSourceFunction<Tuple2<Integer, Long>> {
		private static final long serialVersionUID = 1L;

		private volatile boolean isRunning = true;
		private int maxNum = 0;

		public SimpleSource(int maxNum) {
			this.maxNum = maxNum;
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, Long>> ctx) throws Exception {
			synchronized (ctx.getCheckpointLock()) {
				for (int i = 0; i < maxNum; i++) {
					if (i % getRuntimeContext().getNumberOfParallelSubtasks() == getRuntimeContext().getIndexOfThisSubtask()) {
						ctx.collect(Tuple2.of(i, (long) i));
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

	private static class CheckpointingMapFunction extends RichMapFunction<Tuple2<Integer, Long>, Tuple2<Integer, UserProtobuf.User>> {
		private static final long serialVersionUID = 1L;

		protected final ValueStateDescriptor<UserProtobuf.User> stateDescriptor =
			new ValueStateDescriptor<>("state-name", TypeInformation.of(UserProtobuf.User.class));

		@Override
		public Tuple2<Integer, UserProtobuf.User> map(Tuple2<Integer, Long> value) throws Exception {

			UserProtobuf.User user = PREDEFINED_DATA[value.f0];
			getRuntimeContext().getState(stateDescriptor).update(user);
			return Tuple2.of(value.f0, user);
		}
	}

	private static class CheckingMapFunction extends CheckpointingMapFunction {
		private static final long serialVersionUID = 1L;

		static final String SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR = CheckingMapFunction.class + "_RESTORE_CHECK";

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);

			getRuntimeContext().addAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR, new IntCounter());
		}

		@Override
		public Tuple2<Integer, UserProtobuf.User> map(Tuple2<Integer, Long> value) throws Exception {

			ValueState<UserProtobuf.User> state = getRuntimeContext().getState(stateDescriptor);

			Assert.assertEquals(PREDEFINED_DATA[value.f0], state.value());
			getRuntimeContext().getAccumulator(SUCCESSFUL_RESTORE_CHECK_ACCUMULATOR).add(1);
			return Tuple2.of(value.f0, state.value());
		}
	}
}
