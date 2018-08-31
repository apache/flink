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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.checkpointing.utils.FailingSource;
import org.apache.flink.test.checkpointing.utils.IntType;
import org.apache.flink.test.checkpointing.utils.ValidatingSink;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.MiniClusterResourceConfiguration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.test.checkpointing.EventTimeWindowCheckpointingITCase.StateBackendEnum.ROCKSDB_INCREMENTAL_ZK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This verifies that checkpointing works correctly with event time windows. This is more
 * strict than {@link WindowCheckpointingITCase} because for event-time the contents
 * of the emitted windows are deterministic.
 *
 * <p>Split into multiple test classes in order to decrease the runtime per backend
 * and not run into CI infrastructure limits like no std output being emitted for
 * I/O heavy variants.
 */
@SuppressWarnings("serial")
@RunWith(Parameterized.class)
public class EventTimeWindowCheckpointingITCase extends TestLogger {

	private static final int MAX_MEM_STATE_SIZE = 20 * 1024 * 1024;
	private static final int PARALLELISM = 4;

	private TestingServer zkServer;

	public MiniClusterResource miniClusterResource;

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	@Rule
	public TestName name = new TestName();

	private AbstractStateBackend stateBackend;

	@Parameterized.Parameter
	public StateBackendEnum stateBackendEnum;

	enum StateBackendEnum {
		MEM, FILE, ROCKSDB_FULLY_ASYNC, ROCKSDB_INCREMENTAL, ROCKSDB_INCREMENTAL_ZK, MEM_ASYNC, FILE_ASYNC
	}

	@Parameterized.Parameters(name = "statebackend type ={0}")
	public static Collection<StateBackendEnum> parameter() {
		return Arrays.asList(StateBackendEnum.values());
	}

	protected StateBackendEnum getStateBackend() {
		return this.stateBackendEnum;
	}

	protected final MiniClusterResource getMiniClusterResource() {
		return new MiniClusterResource(
			new MiniClusterResourceConfiguration.Builder()
				.setConfiguration(getConfigurationSafe())
				.setNumberTaskManagers(2)
				.setNumberSlotsPerTaskManager(PARALLELISM / 2)
				.build());
	}

	private Configuration getConfigurationSafe() {
		try {
			return getConfiguration();
		} catch (Exception e) {
			throw new AssertionError("Could not initialize test.", e);
		}
	}

	private Configuration getConfiguration() throws Exception {

		// print a message when starting a test method to avoid Travis' <tt>"Maven produced no
		// output for xxx seconds."</tt> messages
		System.out.println(
			"Starting " + getClass().getCanonicalName() + "#" + name.getMethodName() + ".");

		// Testing HA Scenario / ZKCompletedCheckpointStore with incremental checkpoints
		StateBackendEnum stateBackendEnum = getStateBackend();
		if (ROCKSDB_INCREMENTAL_ZK.equals(stateBackendEnum)) {
			zkServer = new TestingServer();
			zkServer.start();
		}

		Configuration config = createClusterConfig();

		switch (stateBackendEnum) {
			case MEM:
				this.stateBackend = new MemoryStateBackend(MAX_MEM_STATE_SIZE, false);
				break;
			case FILE: {
				String backups = tempFolder.newFolder().getAbsolutePath();
				this.stateBackend = new FsStateBackend("file://" + backups, false);
				break;
			}
			case MEM_ASYNC:
				this.stateBackend = new MemoryStateBackend(MAX_MEM_STATE_SIZE, true);
				break;
			case FILE_ASYNC: {
				String backups = tempFolder.newFolder().getAbsolutePath();
				this.stateBackend = new FsStateBackend("file://" + backups, true);
				break;
			}
			case ROCKSDB_FULLY_ASYNC: {
				setupRocksDB(-1, false);
				break;
			}
			case ROCKSDB_INCREMENTAL:
				// Test RocksDB based timer service as well
				config.setString(
					RocksDBOptions.TIMER_SERVICE_FACTORY,
					RocksDBStateBackend.PriorityQueueStateType.ROCKSDB.toString());
				setupRocksDB(16, true);
				break;
			case ROCKSDB_INCREMENTAL_ZK: {
				setupRocksDB(16, true);
				break;
			}
			default:
				throw new IllegalStateException("No backend selected.");
		}
		return config;
	}

	private void setupRocksDB(int fileSizeThreshold, boolean incrementalCheckpoints) throws IOException {
		String rocksDb = tempFolder.newFolder().getAbsolutePath();
		String backups = tempFolder.newFolder().getAbsolutePath();
		// we use the fs backend with small threshold here to test the behaviour with file
		// references, not self contained byte handles
		RocksDBStateBackend rdb =
			new RocksDBStateBackend(
				new FsStateBackend(
					new Path("file://" + backups).toUri(), fileSizeThreshold),
				incrementalCheckpoints);
		rdb.setDbStoragePath(rocksDb);
		this.stateBackend = rdb;
	}

	protected Configuration createClusterConfig() throws IOException {
		TemporaryFolder temporaryFolder = new TemporaryFolder();
		temporaryFolder.create();
		final File haDir = temporaryFolder.newFolder();

		Configuration config = new Configuration();
		config.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "48m");
		// the default network buffers size (10% of heap max =~ 150MB) seems to much for this test case
		config.setString(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, String.valueOf(80L << 20)); // 80 MB
		config.setString(AkkaOptions.FRAMESIZE, String.valueOf(MAX_MEM_STATE_SIZE) + "b");

		if (zkServer != null) {
			config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
			config.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zkServer.getConnectString());
			config.setString(HighAvailabilityOptions.HA_STORAGE_PATH, haDir.toURI().toString());
		}
		return config;
	}

	@Before
	public void setupTestCluster() throws Exception {
		miniClusterResource = getMiniClusterResource();
		miniClusterResource.before();
	}

	@After
	public void stopTestCluster() throws IOException {
		if (miniClusterResource != null) {
			miniClusterResource.after();
			miniClusterResource = null;
		}

		if (zkServer != null) {
			zkServer.stop();
			zkServer = null;
		}

		//Prints a message when finishing a test method to avoid Travis' <tt>"Maven produced no output
		// for xxx seconds."</tt> messages.
		System.out.println(
			"Finished " + getClass().getCanonicalName() + "#" + name.getMethodName() + ".");
	}

	// ------------------------------------------------------------------------

	@Test
	public void testTumblingTimeWindow() {
		final int numElementsPerKey = numElementsPerKey();
		final int windowSize = windowSize();
		final int numKeys = numKeys();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
			env.getConfig().disableSysoutLogging();
			env.setStateBackend(this.stateBackend);
			env.getConfig().setUseSnapshotCompression(true);

			env
					.addSource(new FailingSource(new KeyedEventTimeGenerator(numKeys, windowSize), numElementsPerKey))
					.rebalance()
					.keyBy(0)
					.timeWindow(Time.of(windowSize, MILLISECONDS))
					.apply(new RichWindowFunction<Tuple2<Long, IntType>, Tuple4<Long, Long, Long, IntType>, Tuple, TimeWindow>() {

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
								Collector<Tuple4<Long, Long, Long, IntType>> out) {

							// validate that the function has been opened properly
							assertTrue(open);

							int sum = 0;
							long key = -1;

							for (Tuple2<Long, IntType> value : values) {
								sum += value.f1.value;
								key = value.f0;
							}

							final Tuple4<Long, Long, Long, IntType> result =
								new Tuple4<>(key, window.getStart(), window.getEnd(), new IntType(sum));
							out.collect(result);
						}
					})
				.addSink(new ValidatingSink<>(
					new SinkValidatorUpdateFun(numElementsPerKey),
					new SinkValidatorCheckFun(numKeys, numElementsPerKey, windowSize))).setParallelism(1);

			env.execute("Tumbling Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testTumblingTimeWindowWithKVStateMinMaxParallelism() {
		doTestTumblingTimeWindowWithKVState(PARALLELISM);
	}

	@Test
	public void testTumblingTimeWindowWithKVStateMaxMaxParallelism() {
		doTestTumblingTimeWindowWithKVState(1 << 15);
	}

	public void doTestTumblingTimeWindowWithKVState(int maxParallelism) {
		final int numElementsPerKey = numElementsPerKey();
		final int windowSize = windowSize();
		final int numKeys = numKeys();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setMaxParallelism(maxParallelism);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
			env.getConfig().disableSysoutLogging();
			env.setStateBackend(this.stateBackend);
			env.getConfig().setUseSnapshotCompression(true);

			env
					.addSource(new FailingSource(new KeyedEventTimeGenerator(numKeys, windowSize), numElementsPerKey))
					.rebalance()
					.keyBy(0)
					.timeWindow(Time.of(windowSize, MILLISECONDS))
					.apply(new RichWindowFunction<Tuple2<Long, IntType>, Tuple4<Long, Long, Long, IntType>, Tuple, TimeWindow>() {

						private boolean open = false;

						private ValueState<Integer> count;

						@Override
						public void open(Configuration parameters) {
							assertEquals(PARALLELISM, getRuntimeContext().getNumberOfParallelSubtasks());
							open = true;
							count = getRuntimeContext().getState(
									new ValueStateDescriptor<>("count", Integer.class, 0));
						}

						@Override
						public void apply(
								Tuple tuple,
								TimeWindow window,
								Iterable<Tuple2<Long, IntType>> values,
								Collector<Tuple4<Long, Long, Long, IntType>> out) throws Exception {

							// the window count state starts with the key, so that we get
							// different count results for each key
							if (count.value() == 0) {
								count.update(tuple.<Long>getField(0).intValue());
							}

							// validate that the function has been opened properly
							assertTrue(open);

							count.update(count.value() + 1);
							out.collect(new Tuple4<>(tuple.<Long>getField(0), window.getStart(), window.getEnd(), new IntType(count.value())));
						}
					})
				.addSink(new ValidatingSink<>(
					new CountingSinkValidatorUpdateFun(),
					new SinkValidatorCheckFun(numKeys, numElementsPerKey, windowSize))).setParallelism(1);

			env.execute("Tumbling Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSlidingTimeWindow() {
		final int numElementsPerKey = numElementsPerKey();
		final int windowSize = windowSize();
		final int windowSlide = windowSlide();
		final int numKeys = numKeys();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setMaxParallelism(2 * PARALLELISM);
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
			env.getConfig().disableSysoutLogging();
			env.setStateBackend(this.stateBackend);
			env.getConfig().setUseSnapshotCompression(true);

			env
					.addSource(new FailingSource(new KeyedEventTimeGenerator(numKeys, windowSlide), numElementsPerKey))
					.rebalance()
					.keyBy(0)
					.timeWindow(Time.of(windowSize, MILLISECONDS), Time.of(windowSlide, MILLISECONDS))
					.apply(new RichWindowFunction<Tuple2<Long, IntType>, Tuple4<Long, Long, Long, IntType>, Tuple, TimeWindow>() {

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
								Collector<Tuple4<Long, Long, Long, IntType>> out) {

							// validate that the function has been opened properly
							assertTrue(open);

							int sum = 0;
							long key = -1;

							for (Tuple2<Long, IntType> value : values) {
								sum += value.f1.value;
								key = value.f0;
							}
							final Tuple4<Long, Long, Long, IntType> output =
								new Tuple4<>(key, window.getStart(), window.getEnd(), new IntType(sum));
							out.collect(output);
						}
					})
				.addSink(new ValidatingSink<>(
					new SinkValidatorUpdateFun(numElementsPerKey),
					new SinkValidatorCheckFun(numKeys, numElementsPerKey, windowSlide))).setParallelism(1);

			env.execute("Tumbling Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPreAggregatedTumblingTimeWindow() {
		final int numElementsPerKey = numElementsPerKey();
		final int windowSize = windowSize();
		final int numKeys = numKeys();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
			env.getConfig().disableSysoutLogging();
			env.setStateBackend(this.stateBackend);
			env.getConfig().setUseSnapshotCompression(true);

			env
					.addSource(new FailingSource(new KeyedEventTimeGenerator(numKeys, windowSize), numElementsPerKey))
					.rebalance()
					.keyBy(0)
					.timeWindow(Time.of(windowSize, MILLISECONDS))
					.reduce(
							new ReduceFunction<Tuple2<Long, IntType>>() {

								@Override
								public Tuple2<Long, IntType> reduce(
										Tuple2<Long, IntType> a,
										Tuple2<Long, IntType> b) {
									return new Tuple2<>(a.f0, new IntType(a.f1.value + b.f1.value));
								}
							},
							new RichWindowFunction<Tuple2<Long, IntType>, Tuple4<Long, Long, Long, IntType>, Tuple, TimeWindow>() {

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
								Iterable<Tuple2<Long, IntType>> input,
								Collector<Tuple4<Long, Long, Long, IntType>> out) {

							// validate that the function has been opened properly
							assertTrue(open);

							for (Tuple2<Long, IntType> in: input) {
								final Tuple4<Long, Long, Long, IntType> output = new Tuple4<>(in.f0,
									window.getStart(),
									window.getEnd(),
									in.f1);
								out.collect(output);
							}
						}
					})
				.addSink(new ValidatingSink<>(
					new SinkValidatorUpdateFun(numElementsPerKey),
					new SinkValidatorCheckFun(numKeys, numElementsPerKey, windowSize))).setParallelism(1);

			env.execute("Tumbling Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPreAggregatedSlidingTimeWindow() {
		final int numElementsPerKey = numElementsPerKey();
		final int windowSize = windowSize();
		final int windowSlide = windowSlide();
		final int numKeys = numKeys();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, 0));
			env.getConfig().disableSysoutLogging();
			env.setStateBackend(this.stateBackend);
			env.getConfig().setUseSnapshotCompression(true);

			env
					.addSource(new FailingSource(new KeyedEventTimeGenerator(numKeys, windowSlide), numElementsPerKey))
					.rebalance()
					.keyBy(0)
					.timeWindow(Time.of(windowSize, MILLISECONDS), Time.of(windowSlide, MILLISECONDS))
					.reduce(
							new ReduceFunction<Tuple2<Long, IntType>>() {

								@Override
								public Tuple2<Long, IntType> reduce(
										Tuple2<Long, IntType> a,
										Tuple2<Long, IntType> b) {

									// validate that the function has been opened properly
									return new Tuple2<>(a.f0, new IntType(a.f1.value + b.f1.value));
								}
							},
							new RichWindowFunction<Tuple2<Long, IntType>, Tuple4<Long, Long, Long, IntType>, Tuple, TimeWindow>() {

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
					.addSink(new ValidatingSink<>(
						new SinkValidatorUpdateFun(numElementsPerKey),
						new SinkValidatorCheckFun(numKeys, numElementsPerKey, windowSlide))).setParallelism(1);

			env.execute("Tumbling Window Test");
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * For validating the stateful window counts.
	 */
	static class CountingSinkValidatorUpdateFun
		implements ValidatingSink.CountUpdater<Tuple4<Long, Long, Long, IntType>> {

		@Override
		public void updateCount(Tuple4<Long, Long, Long, IntType> value, Map<Long, Integer> windowCounts) {

			windowCounts.merge(value.f0, 1, (a, b) -> a + b);

			// verify the contents of that window, the contents should be:
			// (key + num windows so far)
			assertEquals("Window counts don't match for key " + value.f0 + ".", value.f0.intValue() + windowCounts.get(value.f0), value.f3.value);
		}
	}

	//------------------------------------

	static class SinkValidatorUpdateFun implements ValidatingSink.CountUpdater<Tuple4<Long, Long, Long, IntType>> {

		private final int elementsPerKey;

		SinkValidatorUpdateFun(int elementsPerKey) {
			this.elementsPerKey = elementsPerKey;
		}

		@Override
		public void updateCount(Tuple4<Long, Long, Long, IntType> value, Map<Long, Integer> windowCounts) {
			// verify the contents of that window, Tuple4.f1 and .f2 are the window start/end
			// the sum should be "sum (start .. end-1)"

			int expectedSum = 0;
			// we shorten the range if it goes beyond elementsPerKey, because those are "incomplete" sliding windows
			long countUntil = Math.min(value.f2, elementsPerKey);
			for (long i = value.f1; i < countUntil; i++) {
				// only sum up positive vals, to filter out the negative start of the
				// first sliding windows
				if (i > 0) {
					expectedSum += i;
				}
			}

			assertEquals("Window start: " + value.f1 + " end: " + value.f2, expectedSum, value.f3.value);

			windowCounts.merge(value.f0, 1, (val, increment) -> val + increment);
		}
	}

	static class SinkValidatorCheckFun implements ValidatingSink.ResultChecker {

		private final int numKeys;
		private final int numWindowsExpected;

		SinkValidatorCheckFun(int numKeys, int elementsPerKey, int elementsPerWindow) {
			this.numKeys = numKeys;
			this.numWindowsExpected = elementsPerKey / elementsPerWindow;
		}

		@Override
		public boolean checkResult(Map<Long, Integer> windowCounts) {
			if (windowCounts.size() == numKeys) {
				for (Integer windowCount : windowCounts.values()) {
					if (windowCount < numWindowsExpected) {
						return false;
					}
				}
				return true;
			}
			return false;
		}
	}

	static class KeyedEventTimeGenerator implements FailingSource.EventEmittingGenerator {

		private final int keyUniverseSize;
		private final int watermarkTrailing;

		public KeyedEventTimeGenerator(int keyUniverseSize, int numElementsPerWindow) {
			this.keyUniverseSize = keyUniverseSize;
			// we let the watermark a bit behind, so that there can be in-flight timers that required checkpointing
			// to include correct timer snapshots in our testing.
			this.watermarkTrailing = 4 * numElementsPerWindow / 3;
		}

		@Override
		public void emitEvent(SourceFunction.SourceContext<Tuple2<Long, IntType>> ctx, int eventSequenceNo) {
			final IntType intTypeNext = new IntType(eventSequenceNo);
			for (long i = 0; i < keyUniverseSize; i++) {
				final Tuple2<Long, IntType> generatedEvent = new Tuple2<>(i, intTypeNext);
				ctx.collectWithTimestamp(generatedEvent, eventSequenceNo);
			}

			ctx.emitWatermark(new Watermark(eventSequenceNo - watermarkTrailing));
		}
	}

	private int numElementsPerKey() {
		switch (this.stateBackendEnum) {
			case ROCKSDB_FULLY_ASYNC:
			case ROCKSDB_INCREMENTAL:
			case ROCKSDB_INCREMENTAL_ZK:
				return 3000;
			default:
				return 300;
		}
	}

	private int windowSize() {
		switch (this.stateBackendEnum) {
			case ROCKSDB_FULLY_ASYNC:
			case ROCKSDB_INCREMENTAL:
			case ROCKSDB_INCREMENTAL_ZK:
				return 1000;
			default:
				return 100;
		}
	}

	private int windowSlide() {
		return 100;
	}

	private int numKeys() {
		switch (this.stateBackendEnum) {
			case ROCKSDB_FULLY_ASYNC:
			case ROCKSDB_INCREMENTAL:
			case ROCKSDB_INCREMENTAL_ZK:
				return 100;
			default:
				return 20;
		}
	}
}
