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
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.SuccessException;
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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.test.checkpointing.AbstractEventTimeWindowCheckpointingITCase.StateBackendEnum.ROCKSDB_INCREMENTAL_ZK;
import static org.apache.flink.test.util.TestUtils.tryExecute;
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
public abstract class AbstractEventTimeWindowCheckpointingITCase extends TestLogger {

	private static final int MAX_MEM_STATE_SIZE = 20 * 1024 * 1024;
	private static final int PARALLELISM = 4;

	private TestingServer zkServer;

	public MiniClusterResource miniClusterResource;

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	@Rule
	public TestName name = new TestName();

	private AbstractStateBackend stateBackend;

	enum StateBackendEnum {
		MEM, FILE, ROCKSDB_FULLY_ASYNC, ROCKSDB_INCREMENTAL, ROCKSDB_INCREMENTAL_ZK, MEM_ASYNC, FILE_ASYNC
	}

	protected abstract StateBackendEnum getStateBackend();

	protected final MiniClusterResource getMiniClusterResource() {
		return new MiniClusterResource(
			new MiniClusterResource.MiniClusterResourceConfiguration(
				getConfigurationSafe(),
				2,
				PARALLELISM / 2));
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
				String rocksDb = tempFolder.newFolder().getAbsolutePath();
				String backups = tempFolder.newFolder().getAbsolutePath();
				RocksDBStateBackend rdb = new RocksDBStateBackend(new FsStateBackend("file://" + backups));
				rdb.setDbStoragePath(rocksDb);
				this.stateBackend = rdb;
				break;
			}
			case ROCKSDB_INCREMENTAL:
			case ROCKSDB_INCREMENTAL_ZK: {
				String rocksDb = tempFolder.newFolder().getAbsolutePath();
				String backups = tempFolder.newFolder().getAbsolutePath();
				// we use the fs backend with small threshold here to test the behaviour with file
				// references, not self contained byte handles
				RocksDBStateBackend rdb =
					new RocksDBStateBackend(
						new FsStateBackend(
							new Path("file://" + backups).toUri(), 16),
						true);
				rdb.setDbStoragePath(rocksDb);
				this.stateBackend = rdb;
				break;
			}
			default:
				throw new IllegalStateException("No backend selected.");
		}
		return config;
	}

	protected Configuration createClusterConfig() throws IOException {
		TemporaryFolder temporaryFolder = new TemporaryFolder();
		temporaryFolder.create();
		final File haDir = temporaryFolder.newFolder();

		Configuration config = new Configuration();
		config.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 48L);
		// the default network buffers size (10% of heap max =~ 150MB) seems to much for this test case
		config.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, 80L << 20); // 80 MB
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
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
			env.getConfig().disableSysoutLogging();
			env.setStateBackend(this.stateBackend);
			env.getConfig().setUseSnapshotCompression(true);

			env
					.addSource(new FailingSource(numKeys, numElementsPerKey, numElementsPerKey / 3))
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
							out.collect(new Tuple4<>(key, window.getStart(), window.getEnd(), new IntType(sum)));
						}
					})
					.addSink(new ValidatingSink(numKeys, numElementsPerKey / windowSize)).setParallelism(1);

			tryExecute(env, "Tumbling Window Test");
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
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setMaxParallelism(maxParallelism);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
			env.getConfig().disableSysoutLogging();
			env.setStateBackend(this.stateBackend);
			env.getConfig().setUseSnapshotCompression(true);

			env
					.addSource(new FailingSource(numKeys, numElementsPerKey, numElementsPerKey / 3))
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
					.addSink(new CountValidatingSink(numKeys, numElementsPerKey / windowSize)).setParallelism(1);

			tryExecute(env, "Tumbling Window Test");
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
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setMaxParallelism(2 * PARALLELISM);
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
			env.getConfig().disableSysoutLogging();
			env.setStateBackend(this.stateBackend);
			env.getConfig().setUseSnapshotCompression(true);

			env
					.addSource(new FailingSource(numKeys, numElementsPerKey, numElementsPerKey / 3))
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
							out.collect(new Tuple4<>(key, window.getStart(), window.getEnd(), new IntType(sum)));
						}
					})
					.addSink(new ValidatingSink(numKeys, numElementsPerKey / windowSlide)).setParallelism(1);

			tryExecute(env, "Tumbling Window Test");
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
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
			env.getConfig().disableSysoutLogging();
			env.setStateBackend(this.stateBackend);
			env.getConfig().setUseSnapshotCompression(true);

			env
					.addSource(new FailingSource(numKeys, numElementsPerKey, numElementsPerKey / 3))
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
								out.collect(new Tuple4<>(in.f0,
										window.getStart(),
										window.getEnd(),
										in.f1));
							}
						}
					})
					.addSink(new ValidatingSink(numKeys, numElementsPerKey / windowSize)).setParallelism(1);

			tryExecute(env, "Tumbling Window Test");
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
		FailingSource.reset();

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(PARALLELISM);
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
			env.enableCheckpointing(100);
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 0));
			env.getConfig().disableSysoutLogging();
			env.setStateBackend(this.stateBackend);
			env.getConfig().setUseSnapshotCompression(true);

			env
					.addSource(new FailingSource(numKeys, numElementsPerKey, numElementsPerKey / 3))
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
					.addSink(new ValidatingSink(numKeys, numElementsPerKey / windowSlide)).setParallelism(1);

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

		private final int numKeys;
		private final int numElementsToEmit;
		private final int failureAfterNumElements;

		private volatile int numElementsEmitted;
		private volatile int numSuccessfulCheckpoints;
		private volatile boolean running = true;

		private FailingSource(int numKeys, int numElementsToEmitPerKey, int failureAfterNumElements) {
			this.numKeys = numKeys;
			this.numElementsToEmit = numElementsToEmitPerKey;
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
						for (long i = 0; i < numKeys; i++) {
							ctx.collectWithTimestamp(new Tuple2<Long, IntType>(i, new IntType(next)), next);
						}
						ctx.emitWatermark(new Watermark(next));
					}
				}
				else {

					// if our work is done, delay a bit to prevent busy waiting
					Thread.sleep(1);
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

	private static class ValidatingSink extends RichSinkFunction<Tuple4<Long, Long, Long, IntType>>
			implements ListCheckpointed<HashMap<Long, Integer>> {

		private final HashMap<Long, Integer> windowCounts = new HashMap<>();

		private final int numKeys;
		private final int numWindowsExpected;

		private ValidatingSink(int numKeys, int numWindowsExpected) {
			this.numKeys = numKeys;
			this.numWindowsExpected = numWindowsExpected;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			// this sink can only work with DOP 1
			assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());

			// it can happen that a checkpoint happens when the complete success state is
			// already set. In that case we restart with the final state and would never
			// finish because no more elements arrive.
			if (windowCounts.size() == numKeys) {
				boolean seenAll = true;
				for (Integer windowCount: windowCounts.values()) {
					if (windowCount != numWindowsExpected) {
						seenAll = false;
						break;
					}
				}
				if (seenAll) {
					throw new SuccessException();
				}
			}
		}

		@Override
		public void close() throws Exception {
			boolean seenAll = true;
			if (windowCounts.size() == numKeys) {
				for (Integer windowCount: windowCounts.values()) {
					if (windowCount < numWindowsExpected) {
						seenAll = false;
						break;
					}
				}
			}
			assertTrue("The sink must see all expected windows.", seenAll);
		}

		@Override
		public void invoke(Tuple4<Long, Long, Long, IntType> value) throws Exception {

			// verify the contents of that window, Tuple4.f1 and .f2 are the window start/end
			// the sum should be "sum (start .. end-1)"

			int expectedSum = 0;
			for (long i = value.f1; i < value.f2; i++) {
				// only sum up positive vals, to filter out the negative start of the
				// first sliding windows
				if (i > 0) {
					expectedSum += i;
				}
			}

			assertEquals("Window start: " + value.f1 + " end: " + value.f2, expectedSum, value.f3.value);

			Integer curr = windowCounts.get(value.f0);
			if (curr != null) {
				windowCounts.put(value.f0, curr + 1);
			}
			else {
				windowCounts.put(value.f0, 1);
			}

			if (windowCounts.size() == numKeys) {
				boolean seenAll = true;
				for (Integer windowCount: windowCounts.values()) {
					if (windowCount < numWindowsExpected) {
						seenAll = false;
						break;
					} else if (windowCount > numWindowsExpected) {
						fail("Window count to high: " + windowCount);
					}
				}

				if (seenAll) {
					// exit
					throw new SuccessException();
				}

			}
		}

		@Override
		public List<HashMap<Long, Integer>> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(this.windowCounts);
		}

		@Override
		public void restoreState(List<HashMap<Long, Integer>> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}
			windowCounts.putAll(state.get(0));
		}
	}

	// Sink for validating the stateful window counts
	private static class CountValidatingSink extends RichSinkFunction<Tuple4<Long, Long, Long, IntType>>
			implements ListCheckpointed<HashMap<Long, Integer>> {

		private final HashMap<Long, Integer> windowCounts = new HashMap<>();

		private final int numKeys;
		private final int numWindowsExpected;

		private CountValidatingSink(int numKeys, int numWindowsExpected) {
			this.numKeys = numKeys;
			this.numWindowsExpected = numWindowsExpected;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			// this sink can only work with DOP 1
			assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());
		}

		@Override
		public void close() throws Exception {
			boolean seenAll = true;
			if (windowCounts.size() == numKeys) {
				for (Integer windowCount: windowCounts.values()) {
					if (windowCount < numWindowsExpected) {
						seenAll = false;
						break;
					}
				}
			}
			assertTrue("The source must see all expected windows.", seenAll);
		}

		@Override
		public void invoke(Tuple4<Long, Long, Long, IntType> value) throws Exception {

			Integer curr = windowCounts.get(value.f0);
			if (curr != null) {
				windowCounts.put(value.f0, curr + 1);
			}
			else {
				windowCounts.put(value.f0, 1);
			}

			// verify the contents of that window, the contents should be:
			// (key + num windows so far)

			assertEquals("Window counts don't match for key " + value.f0 + ".", value.f0.intValue() + windowCounts.get(value.f0), value.f3.value);

			boolean seenAll = true;
			if (windowCounts.size() == numKeys) {
				for (Integer windowCount: windowCounts.values()) {
					if (windowCount < numWindowsExpected) {
						seenAll = false;
						break;
					} else if (windowCount > numWindowsExpected) {
						fail("Window count to high: " + windowCount);
					}
				}

				if (seenAll) {
					// exit
					throw new SuccessException();
				}

			}
		}

		@Override
		public List<HashMap<Long, Integer>> snapshotState(long checkpointId, long timestamp) throws Exception {
			return Collections.singletonList(this.windowCounts);
		}

		@Override
		public void restoreState(List<HashMap<Long, Integer>> state) throws Exception {
			if (state.isEmpty() || state.size() > 1) {
				throw new RuntimeException("Test failed due to unexpected recovered state size " + state.size());
			}
			this.windowCounts.putAll(state.get(0));
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static class IntType {

		public int value;

		public IntType() {}

		public IntType(int value) {
			this.value = value;
		}
	}

	protected int numElementsPerKey() {
		return 300;
	}

	protected int windowSize() {
		return 100;
	}

	protected int windowSlide() {
		return 100;
	}

	protected int numKeys() {
		return 20;
	}
}
