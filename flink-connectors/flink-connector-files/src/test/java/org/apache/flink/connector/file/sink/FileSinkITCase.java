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

package org.apache.flink.connector.file.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.StreamSource;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the functionality of the {@link FileSink }.
 */
@RunWith(Parameterized.class)
public class FileSinkITCase {

	private static final  int NUMBER_OF_SOURCE = 4;

	private static final int NUMBER_OF_SINK = 3;

	private static final int NUMBER_RECORD = 10000;

	private static final int NUMBER_BUCKET = 4;

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	@Parameterized.Parameter(0)
	public RuntimeExecutionMode executionMode;

	@Parameterized.Parameter(1)
	public boolean triggerFailover;

	@Parameterized.Parameters(name = "executionMode = {0}, triggerFailover = {1}")
	public static Collection<Object[]> params() {
		return Arrays.<Object[]>asList(
				new Object[]{RuntimeExecutionMode.STREAMING, true},
				new Object[]{RuntimeExecutionMode.BATCH, false},
				new Object[]{RuntimeExecutionMode.STREAMING, true},
				new Object[]{RuntimeExecutionMode.BATCH, true});
	}

	@Test
	public void testFileSink() throws Exception {
		String path = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
		String sourceLatchId = UUID.randomUUID().toString();
		TestSource.LATCH_MAP.put(sourceLatchId, new CountDownLatch(NUMBER_OF_SOURCE));

		JobGraph jobGraph = createJobGraph(path, sourceLatchId);

		final Configuration config = new Configuration();
		config.setString(RestOptions.BIND_PORT, "18081-19000");
		final MiniClusterConfiguration cfg = new MiniClusterConfiguration.Builder()
				.setNumTaskManagers(1)
				.setNumSlotsPerTaskManager(4)
				.setConfiguration(config)
				.build();

		try (MiniCluster miniCluster = new MiniCluster(cfg)) {
			miniCluster.start();
			miniCluster.executeJobBlocking(jobGraph);
		} finally {
			TestSource.LATCH_MAP.remove(sourceLatchId);
		}

		checkResult(path);
	}

	private JobGraph createJobGraph(String path, String sourceLatchId) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		Configuration config = new Configuration();
		config.set(ExecutionOptions.RUNTIME_MODE, executionMode);
		env.configure(config, getClass().getClassLoader());

		if (executionMode.equals(RuntimeExecutionMode.STREAMING)) {
			env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
		}

		if (triggerFailover) {
			env.setRestartStrategy(RestartStrategies.fixedDelayRestart(1, Time.milliseconds(100)));
		} else {
			env.setRestartStrategy(RestartStrategies.noRestart());
		}

		// Create a testing job with a bounded legacy source in a bit hacky way.
		StreamSource<Integer, ?> sourceOperator = new StreamSource<>(new TestSource(
				sourceLatchId,
				NUMBER_RECORD,
				triggerFailover,
				executionMode));
		DataStreamSource<Integer> source = new DataStreamSource<>(
				env,
				BasicTypeInfo.INT_TYPE_INFO,
				sourceOperator,
				true,
				"Source",
				Boundedness.BOUNDED);
		FileSink<Integer> fileSink = FileSink
				.forRowFormat(new Path(path), new IntEncoder())
				.withBucketAssigner(new ModuleBucketAssigner())
				.withRollingPolicy(new PartSizeAndCheckpointRollingPolicy(1024))
				.build();
		source.setParallelism(NUMBER_OF_SOURCE)
				.rebalance()
				.map(new FailoverMap(NUMBER_RECORD, triggerFailover))
				.setParallelism(NUMBER_OF_SINK)
				.sinkTo(fileSink)
				.setParallelism(NUMBER_OF_SINK);

		StreamGraph streamGraph = env.getStreamGraph();
		return streamGraph.getJobGraph();
	}

	private void checkResult(String path) throws Exception {
		File dir = new File(path);
		String[] subDirNames = dir.list();
		assertNotNull(subDirNames);

		Arrays.sort(subDirNames, Comparator.comparingInt(Integer::parseInt));
		assertEquals(NUMBER_BUCKET, subDirNames.length);
		for (int i = 0; i < NUMBER_BUCKET; ++i) {
			assertEquals(Integer.toString(i), subDirNames[i]);

			// now check its content
			File bucketDir = new File(path, subDirNames[i]);
			assertTrue(
					bucketDir.getAbsolutePath() + " Should be a existing directory",
					bucketDir.isDirectory());

			Map<Integer, Integer> counts = new HashMap<>();
			File[] files = bucketDir.listFiles(f -> !f.getName().startsWith("."));
			assertNotNull(files);

			for (File file : files) {
				assertTrue(file.isFile());

				try (DataInputStream dataInputStream = new DataInputStream(new FileInputStream(file))) {
					while (true) {
						int value = dataInputStream.readInt();
						counts.compute(value, (k, v) -> v == null ? 1 : v + 1);
					}
				} catch (EOFException e) {
					// End the reading
				}
			}

			int expectedCount = NUMBER_RECORD / NUMBER_BUCKET +
					(i < NUMBER_RECORD % NUMBER_BUCKET ? 1 : 0);
			assertEquals(expectedCount, counts.size());

			for (int j = i; j < NUMBER_RECORD; j += NUMBER_BUCKET) {
				assertEquals(
						"The record " + j + " should occur " + NUMBER_OF_SOURCE + " times, " +
								" but only occurs " + counts.getOrDefault(j, 0) + "time",
						NUMBER_OF_SOURCE,
						counts.getOrDefault(j, 0).intValue());
			}
		}
	}

	private static class IntEncoder implements Encoder<Integer> {

		@Override
		public void encode(Integer element, OutputStream stream) throws IOException {
			stream.write(ByteBuffer.allocate(4).putInt(element).array());
			stream.flush();
		}
	}

	private static class ModuleBucketAssigner implements BucketAssigner<Integer, String> {

		@Override
		public String getBucketId(Integer element, Context context) {
			return Integer.toString(element % NUMBER_BUCKET);
		}

		@Override
		public SimpleVersionedSerializer<String> getSerializer() {
			return SimpleVersionedStringSerializer.INSTANCE;
		}
	}

	private static class PartSizeAndCheckpointRollingPolicy
			extends CheckpointRollingPolicy<Integer, String> {

		private final long maxPartSize;

		public PartSizeAndCheckpointRollingPolicy(long maxPartSize) {
			this.maxPartSize = maxPartSize;
		}

		@Override
		public boolean shouldRollOnEvent(
				PartFileInfo<String> partFileState,
				Integer element) throws IOException {
			return partFileState.getSize() >= maxPartSize;
		}

		@Override
		public boolean shouldRollOnProcessingTime(
				PartFileInfo<String> partFileState,
				long currentTime) throws IOException {
			return false;
		}
	}

	private static class TestSource extends RichParallelSourceFunction<Integer>
			implements CheckpointListener, CheckpointedFunction {

		private static final Map<String, CountDownLatch> LATCH_MAP = new ConcurrentHashMap<>();

		private final String latchId;

		private final int numberOfRecords;

		private final boolean triggerFailover;

		private final RuntimeExecutionMode mode;

		private volatile boolean isCanceled;

		private volatile boolean snapshottedAfterAllRecordsOutput;

		private volatile boolean isWaitingCheckpointComplete;

		public TestSource(
				String latchId,
				int numberOfRecords,
				boolean triggerFailover,
				RuntimeExecutionMode mode) {
			this.latchId = latchId;
			this.numberOfRecords = numberOfRecords;
			this.triggerFailover = triggerFailover;
			this.mode = mode;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			for (int i = 0; i < numberOfRecords && !isCanceled; ++i) {
				ctx.collect(i);
			}

			if (mode.equals(RuntimeExecutionMode.STREAMING) &&
					(!triggerFailover || getRuntimeContext().getAttemptNumber() == 1)) {
				isWaitingCheckpointComplete = true;
				CountDownLatch latch = LATCH_MAP.get(latchId);
				latch.await();
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) {
			if (isWaitingCheckpointComplete) {
				snapshottedAfterAllRecordsOutput = true;
			}
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			if (isWaitingCheckpointComplete && snapshottedAfterAllRecordsOutput) {
				CountDownLatch latch = LATCH_MAP.get(latchId);
				latch.countDown();
			}
		}

		@Override
		public void cancel() {
			isCanceled = true;
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {

		}
	}

	private static class FailoverMap extends RichMapFunction<Integer, Integer> {

		private final int maxNumber;

		private final boolean triggerFailover;

		public FailoverMap(int maxNumber, boolean triggerFailover) {
			this.maxNumber = maxNumber;
			this.triggerFailover = triggerFailover;
		}

		@Override
		public Integer map(Integer value) {
			if (triggerFailover &&
					getRuntimeContext().getIndexOfThisSubtask() == 0 &&
					getRuntimeContext().getAttemptNumber() == 0 &&
					value >= 0.4 * maxNumber) {
				throw new RuntimeException("Designated Failure");
			}

			return value;
		}
	}
}
