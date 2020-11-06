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

import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The base class for the File Sink IT Case in different execution mode.
 */
public abstract class FileSinkITBase extends TestLogger {

	protected static final  int NUM_SOURCES = 4;

	protected static final int NUM_SINKS = 3;

	protected static final int NUM_RECORDS = 10000;

	protected static final int NUM_BUCKETS = 4;

	protected static final double FAILOVER_RATIO = 0.4;

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	@Parameterized.Parameter
	public boolean triggerFailover;

	@Parameterized.Parameters(name = "triggerFailover = {0}")
	public static Collection<Object[]> params() {
		return Arrays.asList(
				new Object[]{false},
				new Object[]{true});
	}

	@Test
	public void testFileSink() throws Exception {
		String path = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

		JobGraph jobGraph = createJobGraph(path);

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
		}

		checkResult(path);
	}

	protected abstract JobGraph createJobGraph(String path);

	protected FileSink<Integer> createFileSink(String path) {
		return FileSink
				.forRowFormat(new Path(path), new IntEncoder())
				.withBucketAssigner(new ModuloBucketAssigner())
				.withRollingPolicy(new PartSizeAndCheckpointRollingPolicy(1024))
				.build();
	}

	private void checkResult(String path) throws Exception {
		File dir = new File(path);
		String[] subDirNames = dir.list();
		assertNotNull(subDirNames);

		Arrays.sort(subDirNames, Comparator.comparingInt(Integer::parseInt));
		assertEquals(NUM_BUCKETS, subDirNames.length);
		for (int i = 0; i < NUM_BUCKETS; ++i) {
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

			int expectedCount = NUM_RECORDS / NUM_BUCKETS +
					(i < NUM_RECORDS % NUM_BUCKETS ? 1 : 0);
			assertEquals(expectedCount, counts.size());

			for (int j = i; j < NUM_RECORDS; j += NUM_BUCKETS) {
				assertEquals(
						"The record " + j + " should occur " + NUM_SOURCES + " times, " +
								" but only occurs " + counts.getOrDefault(j, 0) + "time",
						NUM_SOURCES,
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

	private static class ModuloBucketAssigner implements BucketAssigner<Integer, String> {

		@Override
		public String getBucketId(Integer element, Context context) {
			return Integer.toString(element % NUM_BUCKETS);
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
				long currentTime) {
			return false;
		}
	}

}
