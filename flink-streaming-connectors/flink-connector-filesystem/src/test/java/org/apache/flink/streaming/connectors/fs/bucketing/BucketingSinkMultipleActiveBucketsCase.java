/**
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
package org.apache.flink.streaming.connectors.fs.bucketing;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.util.Collector;
import org.apache.flink.util.NetUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * Tests for {@link BucketingSink}. These
 * tests test that the sink can maintain state for multiple active buckets and files at once,
 * and that buckets are cleaned up and closed when they become inactive.
 */
public class BucketingSinkMultipleActiveBucketsCase extends StreamingMultipleProgramsTestBase {

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	private static MiniDFSCluster hdfsCluster;
	private static org.apache.hadoop.fs.FileSystem dfs;
	private static String hdfsURI;


	@BeforeClass
	public static void createHDFS() throws IOException {
		Configuration conf = new Configuration();

		File dataDir = tempFolder.newFolder();

		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dataDir.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		hdfsCluster = builder.build();

		dfs = hdfsCluster.getFileSystem();

		hdfsURI = "hdfs://"
			+ NetUtils.hostAndPortToUrlString(hdfsCluster.getURI().getHost(), hdfsCluster.getNameNodePort())
			+ "/";
	}

	@AfterClass
	public static void destroyHDFS() {
		hdfsCluster.shutdown();
	}

	/**
	 * This uses a custom bucketing function which determines the bucket from the input.
	 */
	@Test
	public void testCustomBucketing() throws Exception {
		final int NUM_IDS = 4;
		final int NUM_ELEMENTS = 20;

		final String outPath = hdfsURI + "/rolling-out";

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Tuple2<Integer, String>> source = env.addSource(new SimpleSourceFunction(
			NUM_IDS, NUM_ELEMENTS))
			.broadcast();

		DataStream<Tuple2<Integer, String>> mapped = source
			.flatMap(new RichFlatMapFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
				private static final long serialVersionUID = 1L;

				int count = 0;

				@Override
				public void flatMap(Tuple2<Integer, String> value,
									Collector<Tuple2<Integer, String>> out) throws Exception {
					out.collect(value);
					count++;
				}

			});

		BucketingSink<Tuple2<Integer, String>> sink = new BucketingSink<Tuple2<Integer, String>>(outPath)
			.setBucketer(new CustomBucketer())
			.setPartPrefix("part")
			.setPendingPrefix("")
			.setPendingSuffix("");

		mapped.addSink(sink);

		env.execute("BucketingSink Custom Bucketing Test");

		RemoteIterator<LocatedFileStatus> files = dfs.listFiles(new Path(outPath), true);

		// we should have 4 buckets, with 1 file each
		int numFiles = 0;
		while (files.hasNext()) {
			LocatedFileStatus file = files.next();
			if (file.getPath().getName().startsWith("part")) {
				numFiles++;
			}
		}

		Assert.assertEquals(4, numFiles);
	}

	// we use this to synchronize the clock changes to elements being processed
	private final static MultiShotLatch latch1 = new MultiShotLatch();
	private final static MultiShotLatch latch2 = new MultiShotLatch();

	/**
	 * This uses a custom bucketing function which determines the bucket from the input.
	 * We use a simulated clock to reduce the number of buckets being written to over time.
	 * This causes buckets to become 'inactive' and their file parts 'closed' by the sink.
	 */
	@Test
	public void testCustomBucketingInactiveBucketCleanup() throws Exception {
		final int STEP1_NUM_IDS = 4;
		final int STEP2_NUM_IDS = 2;
		final int STEP1_NUM_ELEMENTS = 20;
		final int STEP2_NUM_ELEMENTS = 20;

		final String outPath = hdfsURI + "/rolling-out";

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Tuple2<Integer, String>> source = env.addSource(new SteppedSourceFunction(
			STEP1_NUM_IDS, STEP1_NUM_ELEMENTS, STEP2_NUM_IDS, STEP2_NUM_ELEMENTS))
			.broadcast();

		// the parallel flatMap is chained to the sink, so it can trigger step 2 by firing the latch
		DataStream<Tuple2<Integer, String>> mapped = source
			.flatMap(new RichFlatMapFunction<Tuple2<Integer, String>, Tuple2<Integer, String>>() {
				private static final long serialVersionUID = 1L;

				int count = 0;

				@Override
				public void flatMap(Tuple2<Integer, String> value,
									Collector<Tuple2<Integer, String>> out) throws Exception {
					out.collect(value);
					count++;
					if (count == STEP1_NUM_ELEMENTS) {
						latch1.trigger();
					}
					if (count == STEP1_NUM_ELEMENTS + STEP2_NUM_ELEMENTS) {
						latch2.trigger();
					}
				}

			});

		BucketingSink<Tuple2<Integer, String>> sink = new BucketingSink<Tuple2<Integer, String>>(outPath) {
			// remove close functionality to prevent in.progress files from being renamed when the test job ends
			@Override
			public void close() throws Exception {}
		};

		sink.setBucketer(new CustomBucketer())
			.setPartPrefix("part")
			.setPendingPrefix("")
			.setPendingSuffix("")
			.setInactiveBucketCheckInterval(5 * 60 * 1000L)
			.setInactiveBucketThreshold(5 * 60 * 1000L);

//		BucketingSink.setClock(new ModifyableClock());

		mapped.addSink(sink);

		env.execute("BucketingSink Custom Bucketing Cleanup Test");

		RemoteIterator<LocatedFileStatus> files = dfs.listFiles(new Path(outPath), true);

		// we should have 4 buckets, with 1 file each
		// 2 of these buckets should have been finalised due to becoming inactive
		int numFiles = 0;
		int numInProgress = 0;
		while (files.hasNext()) {
			LocatedFileStatus file = files.next();
			System.out.println(file.getPath().toString());
			if (file.getPath().toString().contains("in-progress")) {
				numInProgress++;
			}
			numFiles++;
		}

		Assert.assertEquals(4, numFiles);
		Assert.assertEquals(2, numInProgress);
	}

	/*
	 * A bucketing function which uses the first field in a Tuple2<Integer, String> as the bucket subdirectory
	 */
	private static class CustomBucketer implements Bucketer<Tuple2<Integer, String>> {

		@Override
		public Path getBucketPath(Clock clock, Path basePath, Tuple2<Integer, String> element) {
			return new Path(basePath + "/" + element.f0.toString());
		}

	}

	/*
	 * Source function which produces tuples with an integer id and string value.
	 */
	private static class SimpleSourceFunction implements SourceFunction<Tuple2<Integer, String>> {
		private static final long serialVersionUID = 1L;

		private volatile boolean running = true;

		private final int numIds;
		private final int numElements;

		public SimpleSourceFunction(int numIds, int numElements) {
			this.numIds = numIds;
			this.numElements = numElements;
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
			for (int i = 0; i < numElements && running; i++) {
				ctx.collect(Tuple2.of(i % numIds, "message #" + i));
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	/*
	 * Source function with three steps. In each step, the source emits a specified number of elements
	 * with a specified range of IDs. The second step is repeated twice. Each step occurs at a different
	 * point in time - using the modifiable clock and latches to synchronize with the topology.
	 */
	private static class SteppedSourceFunction implements SourceFunction<Tuple2<Integer, String>> {
		private static final long serialVersionUID = 1L;

		private volatile boolean running = true;

		private final int step1NumIds;
		private final int step2NumIds;

		private final int step1NumElements;
		private final int step2NumElements;

		public SteppedSourceFunction(int step1NumIds, int step1NumElements, int step2NumIds, int step2NumElements) {
			this.step1NumIds = step1NumIds;
			this.step1NumElements = step1NumElements;
			this.step2NumIds = step2NumIds;
			this.step2NumElements = step2NumElements;
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
			for (int i = 0; i < step1NumElements && running; i++) {
				ctx.collect(Tuple2.of(i % step1NumIds, "message #" + i));
			}
			latch1.await();
			ModifyableClock.setCurrentTime(2 * 60 * 1000L);
			for (int i = 0; i < step2NumElements && running; i++) {
				ctx.collect(Tuple2.of(i % step2NumIds, "message #" + i));
			}
			latch2.await();
			ModifyableClock.setCurrentTime(6 * 60 * 1000L);
			for (int i = 0; i < step2NumElements && running; i++) {
				ctx.collect(Tuple2.of(i % step2NumIds, "message #" + i));
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	public static class ModifyableClock implements Clock {

		private static volatile long currentTime = 0;

		public static void setCurrentTime(long currentTime) {
			ModifyableClock.currentTime = currentTime;
		}

		@Override
		public long currentTimeMillis() {
			return currentTime;
		}
	}

}
