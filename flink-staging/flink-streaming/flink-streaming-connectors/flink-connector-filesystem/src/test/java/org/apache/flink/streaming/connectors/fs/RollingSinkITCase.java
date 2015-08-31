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
package org.apache.flink.streaming.connectors.fs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.taskmanager.MultiShotLatch;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Tests for {@link RollingSink}. These
 * tests test the different output methods as well as the rolling feature using a manual clock
 * that increases time in lockstep with element computation using latches.
 *
 * <p>
 * This only tests the rolling behaviour of the sink. There is a separate ITCase that verifies
 * exactly once behaviour.
 */
public class RollingSinkITCase extends StreamingMultipleProgramsTestBase {

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

		hdfsURI = "hdfs://" + hdfsCluster.getURI().getHost() + ":" + hdfsCluster.getNameNodePort() +"/";
	}

	@AfterClass
	public static void destroyHDFS() {
		hdfsCluster.shutdown();
	}

	/**
	 * This tests {@link StringWriter} with
	 * non-rolling output.
	 */
	@Test
	public void testNonRollingStringWriter() throws Exception {
		final int NUM_ELEMENTS = 20;
		final int PARALLELISM = 2;
		final String outPath = hdfsURI + "/string-non-rolling-out";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(PARALLELISM);

		DataStream<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction(NUM_ELEMENTS))
				.broadcast()
				.filter(new OddEvenFilter());

		RollingSink<String> sink = new RollingSink<String>(outPath)
				.setBucketer(new NonRollingBucketer())
				.setPartPrefix("part")
				.setPendingPrefix("")
				.setPendingSuffix("");

		source
				.map(new MapFunction<Tuple2<Integer,String>, String>() {
					private static final long serialVersionUID = 1L;
					@Override
					public String map(Tuple2<Integer, String> value) throws Exception {
						return value.f1;
					}
				})
				.addSink(sink);

		env.execute("RollingSink String Write Test");

		FSDataInputStream inStream = dfs.open(new Path(outPath + "/part-0-0"));

		BufferedReader br = new BufferedReader(new InputStreamReader(inStream));

		for (int i = 0; i < NUM_ELEMENTS; i += 2) {
			String line = br.readLine();
			Assert.assertEquals("message #" + i, line);
		}

		inStream.close();

		inStream = dfs.open(new Path(outPath + "/part-1-0"));

		br = new BufferedReader(new InputStreamReader(inStream));

		for (int i = 1; i < NUM_ELEMENTS; i += 2) {
			String line = br.readLine();
			Assert.assertEquals("message #" + i, line);
		}

		inStream.close();
	}

	/**
	 * This tests {@link SequenceFileWriter}
	 * with non-rolling output and without compression.
	 */
	@Test
	public void testNonRollingSequenceFileWithoutCompressionWriter() throws Exception {
		final int NUM_ELEMENTS = 20;
		final int PARALLELISM = 2;
		final String outPath = hdfsURI + "/seq-no-comp-non-rolling-out";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(PARALLELISM);

		DataStream<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction(NUM_ELEMENTS))
				.broadcast()
				.filter(new OddEvenFilter());

		DataStream<Tuple2<IntWritable, Text>> mapped =  source.map(new MapFunction<Tuple2<Integer,String>, Tuple2<IntWritable, Text>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<IntWritable, Text> map(Tuple2<Integer, String> value) throws Exception {
				return Tuple2.of(new IntWritable(value.f0), new Text(value.f1));
			}
		});


		RollingSink<Tuple2<IntWritable, Text>> sink = new RollingSink<Tuple2<IntWritable, Text>>(outPath)
				.setWriter(new SequenceFileWriter<IntWritable, Text>())
				.setBucketer(new NonRollingBucketer())
				.setPartPrefix("part")
				.setPendingPrefix("")
				.setPendingSuffix("");

		mapped.addSink(sink);

		env.execute("RollingSink String Write Test");

		FSDataInputStream inStream = dfs.open(new Path(outPath + "/part-0-0"));

		SequenceFile.Reader reader = new SequenceFile.Reader(inStream,
				1000,
				0,
				100000,
				new Configuration());

		IntWritable intWritable = new IntWritable();
		Text txt = new Text();

		for (int i = 0; i < NUM_ELEMENTS; i += 2) {
			reader.next(intWritable, txt);
			Assert.assertEquals(i, intWritable.get());
			Assert.assertEquals("message #" + i, txt.toString());
		}

		reader.close();
		inStream.close();

		inStream = dfs.open(new Path(outPath + "/part-1-0"));

		reader = new SequenceFile.Reader(inStream,
				1000,
				0,
				100000,
				new Configuration());

		for (int i = 1; i < NUM_ELEMENTS; i += 2) {
			reader.next(intWritable, txt);
			Assert.assertEquals(i, intWritable.get());
			Assert.assertEquals("message #" + i, txt.toString());
		}

		reader.close();
		inStream.close();
	}

	/**
	 * This tests {@link SequenceFileWriter}
	 * with non-rolling output but with compression.
	 */
	@Test
	public void testNonRollingSequenceFileWithCompressionWriter() throws Exception {
		final int NUM_ELEMENTS = 20;
		final int PARALLELISM = 2;
		final String outPath = hdfsURI + "/seq-non-rolling-out";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(PARALLELISM);

		DataStream<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction(NUM_ELEMENTS))
				.broadcast()
				.filter(new OddEvenFilter());

		DataStream<Tuple2<IntWritable, Text>> mapped =  source.map(new MapFunction<Tuple2<Integer,String>, Tuple2<IntWritable, Text>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<IntWritable, Text> map(Tuple2<Integer, String> value) throws Exception {
				return Tuple2.of(new IntWritable(value.f0), new Text(value.f1));
			}
		});


		RollingSink<Tuple2<IntWritable, Text>> sink = new RollingSink<Tuple2<IntWritable, Text>>(outPath)
				.setWriter(new SequenceFileWriter<IntWritable, Text>("Default", SequenceFile.CompressionType.BLOCK))
				.setBucketer(new NonRollingBucketer())
				.setPartPrefix("part")
				.setPendingPrefix("")
				.setPendingSuffix("");

		mapped.addSink(sink);

		env.execute("RollingSink String Write Test");

		FSDataInputStream inStream = dfs.open(new Path(outPath + "/part-0-0"));

		SequenceFile.Reader reader = new SequenceFile.Reader(inStream,
				1000,
				0,
				100000,
				new Configuration());

		IntWritable intWritable = new IntWritable();
		Text txt = new Text();

		for (int i = 0; i < NUM_ELEMENTS; i += 2) {
			reader.next(intWritable, txt);
			Assert.assertEquals(i, intWritable.get());
			Assert.assertEquals("message #" + i, txt.toString());
		}

		reader.close();
		inStream.close();

		inStream = dfs.open(new Path(outPath + "/part-1-0"));

		reader = new SequenceFile.Reader(inStream,
				1000,
				0,
				100000,
				new Configuration());

		for (int i = 1; i < NUM_ELEMENTS; i += 2) {
			reader.next(intWritable, txt);
			Assert.assertEquals(i, intWritable.get());
			Assert.assertEquals("message #" + i, txt.toString());
		}

		reader.close();
		inStream.close();
	}

	// we use this to synchronize the clock changes to elements being processed
	final static MultiShotLatch latch1 = new MultiShotLatch();
	final static MultiShotLatch latch2 = new MultiShotLatch();

	/**
	 * This uses {@link org.apache.flink.streaming.connectors.fs.DateTimeBucketer} to
	 * produce rolling files. The clock of DateTimeBucketer is set to
	 * {@link ModifyableClock} to keep the time in lockstep with the processing of elements using
	 * latches.
	 */
	@Test
	public void testDateTimeRollingStringWriter() throws Exception {
		final int NUM_ELEMENTS = 20;
		final int PARALLELISM = 2;
		final String outPath = hdfsURI + "/rolling-out";
		DateTimeBucketer.setClock(new ModifyableClock());
		ModifyableClock.setCurrentTime(0);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(PARALLELISM);



		DataStream<Tuple2<Integer, String>> source = env.addSource(new WaitingTestSourceFunction(
				NUM_ELEMENTS))
				.broadcast();

		// the parallel flatMap is chained to the sink, so when it has seen 5 elements it can
		// fire the latch
		DataStream<String> mapped = source
				.flatMap(new RichFlatMapFunction<Tuple2<Integer, String>, String>() {
					private static final long serialVersionUID = 1L;

					int count = 0;
					@Override
					public void flatMap(Tuple2<Integer, String> value,
							Collector<String> out) throws Exception {
						out.collect(value.f1);
						count++;
						if (count >= 5) {
							if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
								latch1.trigger();
							} else {
								latch2.trigger();
							}
							count = 0;
						}
					}

				});

		RollingSink<String> sink = new RollingSink<String>(outPath)
				.setBucketer(new DateTimeBucketer("ss"))
				.setPartPrefix("part")
				.setPendingPrefix("")
				.setPendingSuffix("");

		mapped.addSink(sink);

		env.execute("RollingSink String Write Test");

		RemoteIterator<LocatedFileStatus> files = dfs.listFiles(new Path(outPath), true);

		// we should have 8 rolling files, 4 time intervals and parallelism of 2
		int numFiles = 0;
		while (files.hasNext()) {
			LocatedFileStatus file = files.next();
			numFiles++;
			if (file.getPath().toString().contains("rolling-out/00")) {
				FSDataInputStream inStream = dfs.open(file.getPath());

				BufferedReader br = new BufferedReader(new InputStreamReader(inStream));

				for (int i = 0; i < 5; i++) {
					String line = br.readLine();
					Assert.assertEquals("message #" + i, line);
				}

				inStream.close();
			} else if (file.getPath().toString().contains("rolling-out/05")) {
				FSDataInputStream inStream = dfs.open(file.getPath());

				BufferedReader br = new BufferedReader(new InputStreamReader(inStream));

				for (int i = 5; i < 10; i++) {
					String line = br.readLine();
					Assert.assertEquals("message #" + i, line);
				}

				inStream.close();
			} else if (file.getPath().toString().contains("rolling-out/10")) {
				FSDataInputStream inStream = dfs.open(file.getPath());

				BufferedReader br = new BufferedReader(new InputStreamReader(inStream));

				for (int i = 10; i < 15; i++) {
					String line = br.readLine();
					Assert.assertEquals("message #" + i, line);
				}

				inStream.close();
			} else if (file.getPath().toString().contains("rolling-out/15")) {
				FSDataInputStream inStream = dfs.open(file.getPath());

				BufferedReader br = new BufferedReader(new InputStreamReader(inStream));

				for (int i = 15; i < 20; i++) {
					String line = br.readLine();
					Assert.assertEquals("message #" + i, line);
				}

				inStream.close();
			} else {
				Assert.fail("File " + file + " does not match any expected roll pattern.");
			}
		}

		Assert.assertEquals(8, numFiles);
	}


	private static class TestSourceFunction implements SourceFunction<Tuple2<Integer, String>> {
		private static final long serialVersionUID = 1L;

		private volatile boolean running = true;

		private final int numElements;

		public TestSourceFunction(int numElements) {
			this.numElements = numElements;
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
			for (int i = 0; i < numElements && running; i++) {
				ctx.collect(Tuple2.of(i, "message #" + i));
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	/**
	 * This waits on the two multi-shot latches. The latches are triggered in a parallel
	 * flatMap inside the test topology.
	 */
	private static class WaitingTestSourceFunction implements SourceFunction<Tuple2<Integer, String>> {
		private static final long serialVersionUID = 1L;

		private volatile boolean running = true;

		private final int numElements;

		public WaitingTestSourceFunction(int numElements) {
			this.numElements = numElements;
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, String>> ctx) throws Exception {
			for (int i = 0; i < numElements && running; i++) {
				if (i % 5 == 0 && i > 0) {
					// update the clock after "five seconds", so we get 20 seconds in total
					// with 5 elements in each time window
					latch1.await();
					latch2.await();
					ModifyableClock.setCurrentTime(i * 1000);
				}
				ctx.collect(Tuple2.of(i, "message #" + i));
			}
		}

		@Override
		public void cancel() {
			running = false;
		}
	}

	public static class OddEvenFilter extends RichFilterFunction<Tuple2<Integer, String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Tuple2<Integer, String> value) throws Exception {
			if (getRuntimeContext().getIndexOfThisSubtask() == 0) {
				return value.f0 % 2 == 0;
			} else {
				return value.f0 % 2 == 1;
			}
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
