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

package org.apache.flink.streaming.connectors.fs;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.fs.AvroKeyValueSinkWriter.AvroKeyValue;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLogger;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.connectors.fs.bucketing.BucketingSinkTestUtils.checkLocalFs;

/**
 * Tests for {@link RollingSink}. These
 * tests test the different output methods as well as the rolling feature using a manual clock
 * that increases time in lockstep with element computation using latches.
 *
 *
 * <p>This only tests the rolling behaviour of the sink. There is a separate ITCase that verifies
 * exactly once behaviour.
 *
 * @deprecated should be removed with the {@link RollingSink}.
 */
@Deprecated
public class RollingSinkITCase extends TestLogger {

	protected static final Logger LOG = LoggerFactory.getLogger(RollingSinkITCase.class);

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	protected static MiniClusterResource miniClusterResource;
	protected static MiniDFSCluster hdfsCluster;
	protected static org.apache.hadoop.fs.FileSystem dfs;
	protected static String hdfsURI;
	protected static Configuration conf = new Configuration();

	protected static File dataDir;

	@BeforeClass
	public static void setup() throws Exception {

		LOG.info("In RollingSinkITCase: Starting MiniDFSCluster ");

		dataDir = tempFolder.newFolder();

		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dataDir.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		hdfsCluster = builder.build();

		dfs = hdfsCluster.getFileSystem();

		hdfsURI = "hdfs://"
				+ NetUtils.hostAndPortToUrlString(hdfsCluster.getURI().getHost(), hdfsCluster.getNameNodePort())
				+ "/";

		miniClusterResource = new MiniClusterResource(
			new MiniClusterResource.MiniClusterResourceConfiguration(
				new org.apache.flink.configuration.Configuration(),
				1,
				4));

		miniClusterResource.before();
	}

	@AfterClass
	public static void teardown() throws Exception {
		LOG.info("In RollingSinkITCase: tearing down MiniDFSCluster ");
		hdfsCluster.shutdown();

		if (miniClusterResource != null) {
			miniClusterResource.after();
		}
	}

	/**
	 * This tests {@link StringWriter} with
	 * non-rolling output.
	 */
	@Test
	public void testNonRollingStringWriter() throws Exception {
		final int numElements = 20;
		final String outPath = hdfsURI + "/string-non-rolling-out";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		DataStream<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction(numElements))
				.broadcast()
				.filter(new OddEvenFilter());

		RollingSink<String> sink = new RollingSink<String>(outPath)
				.setBucketer(new NonRollingBucketer())
				.setPartPrefix("part")
				.setPendingPrefix("")
				.setPendingSuffix("");

		source
				.map(new MapFunction<Tuple2<Integer, String>, String>() {
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

		for (int i = 0; i < numElements; i += 2) {
			String line = br.readLine();
			Assert.assertEquals("message #" + i, line);
		}

		inStream.close();

		inStream = dfs.open(new Path(outPath + "/part-1-0"));

		br = new BufferedReader(new InputStreamReader(inStream));

		for (int i = 1; i < numElements; i += 2) {
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
		final int numElements = 20;
		final String outPath = hdfsURI + "/seq-no-comp-non-rolling-out";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		DataStream<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction(numElements))
				.broadcast()
				.filter(new OddEvenFilter());

		DataStream<Tuple2<IntWritable, Text>> mapped =  source.map(new MapFunction<Tuple2<Integer, String>, Tuple2<IntWritable, Text>>() {
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

		for (int i = 0; i < numElements; i += 2) {
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

		for (int i = 1; i < numElements; i += 2) {
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
		final int numElements = 20;
		final String outPath = hdfsURI + "/seq-non-rolling-out";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		DataStream<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction(numElements))
				.broadcast()
				.filter(new OddEvenFilter());

		DataStream<Tuple2<IntWritable, Text>> mapped =  source.map(new MapFunction<Tuple2<Integer, String>, Tuple2<IntWritable, Text>>() {
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

		for (int i = 0; i < numElements; i += 2) {
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

		for (int i = 1; i < numElements; i += 2) {
			reader.next(intWritable, txt);
			Assert.assertEquals(i, intWritable.get());
			Assert.assertEquals("message #" + i, txt.toString());
		}

		reader.close();
		inStream.close();
	}

	/**
	 * This tests {@link AvroKeyValueSinkWriter}
	 * with non-rolling output and without compression.
	 */
	@Test
	public void testNonRollingAvroKeyValueWithoutCompressionWriter() throws Exception {
		final int numElements = 20;
		final String outPath = hdfsURI + "/avro-kv-no-comp-non-rolling-out";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		DataStream<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction(numElements))
				.broadcast()
				.filter(new OddEvenFilter());

		Map<String, String> properties = new HashMap<>();
		Schema keySchema = Schema.create(Type.INT);
		Schema valueSchema = Schema.create(Type.STRING);
		properties.put(AvroKeyValueSinkWriter.CONF_OUTPUT_KEY_SCHEMA, keySchema.toString());
		properties.put(AvroKeyValueSinkWriter.CONF_OUTPUT_VALUE_SCHEMA, valueSchema.toString());
		RollingSink<Tuple2<Integer, String>> sink = new RollingSink<Tuple2<Integer, String>>(outPath)
				.setWriter(new AvroKeyValueSinkWriter<Integer, String>(properties))
				.setBucketer(new NonRollingBucketer())
				.setPartPrefix("part")
				.setPendingPrefix("")
				.setPendingSuffix("");

		source.addSink(sink);

		env.execute("RollingSink Avro KeyValue Writer Test");

		GenericData.setStringType(valueSchema, StringType.String);
		Schema elementSchema = AvroKeyValue.getSchema(keySchema, valueSchema);

		FSDataInputStream inStream = dfs.open(new Path(outPath + "/part-0-0"));
		SpecificDatumReader<GenericRecord> elementReader = new SpecificDatumReader<GenericRecord>(elementSchema);
		DataFileStream<GenericRecord> dataFileStream = new DataFileStream<GenericRecord>(inStream, elementReader);
		for (int i = 0; i < numElements; i += 2) {
			AvroKeyValue<Integer, String> wrappedEntry = new AvroKeyValue<Integer, String>(dataFileStream.next());
			int key = wrappedEntry.getKey().intValue();
			Assert.assertEquals(i, key);
			String value = wrappedEntry.getValue();
			Assert.assertEquals("message #" + i, value);
		}

		dataFileStream.close();
		inStream.close();

		inStream = dfs.open(new Path(outPath + "/part-1-0"));
		dataFileStream = new DataFileStream<GenericRecord>(inStream, elementReader);

		for (int i = 1; i < numElements; i += 2) {
			AvroKeyValue<Integer, String> wrappedEntry = new AvroKeyValue<Integer, String>(dataFileStream.next());
			int key = wrappedEntry.getKey().intValue();
			Assert.assertEquals(i, key);
			String value = wrappedEntry.getValue();
			Assert.assertEquals("message #" + i, value);
		}

		dataFileStream.close();
		inStream.close();
	}

	/**
	 * This tests {@link AvroKeyValueSinkWriter}
	 * with non-rolling output and with compression.
	 */
	@Test
	public void testNonRollingAvroKeyValueWithCompressionWriter() throws Exception {
		final int numElements = 20;
		final String outPath = hdfsURI + "/avro-kv-no-comp-non-rolling-out";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		DataStream<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction(numElements))
				.broadcast()
				.filter(new OddEvenFilter());

		Map<String, String> properties = new HashMap<>();
		Schema keySchema = Schema.create(Type.INT);
		Schema valueSchema = Schema.create(Type.STRING);
		properties.put(AvroKeyValueSinkWriter.CONF_OUTPUT_KEY_SCHEMA, keySchema.toString());
		properties.put(AvroKeyValueSinkWriter.CONF_OUTPUT_VALUE_SCHEMA, valueSchema.toString());
		properties.put(AvroKeyValueSinkWriter.CONF_COMPRESS, String.valueOf(true));
		properties.put(AvroKeyValueSinkWriter.CONF_COMPRESS_CODEC, DataFileConstants.SNAPPY_CODEC);
		RollingSink<Tuple2<Integer, String>> sink = new RollingSink<Tuple2<Integer, String>>(outPath)
				.setWriter(new AvroKeyValueSinkWriter<Integer, String>(properties))
				.setBucketer(new NonRollingBucketer())
				.setPartPrefix("part")
				.setPendingPrefix("")
				.setPendingSuffix("");

		source.addSink(sink);

		env.execute("RollingSink Avro KeyValue Writer Test");

		GenericData.setStringType(valueSchema, StringType.String);
		Schema elementSchema = AvroKeyValue.getSchema(keySchema, valueSchema);

		FSDataInputStream inStream = dfs.open(new Path(outPath + "/part-0-0"));
		SpecificDatumReader<GenericRecord> elementReader = new SpecificDatumReader<GenericRecord>(elementSchema);
		DataFileStream<GenericRecord> dataFileStream = new DataFileStream<GenericRecord>(inStream, elementReader);
		for (int i = 0; i < numElements; i += 2) {
			AvroKeyValue<Integer, String> wrappedEntry = new AvroKeyValue<Integer, String>(dataFileStream.next());
			int key = wrappedEntry.getKey().intValue();
			Assert.assertEquals(i, key);
			String value = wrappedEntry.getValue();
			Assert.assertEquals("message #" + i, value);
		}

		dataFileStream.close();
		inStream.close();

		inStream = dfs.open(new Path(outPath + "/part-1-0"));
		dataFileStream = new DataFileStream<GenericRecord>(inStream, elementReader);

		for (int i = 1; i < numElements; i += 2) {
			AvroKeyValue<Integer, String> wrappedEntry = new AvroKeyValue<Integer, String>(dataFileStream.next());
			int key = wrappedEntry.getKey().intValue();
			Assert.assertEquals(i, key);
			String value = wrappedEntry.getValue();
			Assert.assertEquals("message #" + i, value);
		}

		dataFileStream.close();
		inStream.close();
	}

	/**
	 * This tests user defined hdfs configuration.
	 * @throws Exception
     */
	@Test
	public void testUserDefinedConfiguration() throws Exception {
		final int numElements = 20;
		final String outPath = hdfsURI + "/string-non-rolling-with-config";
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		DataStream<Tuple2<Integer, String>> source = env.addSource(new TestSourceFunction(numElements))
			.broadcast()
			.filter(new OddEvenFilter());

		Configuration conf = new Configuration();
		conf.set("io.file.buffer.size", "40960");
		RollingSink<String> sink = new RollingSink<String>(outPath)
			.setFSConfig(conf)
			.setWriter(new StreamWriterWithConfigCheck<String>("io.file.buffer.size", "40960"))
			.setBucketer(new NonRollingBucketer())
			.setPartPrefix("part")
			.setPendingPrefix("")
			.setPendingSuffix("");

		source
			.map(new MapFunction<Tuple2<Integer, String>, String>() {
				private static final long serialVersionUID = 1L;
				@Override
				public String map(Tuple2<Integer, String> value) throws Exception {
					return value.f1;
				}
			})
			.addSink(sink);

		env.execute("RollingSink with configuration Test");

		FSDataInputStream inStream = dfs.open(new Path(outPath + "/part-0-0"));

		BufferedReader br = new BufferedReader(new InputStreamReader(inStream));

		for (int i = 0; i < numElements; i += 2) {
			String line = br.readLine();
			Assert.assertEquals("message #" + i, line);
		}

		inStream.close();

		inStream = dfs.open(new Path(outPath + "/part-1-0"));

		br = new BufferedReader(new InputStreamReader(inStream));

		for (int i = 1; i < numElements; i += 2) {
			String line = br.readLine();
			Assert.assertEquals("message #" + i, line);
		}

		inStream.close();
	}

	// we use this to synchronize the clock changes to elements being processed
	private static final MultiShotLatch latch1 = new MultiShotLatch();
	private static final MultiShotLatch latch2 = new MultiShotLatch();

	/**
	 * This uses {@link org.apache.flink.streaming.connectors.fs.DateTimeBucketer} to
	 * produce rolling files. The clock of DateTimeBucketer is set to
	 * {@link ModifyableClock} to keep the time in lockstep with the processing of elements using
	 * latches.
	 */
	@Test
	public void testDateTimeRollingStringWriter() throws Exception {
		final int numElements = 20;
		final String outPath = hdfsURI + "/rolling-out";
		DateTimeBucketer.setClock(new ModifyableClock());
		ModifyableClock.setCurrentTime(0);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(2);

		DataStream<Tuple2<Integer, String>> source = env.addSource(new WaitingTestSourceFunction(
				numElements))
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

	private static final String PART_PREFIX = "part";
	private static final String PENDING_SUFFIX = ".pending";
	private static final String IN_PROGRESS_SUFFIX = ".in-progress";
	private static final String VALID_LENGTH_SUFFIX = ".valid";

	@Test
	public void testBucketStateTransitions() throws Exception {
		final File outDir = tempFolder.newFolder();

		OneInputStreamOperatorTestHarness<String, Object> testHarness = createRescalingTestSink(outDir, 1, 0);
		testHarness.setup();
		testHarness.open();

		testHarness.setProcessingTime(0L);

		// we have a bucket size of 5 bytes, so each record will get its own bucket,
		// i.e. the bucket should roll after every record.

		testHarness.processElement(new StreamRecord<>("test1", 1L));
		testHarness.processElement(new StreamRecord<>("test2", 1L));
		checkLocalFs(outDir, 1, 1 , 0, 0);

		testHarness.processElement(new StreamRecord<>("test3", 1L));
		checkLocalFs(outDir, 1, 2, 0, 0);

		testHarness.snapshot(0, 0);
		checkLocalFs(outDir, 1, 2, 0, 0);

		testHarness.notifyOfCompletedCheckpoint(0);
		checkLocalFs(outDir, 1, 0, 2, 0);

		OperatorSubtaskState snapshot = testHarness.snapshot(1, 0);

		testHarness.close();
		checkLocalFs(outDir, 0, 1, 2, 0);

		testHarness = createRescalingTestSink(outDir, 1, 0);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();
		checkLocalFs(outDir, 0, 0, 3, 1);

		snapshot = testHarness.snapshot(2, 0);

		testHarness.processElement(new StreamRecord<>("test4", 10));
		checkLocalFs(outDir, 1, 0, 3, 1);

		testHarness = createRescalingTestSink(outDir, 1, 0);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		// the in-progress file remains as we do not clean up now
		checkLocalFs(outDir, 1, 0, 3, 1);

		testHarness.close();

		// at close it is not moved to final because it is not part
		// of the current task's state, it was just a not cleaned up leftover.
		checkLocalFs(outDir, 1, 0, 3, 1);
	}

	@Test
	public void testScalingDown() throws Exception {
		final File outDir = tempFolder.newFolder();

		OneInputStreamOperatorTestHarness<String, Object> testHarness1 = createRescalingTestSink(outDir, 3, 0);
		testHarness1.setup();
		testHarness1.open();

		OneInputStreamOperatorTestHarness<String, Object> testHarness2 = createRescalingTestSink(outDir, 3, 1);
		testHarness2.setup();
		testHarness2.open();

		OneInputStreamOperatorTestHarness<String, Object> testHarness3 = createRescalingTestSink(outDir, 3, 2);
		testHarness3.setup();
		testHarness3.open();

		testHarness1.processElement(new StreamRecord<>("test1", 0L));
		checkLocalFs(outDir, 1, 0, 0, 0);

		testHarness2.processElement(new StreamRecord<>("test2", 0L));
		testHarness2.processElement(new StreamRecord<>("test3", 0L));
		testHarness2.processElement(new StreamRecord<>("test4", 0L));
		testHarness2.processElement(new StreamRecord<>("test5", 0L));
		testHarness2.processElement(new StreamRecord<>("test6", 0L));
		checkLocalFs(outDir, 2, 4, 0, 0);

		testHarness3.processElement(new StreamRecord<>("test7", 0L));
		testHarness3.processElement(new StreamRecord<>("test8", 0L));
		checkLocalFs(outDir, 3, 5, 0, 0);

		// intentionally we snapshot them in a not ascending order so that the states are shuffled
		OperatorSubtaskState mergedSnapshot = AbstractStreamOperatorTestHarness.repackageState(
			testHarness3.snapshot(0, 0),
			testHarness1.snapshot(0, 0),
			testHarness2.snapshot(0, 0)
		);

		// with the above state reshuffling, we expect testHarness4 to take the
		// state of the previous testHarness3 and testHarness1 while testHarness5
		// will take that of the previous testHarness1

		OneInputStreamOperatorTestHarness<String, Object> testHarness4 = createRescalingTestSink(outDir, 2, 0);
		testHarness4.setup();
		testHarness4.initializeState(mergedSnapshot);
		testHarness4.open();

		// we do not have a length file for part-2-0 because bucket part-2-0
		// was not "in-progress", but "pending" (its full content is valid).
		checkLocalFs(outDir, 1, 4, 3, 2);

		OneInputStreamOperatorTestHarness<String, Object> testHarness5 = createRescalingTestSink(outDir, 2, 1);
		testHarness5.setup();
		testHarness5.initializeState(mergedSnapshot);
		testHarness5.open();

		checkLocalFs(outDir, 0, 0, 8, 3);
	}

	@Test
	public void testScalingUp() throws Exception {
		final File outDir = tempFolder.newFolder();

		OneInputStreamOperatorTestHarness<String, Object> testHarness1 = createRescalingTestSink(outDir, 2, 0);
		testHarness1.setup();
		testHarness1.open();

		OneInputStreamOperatorTestHarness<String, Object> testHarness2 = createRescalingTestSink(outDir, 2, 0);
		testHarness2.setup();
		testHarness2.open();

		testHarness1.processElement(new StreamRecord<>("test1", 0L));
		testHarness1.processElement(new StreamRecord<>("test2", 0L));

		checkLocalFs(outDir, 1, 1, 0, 0);

		testHarness2.processElement(new StreamRecord<>("test3", 0L));
		testHarness2.processElement(new StreamRecord<>("test4", 0L));
		testHarness2.processElement(new StreamRecord<>("test5", 0L));

		checkLocalFs(outDir, 2, 3, 0, 0);

		// intentionally we snapshot them in the reverse order so that the states are shuffled
		OperatorSubtaskState mergedSnapshot = AbstractStreamOperatorTestHarness.repackageState(
			testHarness2.snapshot(0, 0),
			testHarness1.snapshot(0, 0)
		);

		testHarness1 = createRescalingTestSink(outDir, 3, 0);
		testHarness1.setup();
		testHarness1.initializeState(mergedSnapshot);
		testHarness1.open();

		checkLocalFs(outDir, 1, 1, 3, 1);

		testHarness2 = createRescalingTestSink(outDir, 3, 1);
		testHarness2.setup();
		testHarness2.initializeState(mergedSnapshot);
		testHarness2.open();

		checkLocalFs(outDir, 0, 0, 5, 2);

		OneInputStreamOperatorTestHarness<String, Object> testHarness3 = createRescalingTestSink(outDir, 3, 2);
		testHarness3.setup();
		testHarness3.initializeState(mergedSnapshot);
		testHarness3.open();

		checkLocalFs(outDir, 0, 0, 5, 2);

		testHarness1.processElement(new StreamRecord<>("test6", 0));
		testHarness2.processElement(new StreamRecord<>("test6", 0));
		testHarness3.processElement(new StreamRecord<>("test6", 0));

		// 3 for the different tasks
		checkLocalFs(outDir, 3, 0, 5, 2);

		testHarness1.snapshot(1, 0);
		testHarness2.snapshot(1, 0);
		testHarness3.snapshot(1, 0);

		testHarness1.close();
		testHarness2.close();
		testHarness3.close();

		checkLocalFs(outDir, 0, 3, 5, 2);
	}

	private OneInputStreamOperatorTestHarness<String, Object> createRescalingTestSink(
		File outDir, int totalParallelism, int taskIdx) throws Exception {

		RollingSink<String> sink = new RollingSink<String>(outDir.getAbsolutePath())
			.setWriter(new StringWriter<String>())
			.setBatchSize(5)
			.setPartPrefix(PART_PREFIX)
			.setInProgressPrefix("")
			.setPendingPrefix("")
			.setValidLengthPrefix("")
			.setInProgressSuffix(IN_PROGRESS_SUFFIX)
			.setPendingSuffix(PENDING_SUFFIX)
			.setValidLengthSuffix(VALID_LENGTH_SUFFIX);

		return createTestSink(sink, totalParallelism, taskIdx);
	}

	private <T> OneInputStreamOperatorTestHarness<T, Object> createTestSink(
		RollingSink<T> sink, int totalParallelism, int taskIdx) throws Exception {
		return new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), 10, totalParallelism, taskIdx);
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

	private static class StreamWriterWithConfigCheck<T> extends StringWriter<T> {
		private static final long serialVersionUID = 761584896826819477L;

		private String key;
		private String expect;
		public StreamWriterWithConfigCheck(String key, String expect) {
			this.key = key;
			this.expect = expect;
		}

		@Override
		public void open(FileSystem fs, Path path) throws IOException {
			super.open(fs, path);
			Assert.assertEquals(expect, fs.getConf().get(key));
		}

		@Override
		public Writer<T> duplicate() {
			return new StreamWriterWithConfigCheck<>(key, expect);
		}
	}

	private static class OddEvenFilter extends RichFilterFunction<Tuple2<Integer, String>> {
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

	private static class ModifyableClock implements Clock {

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
