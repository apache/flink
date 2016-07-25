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
package org.apache.flink.streaming.connectors.fs.bucketing;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileConstants;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.connectors.fs.AvroKeyValueSinkWriter;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.streaming.runtime.tasks.TestTimeServiceProvider;
import org.apache.flink.streaming.runtime.tasks.TimeServiceProvider;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.NetUtils;
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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class BucketingSinkTest {
	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	private static MiniDFSCluster hdfsCluster;
	private static org.apache.hadoop.fs.FileSystem dfs;
	private static String hdfsURI;

	private OneInputStreamOperatorTestHarness<String, Object> createTestSink(File dataDir, TimeServiceProvider clock) {
		BucketingSink<String> sink = new BucketingSink<String>(dataDir.getAbsolutePath())
			.setBucketer(new Bucketer<String>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Path getBucketPath(Clock clock, Path basePath, String element) {
					return new Path(basePath, element);
				}
			})
			.setWriter(new StringWriter<String>())
			.setPartPrefix("part")
			.setPendingPrefix("")
			.setInactiveBucketCheckInterval(5*60*1000L)
			.setInactiveBucketThreshold(5*60*1000L)
			.setPendingSuffix(".pending");

		return createTestSink(sink, clock);
	}

	private <T> OneInputStreamOperatorTestHarness<T, Object> createTestSink(BucketingSink<T> sink,
																			TimeServiceProvider clock) {
		return new OneInputStreamOperatorTestHarness<>(new StreamSink<>(sink), new ExecutionConfig(), clock);
	}

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

	@Test
	public void testCheckpointWithoutNotify() throws Exception {
		File dataDir = tempFolder.newFolder();

		TestTimeServiceProvider clock = new TestTimeServiceProvider();
		clock.setCurrentTime(0L);

		OneInputStreamOperatorTestHarness<String, Object> testHarness = createTestSink(dataDir, clock);

		testHarness.setup();
		testHarness.open();

		testHarness.processElement(new StreamRecord<>("Hello"));
		testHarness.processElement(new StreamRecord<>("Hello"));
		testHarness.processElement(new StreamRecord<>("Hello"));

		clock.setCurrentTime(10000L);

		// snapshot but don't call notify to simulate a notify that never
		// arrives, the sink should move pending files in restore() in that case
		StreamTaskState snapshot1 = testHarness.snapshot(0, 0);

		testHarness = createTestSink(dataDir, clock);
		testHarness.setup();
		testHarness.restore(snapshot1, 1);
		testHarness.open();

		testHarness.processElement(new StreamRecord<>("Hello"));

		testHarness.close();

		int numComplete = 0;
		int numPending = 0;
		for (File file: FileUtils.listFiles(dataDir, null, true)) {
			if (file.getAbsolutePath().endsWith("crc")) {
				continue;
			}
			if (file.getPath().contains("pending")) {
				numPending++;
			} else if (file.getName().startsWith("part")) {
				numComplete++;
			}
		}

		Assert.assertEquals(1, numComplete);
		Assert.assertEquals(1, numPending);
	}

	/**
	 * This tests {@link StringWriter} with
	 * non-bucketing output.
	 */
	@Test
	public void testNonRollingStringWriter() throws Exception {
		final String outPath = hdfsURI + "/string-non-rolling-out";

		final int numElements = 20;

		TestTimeServiceProvider clock = new TestTimeServiceProvider();
		clock.setCurrentTime(0L);

		BucketingSink<String> sink = new BucketingSink<String>(outPath)
			.setBucketer(new BasePathBucketer<String>())
			.setPartPrefix("part")
			.setPendingPrefix("")
			.setPendingSuffix("");

		OneInputStreamOperatorTestHarness<String, Object> testHarness = createTestSink(sink, clock);

		testHarness.setup();
		testHarness.open();

		for (int i = 0; i < numElements; i++) {
			testHarness.processElement(new StreamRecord<>("message #" + Integer.toString(i)));
		}

		testHarness.close();

		FSDataInputStream inStream = dfs.open(new Path(outPath + "/part-0-0"));

		BufferedReader br = new BufferedReader(new InputStreamReader(inStream));

		for (int i = 0; i < numElements; i++) {
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
		final String outPath = hdfsURI + "/seq-no-comp-non-rolling-out";

		final int numElements = 20;

		TestTimeServiceProvider clock = new TestTimeServiceProvider();
		clock.setCurrentTime(0L);

		BucketingSink<Tuple2<IntWritable, Text>> sink = new BucketingSink<Tuple2<IntWritable, Text>>(outPath)
			.setWriter(new SequenceFileWriter<IntWritable, Text>())
			.setBucketer(new BasePathBucketer<Tuple2<IntWritable, Text>>())
			.setPartPrefix("part")
			.setPendingPrefix("")
			.setPendingSuffix("");

		sink.setInputType(TypeInformation.of(new TypeHint<Tuple2<IntWritable, Text>>(){}), new ExecutionConfig());

		OneInputStreamOperatorTestHarness<Tuple2<IntWritable, Text>, Object> testHarness =
			createTestSink(sink, clock);

		testHarness.setup();
		testHarness.open();

		for (int i = 0; i < numElements; i++) {
			testHarness.processElement(new StreamRecord<>(Tuple2.of(
				new IntWritable(i),
				new Text("message #" + Integer.toString(i))
			)));
		}

		testHarness.close();

		FSDataInputStream inStream = dfs.open(new Path(outPath + "/part-0-0"));

		SequenceFile.Reader reader = new SequenceFile.Reader(inStream, 1000, 0, 100000, new Configuration());

		IntWritable intWritable = new IntWritable();
		Text txt = new Text();

		for (int i = 0; i < numElements; i++) {
			reader.next(intWritable, txt);
			Assert.assertEquals(i, intWritable.get());
			Assert.assertEquals("message #" + i, txt.toString());
		}

		reader.close();
		inStream.close();
	}

	/**
	 * This tests {@link AvroKeyValueSinkWriter}
	 * with non-rolling output and with compression.
	 */
	@Test
	public void testNonRollingAvroKeyValueWithCompressionWriter() throws Exception {
		final String outPath = hdfsURI + "/avro-kv-no-comp-non-rolling-out";

		final int numElements = 20;

		TestTimeServiceProvider clock = new TestTimeServiceProvider();
		clock.setCurrentTime(0L);

		Map<String, String> properties = new HashMap<>();
		Schema keySchema = Schema.create(Schema.Type.INT);
		Schema valueSchema = Schema.create(Schema.Type.STRING);
		properties.put(AvroKeyValueSinkWriter.CONF_OUTPUT_KEY_SCHEMA, keySchema.toString());
		properties.put(AvroKeyValueSinkWriter.CONF_OUTPUT_VALUE_SCHEMA, valueSchema.toString());
		properties.put(AvroKeyValueSinkWriter.CONF_COMPRESS, String.valueOf(true));
		properties.put(AvroKeyValueSinkWriter.CONF_COMPRESS_CODEC, DataFileConstants.SNAPPY_CODEC);

		BucketingSink<Tuple2<Integer, String>> sink = new BucketingSink<Tuple2<Integer, String>>(outPath)
			.setWriter(new AvroKeyValueSinkWriter<Integer, String>(properties))
			.setBucketer(new BasePathBucketer<Tuple2<Integer, String>>())
			.setPartPrefix("part")
			.setPendingPrefix("")
			.setPendingSuffix("");

		OneInputStreamOperatorTestHarness<Tuple2<Integer, String>, Object> testHarness =
			createTestSink(sink, clock);

		testHarness.setup();
		testHarness.open();

		for (int i = 0; i < numElements; i++) {
			testHarness.processElement(new StreamRecord<>(Tuple2.of(
				i, "message #" + Integer.toString(i)
			)));
		}

		testHarness.close();

		GenericData.setStringType(valueSchema, GenericData.StringType.String);
		Schema elementSchema = AvroKeyValueSinkWriter.AvroKeyValue.getSchema(keySchema, valueSchema);

		FSDataInputStream inStream = dfs.open(new Path(outPath + "/part-0-0"));

		SpecificDatumReader<GenericRecord> elementReader = new SpecificDatumReader<>(elementSchema);
		DataFileStream<GenericRecord> dataFileStream = new DataFileStream<>(inStream, elementReader);
		for (int i = 0; i < numElements; i++) {
			AvroKeyValueSinkWriter.AvroKeyValue<Integer, String> wrappedEntry =
				new AvroKeyValueSinkWriter.AvroKeyValue<>(dataFileStream.next());
			int key = wrappedEntry.getKey();
			Assert.assertEquals(i, key);
			String value = wrappedEntry.getValue();
			Assert.assertEquals("message #" + i, value);
		}

		dataFileStream.close();
		inStream.close();
	}

	/**
	 * This uses {@link DateTimeBucketer} to
	 * produce rolling files. A custom {@link TimeServiceProvider} is set
	 * to simulate the advancing of time alongside the processing of elements.
	 */
	@Test
	public void testDateTimeRollingStringWriter() throws Exception {
		final int numElements = 20;

		final String outPath = hdfsURI + "/rolling-out";

		TestTimeServiceProvider clock = new TestTimeServiceProvider();
		clock.setCurrentTime(0L);

		BucketingSink<String> sink = new BucketingSink<String>(outPath)
			.setBucketer(new DateTimeBucketer<String>("ss"))
			.setPartPrefix("part")
			.setPendingPrefix("")
			.setPendingSuffix("");

		OneInputStreamOperatorTestHarness<String, Object> testHarness = createTestSink(sink, clock);

		testHarness.setup();
		testHarness.open();

		for (int i = 0; i < numElements; i++) {
			// Every 5 elements, increase the clock time. We should end up with 5 elements per bucket.
			if (i % 5 == 0) {
				clock.setCurrentTime(i * 1000L);
			}
			testHarness.processElement(new StreamRecord<>("message #" + Integer.toString(i)));
		}

		testHarness.close();

		RemoteIterator<LocatedFileStatus> files = dfs.listFiles(new Path(outPath), true);

		// We should have 4 rolling files across 4 time intervals
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

		Assert.assertEquals(4, numFiles);
	}

	/**
	 * This uses a custom bucketing function which determines the bucket from the input.
	 */
	@Test
	public void testCustomBucketing() throws Exception {
		File dataDir = tempFolder.newFolder();

		final int numIds = 4;
		final int numElements = 20;

		TestTimeServiceProvider clock = new TestTimeServiceProvider();
		clock.setCurrentTime(0L);

		OneInputStreamOperatorTestHarness<String, Object> testHarness = createTestSink(dataDir, clock);

		testHarness.setup();
		testHarness.open();

		for (int i = 0; i < numElements; i++) {
			testHarness.processElement(new StreamRecord<>(Integer.toString(i % numIds)));
		}

		testHarness.close();

		// we should have 4 buckets, with 1 file each
		int numFiles = 0;
		for (File file: FileUtils.listFiles(dataDir, null, true)) {
			if (file.getName().startsWith("part")) {
				numFiles++;
			}
		}

		Assert.assertEquals(4, numFiles);
	}

	/**
	 * This uses a custom bucketing function which determines the bucket from the input.
	 * We use a simulated clock to reduce the number of buckets being written to over time.
	 * This causes buckets to become 'inactive' and their file parts 'closed' by the sink.
	 */
	@Test
	public void testCustomBucketingInactiveBucketCleanup() throws Exception {
		File dataDir = tempFolder.newFolder();

		final int step1NumIds = 4;
		final int step2NumIds = 2;
		final int numElementsPerStep = 20;

		TestTimeServiceProvider clock = new TestTimeServiceProvider();
		clock.setCurrentTime(0L);

		OneInputStreamOperatorTestHarness<String, Object> testHarness = createTestSink(dataDir, clock);

		testHarness.setup();
		testHarness.open();

		for (int i = 0; i < numElementsPerStep; i++) {
			testHarness.processElement(new StreamRecord<>(Integer.toString(i % step1NumIds)));
		}

		clock.setCurrentTime(2*60*1000L);

		for (int i = 0; i < numElementsPerStep; i++) {
			testHarness.processElement(new StreamRecord<>(Integer.toString(i % step2NumIds)));
		}

		clock.setCurrentTime(6*60*1000L);

		for (int i = 0; i < numElementsPerStep; i++) {
			testHarness.processElement(new StreamRecord<>(Integer.toString(i % step2NumIds)));
		}

		// we should have 4 buckets, with 1 file each
		// 2 of these buckets should have been finalised due to becoming inactive
		int numFiles = 0;
		int numInProgress = 0;
		for (File file: FileUtils.listFiles(dataDir, null, true)) {
			if (file.getAbsolutePath().endsWith("crc")) {
				continue;
			}
			if (file.getPath().contains("in-progress")) {
				numInProgress++;
			}
			numFiles++;
		}

		testHarness.close();

		Assert.assertEquals(4, numFiles);
		Assert.assertEquals(2, numInProgress);
	}
}
