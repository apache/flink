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

package org.apache.flink.hdfstests;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperator;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorTest;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URL;

public class ContinuousFileProcessingMigrationTest {

	private static final int NO_OF_FILES = 5;
	private static final int LINES_PER_FILE = 10;

	private static final long INTERVAL = 100;

	private static File baseDir;

	private static FileSystem hdfs;
	private static String hdfsURI;
	private static MiniDFSCluster hdfsCluster;

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	//						PREPARING FOR THE TESTS

	@BeforeClass
	public static void createHDFS() {
		try {
			baseDir = tempFolder.newFolder().getAbsoluteFile();
			FileUtil.fullyDelete(baseDir);

			Configuration hdConf = new Configuration();
			hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
			hdConf.set("dfs.block.size", String.valueOf(1048576)); // this is the minimum we can set.

			MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
			hdfsCluster = builder.build();

			hdfsURI = "hdfs://" + hdfsCluster.getURI().getHost() + ":" + hdfsCluster.getNameNodePort() +"/";
			hdfs = new org.apache.hadoop.fs.Path(hdfsURI).getFileSystem(hdConf);

		} catch(Throwable e) {
			e.printStackTrace();
			Assert.fail("Test failed " + e.getMessage());
		}
	}

	@AfterClass
	public static void destroyHDFS() {
		try {
			FileUtil.fullyDelete(baseDir);
			hdfsCluster.shutdown();
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}

	private static String getResourceFilename(String filename) {
		ClassLoader cl = WindowOperatorTest.class.getClassLoader();
		URL resource = cl.getResource(filename);
		return resource.getFile();
	}

	//						END OF PREPARATIONS

	//						TESTS

	@Test
	public void testReaderSnapshotRestore() throws Exception {

		/*

		FileInputSplit split1 =
			new FileInputSplit(3, new Path("test/test1"), 0, 100, null);
		FileInputSplit split2 =
			new FileInputSplit(2, new Path("test/test2"), 101, 200, null);
		FileInputSplit split3 =
			new FileInputSplit(1, new Path("test/test2"), 0, 100, null);
		FileInputSplit split4 =
			new FileInputSplit(0, new Path("test/test3"), 0, 100, null);

		final OneShotLatch latch = new OneShotLatch();
		BlockingFileInputFormat format = new BlockingFileInputFormat(latch, new Path(hdfsURI));
		TypeInformation<FileInputSplit> typeInfo = TypeExtractor.getInputFormatTypes(format);
		ContinuousFileReaderOperator<FileInputSplit, ?> initReader = new ContinuousFileReaderOperator<>(format);
		initReader.setOutputType(typeInfo, new ExecutionConfig());
		OneInputStreamOperatorTestHarness<FileInputSplit, FileInputSplit> initTestInstance =
			new OneInputStreamOperatorTestHarness<>(initReader);
		initTestInstance.setTimeCharacteristic(TimeCharacteristic.EventTime);
		initTestInstance.open();
		// create some state in the reader
		initTestInstance.processElement(new StreamRecord<>(split1));
		initTestInstance.processElement(new StreamRecord<>(split2));
		initTestInstance.processElement(new StreamRecord<>(split3));
		initTestInstance.processElement(new StreamRecord<>(split4));
		// take a snapshot of the operator's state. This will be used
		// to initialize another reader and compare the results of the
		// two operators.
		final StreamTaskState snapshot;
		synchronized (initTestInstance.getCheckpointLock()) {
			snapshot = initTestInstance.snapshot(0L, 0L);
		}

		initTestInstance.snaphotToFile(snapshot, "src/test/resources/reader-migration-test-flink1.1-snapshot");

		*/
		TimestampedFileInputSplit split1 =
			new TimestampedFileInputSplit(0, 3, new Path("test/test1"), 0, 100, null);

		TimestampedFileInputSplit split2 =
			new TimestampedFileInputSplit(10, 2, new Path("test/test2"), 101, 200, null);

		TimestampedFileInputSplit split3 =
			new TimestampedFileInputSplit(10, 1, new Path("test/test2"), 0, 100, null);

		TimestampedFileInputSplit split4 =
			new TimestampedFileInputSplit(11, 0, new Path("test/test3"), 0, 100, null);


		final OneShotLatch latch = new OneShotLatch();

		BlockingFileInputFormat format = new BlockingFileInputFormat(latch, new Path(hdfsURI));
		TypeInformation<FileInputSplit> typeInfo = TypeExtractor.getInputFormatTypes(format);

		ContinuousFileReaderOperator<FileInputSplit> initReader = new ContinuousFileReaderOperator<>(format);
		initReader.setOutputType(typeInfo, new ExecutionConfig());

		OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, FileInputSplit> initTestInstance =
			new OneInputStreamOperatorTestHarness<>(initReader);
		initTestInstance.setTimeCharacteristic(TimeCharacteristic.EventTime);

		initTestInstance.setup();
		initTestInstance.initializeStateFromLegacyCheckpoint(getResourceFilename("reader-migration-test-flink1.1-snapshot"));
		initTestInstance.open();

		latch.trigger();

		// ... and wait for the operators to close gracefully

		synchronized (initTestInstance.getCheckpointLock()) {
			initTestInstance.close();
		}

		FileInputSplit fsSplit1 = createSplitFromTimestampedSplit(split1);
		FileInputSplit fsSplit2 = createSplitFromTimestampedSplit(split2);
		FileInputSplit fsSplit3 = createSplitFromTimestampedSplit(split3);
		FileInputSplit fsSplit4 = createSplitFromTimestampedSplit(split4);

		// compare if the results contain what they should contain and also if
		// they are the same, as they should.

		Assert.assertTrue(initTestInstance.getOutput().contains(new StreamRecord<>(fsSplit1)));
		Assert.assertTrue(initTestInstance.getOutput().contains(new StreamRecord<>(fsSplit2)));
		Assert.assertTrue(initTestInstance.getOutput().contains(new StreamRecord<>(fsSplit3)));
		Assert.assertTrue(initTestInstance.getOutput().contains(new StreamRecord<>(fsSplit4)));
	}

	private FileInputSplit createSplitFromTimestampedSplit(TimestampedFileInputSplit split) {
		Preconditions.checkNotNull(split);

		return new FileInputSplit(
			split.getSplitNumber(),
			split.getPath(),
			split.getStart(),
			split.getLength(),
			split.getHostnames()
		);
	}

	private static class BlockingFileInputFormat extends FileInputFormat<FileInputSplit> {

		private static final long serialVersionUID = -6727603565381560267L;

		private final OneShotLatch latch;

		private FileInputSplit split;

		private boolean reachedEnd;

		BlockingFileInputFormat(OneShotLatch latch, Path filePath) {
			super(filePath);
			this.latch = latch;
			this.reachedEnd = false;
		}

		@Override
		public void open(FileInputSplit fileSplit) throws IOException {
			this.split = fileSplit;
			this.reachedEnd = false;
		}

		@Override
		public boolean reachedEnd() throws IOException {
			if (!latch.isTriggered()) {
				try {
					latch.await();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			return reachedEnd;
		}

		@Override
		public FileInputSplit nextRecord(FileInputSplit reuse) throws IOException {
			this.reachedEnd = true;
			return split;
		}

		@Override
		public void close() {

		}
	}

	////				Monitoring Function Tests				//////

	@Test
	public void testFunctionRestore() throws Exception {

		/*
		org.apache.hadoop.fs.Path path = null;
		long fileModTime = Long.MIN_VALUE;
		for (int i = 0; i < 1; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file = fillWithData(hdfsURI, "file", i, "This is test line.");
			path = file.f0;
			fileModTime = hdfs.getFileStatus(file.f0).getModificationTime();
		}

		TextInputFormat format = new TextInputFormat(new Path(hdfsURI));

		final ContinuousFileMonitoringFunction<String> monitoringFunction =
			new ContinuousFileMonitoringFunction<>(format, format.getFilePath().toString(), new PathFilter(), FileProcessingMode.PROCESS_CONTINUOUSLY, 1, INTERVAL);

		StreamSource<FileInputSplit, ContinuousFileMonitoringFunction<String>> src =
			new StreamSource<>(monitoringFunction);

		final OneInputStreamOperatorTestHarness<Void, FileInputSplit> testHarness =
			new OneInputStreamOperatorTestHarness<>(src);
		testHarness.open();

		final Throwable[] error = new Throwable[1];

		final OneShotLatch latch = new OneShotLatch();

		// run the source asynchronously
		Thread runner = new Thread() {
			@Override
			public void run() {
				try {
					monitoringFunction.run(new DummySourceContext() {
						@Override
						public void collect(FileInputSplit element) {
							latch.trigger();
						}
					});
				}
				catch (Throwable t) {
					t.printStackTrace();
					error[0] = t;
				}
			}
		};
		runner.start();

		if (!latch.isTriggered()) {
			latch.await();
		}

		StreamTaskState snapshot = testHarness.snapshot(0, 0);
		testHarness.snaphotToFile(snapshot, "src/test/resources/monitoring-function-migration-test-" + fileModTime +"-flink1.1-snapshot");
		monitoringFunction.cancel();
		runner.join();

		testHarness.close();
		*/

		Long expectedModTime = Long.parseLong("1482144479339");
		TextInputFormat format = new TextInputFormat(new Path(hdfsURI));

		final ContinuousFileMonitoringFunction<String> monitoringFunction =
			new ContinuousFileMonitoringFunction<>(format, FileProcessingMode.PROCESS_CONTINUOUSLY, 1, INTERVAL);

		StreamSource<TimestampedFileInputSplit, ContinuousFileMonitoringFunction<String>> src =
			new StreamSource<>(monitoringFunction);

		final AbstractStreamOperatorTestHarness<TimestampedFileInputSplit> testHarness =
			new AbstractStreamOperatorTestHarness<>(src, 1, 1, 0);
		testHarness.setup();
		testHarness.initializeStateFromLegacyCheckpoint(getResourceFilename("monitoring-function-migration-test-1482144479339-flink1.1-snapshot"));
		testHarness.open();

		Assert.assertEquals((long) expectedModTime, monitoringFunction.getGlobalModificationTime());

	}

	///////////				Source Contexts Used by the tests				/////////////////

	private static abstract class DummySourceContext
		implements SourceFunction.SourceContext<TimestampedFileInputSplit> {

		private final Object lock = new Object();

		@Override
		public void collectWithTimestamp(TimestampedFileInputSplit element, long timestamp) {
		}

		@Override
		public void emitWatermark(Watermark mark) {
		}

		@Override
		public Object getCheckpointLock() {
			return lock;
		}

		@Override
		public void close() {
		}
	}

	/////////				Auxiliary Methods				/////////////

	/**
	 * Create a file with pre-determined String format of the form:
	 * {@code fileIdx +": "+ sampleLine +" "+ lineNo}.
	 * */
	private Tuple2<org.apache.hadoop.fs.Path, String> createFileAndFillWithData(
		String base, String fileName, int fileIdx, String sampleLine) throws IOException {

		assert (hdfs != null);

		org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(base + "/" + fileName + fileIdx);
		Assert.assertFalse(hdfs.exists(file));

		org.apache.hadoop.fs.Path tmp = new org.apache.hadoop.fs.Path(base + "/." + fileName + fileIdx);
		FSDataOutputStream stream = hdfs.create(tmp);
		StringBuilder str = new StringBuilder();
		for (int i = 0; i < LINES_PER_FILE; i++) {
			String line = fileIdx +": "+ sampleLine + " " + i +"\n";
			str.append(line);
			stream.write(line.getBytes());
		}
		stream.close();

		hdfs.rename(tmp, file);

		Assert.assertTrue("No result file present", hdfs.exists(file));
		return new Tuple2<>(file, str.toString());
	}
}
