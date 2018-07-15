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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperator;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Tests for the {@link ContinuousFileMonitoringFunction} and {@link ContinuousFileReaderOperator}.
 */
public class ContinuousFileProcessingTest {

	private static final int NO_OF_FILES = 5;
	private static final int LINES_PER_FILE = 10;

	private static final long INTERVAL = 100;

	private static FileSystem hdfs;
	private static String hdfsURI;
	private static MiniDFSCluster hdfsCluster;

	@ClassRule
	public static TemporaryFolder tempFolder = new TemporaryFolder();

	@BeforeClass
	public static void createHDFS() {
		Assume.assumeTrue("HDFS cluster cannot be start on Windows without extensions.", !OperatingSystem.isWindows());

		try {
			File hdfsDir = tempFolder.newFolder();

			org.apache.hadoop.conf.Configuration hdConf = new org.apache.hadoop.conf.Configuration();
			hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsDir.getAbsolutePath());
			hdConf.set("dfs.block.size", String.valueOf(1048576)); // this is the minimum we can set.

			MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(hdConf);
			hdfsCluster = builder.build();

			hdfsURI = "hdfs://" + hdfsCluster.getURI().getHost() + ":" + hdfsCluster.getNameNodePort() + "/";
			hdfs = new org.apache.hadoop.fs.Path(hdfsURI).getFileSystem(hdConf);

		} catch (Throwable e) {
			e.printStackTrace();
			Assert.fail("Test failed " + e.getMessage());
		}
	}

	@AfterClass
	public static void destroyHDFS() {
		if (hdfsCluster != null) {
			hdfsCluster.shutdown();
		}
	}

	@Test
	public void testInvalidPathSpecification() throws Exception {

		String invalidPath = "hdfs://" + hdfsCluster.getURI().getHost() + ":" + hdfsCluster.getNameNodePort() + "/invalid/";
		TextInputFormat format = new TextInputFormat(new Path(invalidPath));

		ContinuousFileMonitoringFunction<String> monitoringFunction =
			new ContinuousFileMonitoringFunction<>(format,
				FileProcessingMode.PROCESS_ONCE, 1, INTERVAL);
		try {
			monitoringFunction.run(new DummySourceContext() {
				@Override
				public void collect(TimestampedFileInputSplit element) {
					// we should never arrive here with an invalid path
					Assert.fail("Test passes with an invalid path.");
				}
			});

			// we should never arrive here with an invalid path
			Assert.fail("Test passed with an invalid path.");

		} catch (FileNotFoundException e) {
			Assert.assertEquals("The provided file path " + format.getFilePath() + " does not exist.", e.getMessage());
		}
	}

	@Test
	public void testFileReadingOperatorWithIngestionTime() throws Exception {
		String testBasePath = hdfsURI + "/" + UUID.randomUUID() + "/";

		Set<org.apache.hadoop.fs.Path> filesCreated = new HashSet<>();
		Map<Integer, String> expectedFileContents = new HashMap<>();
		Map<String, Long> modTimes = new HashMap<>();
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file = createFileAndFillWithData(testBasePath, "file", i, "This is test line.");
			filesCreated.add(file.f0);
			modTimes.put(file.f0.getName(), hdfs.getFileStatus(file.f0).getModificationTime());
			expectedFileContents.put(i, file.f1);
		}

		TextInputFormat format = new TextInputFormat(new Path(testBasePath));
		TypeInformation<String> typeInfo = TypeExtractor.getInputFormatTypes(format);

		final long watermarkInterval = 10;

		ContinuousFileReaderOperator<String> reader = new ContinuousFileReaderOperator<>(format);
		final OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> tester =
			new OneInputStreamOperatorTestHarness<>(reader);

		tester.getExecutionConfig().setAutoWatermarkInterval(watermarkInterval);
		tester.setTimeCharacteristic(TimeCharacteristic.IngestionTime);
		reader.setOutputType(typeInfo, tester.getExecutionConfig());

		tester.open();

		Assert.assertEquals(TimeCharacteristic.IngestionTime, tester.getTimeCharacteristic());

		// test that watermarks are correctly emitted

		ConcurrentLinkedQueue<Object> output = tester.getOutput();

		tester.setProcessingTime(201);
		Assert.assertTrue(output.peek() instanceof Watermark);
		Assert.assertEquals(200, ((Watermark) output.poll()).getTimestamp());

		tester.setProcessingTime(301);
		Assert.assertTrue(output.peek() instanceof Watermark);
		Assert.assertEquals(300, ((Watermark) output.poll()).getTimestamp());

		tester.setProcessingTime(401);
		Assert.assertTrue(output.peek() instanceof Watermark);
		Assert.assertEquals(400, ((Watermark) output.poll()).getTimestamp());

		tester.setProcessingTime(501);
		Assert.assertTrue(output.peek() instanceof Watermark);
		Assert.assertEquals(500, ((Watermark) output.poll()).getTimestamp());

		Assert.assertTrue(output.isEmpty());

		// create the necessary splits for the test
		FileInputSplit[] splits = format.createInputSplits(
			reader.getRuntimeContext().getNumberOfParallelSubtasks());

		// and feed them to the operator
		Map<Integer, List<String>> actualFileContents = new HashMap<>();

		long lastSeenWatermark = Long.MIN_VALUE;
		int lineCounter = 0;	// counter for the lines read from the splits
		int watermarkCounter = 0;

		for (FileInputSplit split: splits) {

			// set the next "current processing time".
			long nextTimestamp = tester.getProcessingTime() + watermarkInterval;
			tester.setProcessingTime(nextTimestamp);

			// send the next split to be read and wait until it is fully read, the +1 is for the watermark.
			tester.processElement(new StreamRecord<>(
				new TimestampedFileInputSplit(modTimes.get(split.getPath().getName()),
					split.getSplitNumber(), split.getPath(), split.getStart(),
					split.getLength(), split.getHostnames())));

			// NOTE: the following check works because each file fits in one split.
			// In other case it would fail and wait forever.
			// BUT THIS IS JUST FOR THIS TEST
			while (tester.getOutput().isEmpty() || tester.getOutput().size() != (LINES_PER_FILE + 1)) {
				Thread.sleep(10);
			}

			// verify that the results are the expected
			for (Object line: tester.getOutput()) {

				if (line instanceof StreamRecord) {

					@SuppressWarnings("unchecked")
					StreamRecord<String> element = (StreamRecord<String>) line;
					lineCounter++;

					Assert.assertEquals(nextTimestamp, element.getTimestamp());

					int fileIdx = Character.getNumericValue(element.getValue().charAt(0));
					List<String> content = actualFileContents.get(fileIdx);
					if (content == null) {
						content = new ArrayList<>();
						actualFileContents.put(fileIdx, content);
					}
					content.add(element.getValue() + "\n");
				} else if (line instanceof Watermark) {
					long watermark = ((Watermark) line).getTimestamp();

					Assert.assertEquals(nextTimestamp - (nextTimestamp % watermarkInterval), watermark);
					Assert.assertTrue(watermark > lastSeenWatermark);
					watermarkCounter++;

					lastSeenWatermark = watermark;
				} else {
					Assert.fail("Unknown element in the list.");
				}
			}

			// clean the output to be ready for the next split
			tester.getOutput().clear();
		}

		// now we are processing one split after the other,
		// so all the elements must be here by now.
		Assert.assertEquals(NO_OF_FILES * LINES_PER_FILE, lineCounter);

		// because we expect one watermark per split.
		Assert.assertEquals(splits.length, watermarkCounter);

		// then close the reader gracefully so that the Long.MAX watermark is emitted
		synchronized (tester.getCheckpointLock()) {
			tester.close();
		}

		for (org.apache.hadoop.fs.Path file: filesCreated) {
			hdfs.delete(file, false);
		}

		// check if the last element is the LongMax watermark (by now this must be the only element)
		Assert.assertEquals(1, tester.getOutput().size());
		Assert.assertTrue(tester.getOutput().peek() instanceof Watermark);
		Assert.assertEquals(Long.MAX_VALUE, ((Watermark) tester.getOutput().poll()).getTimestamp());

		// check if the elements are the expected ones.
		Assert.assertEquals(expectedFileContents.size(), actualFileContents.size());
		for (Integer fileIdx: expectedFileContents.keySet()) {
			Assert.assertTrue("file" + fileIdx + " not found", actualFileContents.keySet().contains(fileIdx));

			List<String> cntnt = actualFileContents.get(fileIdx);
			Collections.sort(cntnt, new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					return getLineNo(o1) - getLineNo(o2);
				}
			});

			StringBuilder cntntStr = new StringBuilder();
			for (String line: cntnt) {
				cntntStr.append(line);
			}
			Assert.assertEquals(expectedFileContents.get(fileIdx), cntntStr.toString());
		}
	}

	@Test
	public void testFileReadingOperatorWithEventTime() throws Exception {
		String testBasePath = hdfsURI + "/" + UUID.randomUUID() + "/";

		Set<org.apache.hadoop.fs.Path> filesCreated = new HashSet<>();
		Map<String, Long> modTimes = new HashMap<>();
		Map<Integer, String> expectedFileContents = new HashMap<>();
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file = createFileAndFillWithData(testBasePath, "file", i, "This is test line.");
			modTimes.put(file.f0.getName(), hdfs.getFileStatus(file.f0).getModificationTime());
			filesCreated.add(file.f0);
			expectedFileContents.put(i, file.f1);
		}

		TextInputFormat format = new TextInputFormat(new Path(testBasePath));
		TypeInformation<String> typeInfo = TypeExtractor.getInputFormatTypes(format);

		ContinuousFileReaderOperator<String> reader = new ContinuousFileReaderOperator<>(format);
		reader.setOutputType(typeInfo, new ExecutionConfig());

		OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, String> tester =
			new OneInputStreamOperatorTestHarness<>(reader);
		tester.setTimeCharacteristic(TimeCharacteristic.EventTime);
		tester.open();

		// create the necessary splits for the test
		FileInputSplit[] splits = format.createInputSplits(
			reader.getRuntimeContext().getNumberOfParallelSubtasks());

		// and feed them to the operator
		for (FileInputSplit split: splits) {
			tester.processElement(new StreamRecord<>(new TimestampedFileInputSplit(
				modTimes.get(split.getPath().getName()), split.getSplitNumber(), split.getPath(),
				split.getStart(), split.getLength(), split.getHostnames())));
		}

		// then close the reader gracefully (and wait to finish reading)
		synchronized (tester.getCheckpointLock()) {
			tester.close();
		}

		// the lines received must be the elements in the files +1 for for the longMax watermark
		// we are in event time, which emits no watermarks, so the last watermark will mark the
		// of the input stream.

		Assert.assertEquals(NO_OF_FILES * LINES_PER_FILE + 1, tester.getOutput().size());

		Map<Integer, List<String>> actualFileContents = new HashMap<>();
		Object lastElement = null;
		for (Object line: tester.getOutput()) {
			lastElement = line;

			if (line instanceof StreamRecord) {

				@SuppressWarnings("unchecked")
				StreamRecord<String> element = (StreamRecord<String>) line;

				int fileIdx = Character.getNumericValue(element.getValue().charAt(0));
				List<String> content = actualFileContents.get(fileIdx);
				if (content == null) {
					content = new ArrayList<>();
					actualFileContents.put(fileIdx, content);
				}
				content.add(element.getValue() + "\n");
			}
		}

		// check if the last element is the LongMax watermark
		Assert.assertTrue(lastElement instanceof Watermark);
		Assert.assertEquals(Long.MAX_VALUE, ((Watermark) lastElement).getTimestamp());

		Assert.assertEquals(expectedFileContents.size(), actualFileContents.size());
		for (Integer fileIdx: expectedFileContents.keySet()) {
			Assert.assertTrue("file" + fileIdx + " not found", actualFileContents.keySet().contains(fileIdx));

			List<String> cntnt = actualFileContents.get(fileIdx);
			Collections.sort(cntnt, new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					return getLineNo(o1) - getLineNo(o2);
				}
			});

			StringBuilder cntntStr = new StringBuilder();
			for (String line: cntnt) {
				cntntStr.append(line);
			}
			Assert.assertEquals(expectedFileContents.get(fileIdx), cntntStr.toString());
		}

		for (org.apache.hadoop.fs.Path file: filesCreated) {
			hdfs.delete(file, false);
		}
	}

	@Test
	public void testReaderSnapshotRestore() throws Exception {
		String testBasePath = hdfsURI + "/" + UUID.randomUUID() + "/";

		TimestampedFileInputSplit split1 =
			new TimestampedFileInputSplit(0, 3, new Path("test/test1"), 0, 100, null);

		TimestampedFileInputSplit split2 =
			new TimestampedFileInputSplit(10, 2, new Path("test/test2"), 101, 200, null);

		TimestampedFileInputSplit split3 =
			new TimestampedFileInputSplit(10, 1, new Path("test/test2"), 0, 100, null);

		TimestampedFileInputSplit split4 =
			new TimestampedFileInputSplit(11, 0, new Path("test/test3"), 0, 100, null);

		final OneShotLatch latch = new OneShotLatch();

		BlockingFileInputFormat format = new BlockingFileInputFormat(latch, new Path(testBasePath));
		TypeInformation<FileInputSplit> typeInfo = TypeExtractor.getInputFormatTypes(format);

		ContinuousFileReaderOperator<FileInputSplit> initReader = new ContinuousFileReaderOperator<>(format);
		initReader.setOutputType(typeInfo, new ExecutionConfig());

		OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, FileInputSplit> initTestInstance =
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

		final OperatorSubtaskState snapshot;
		synchronized (initTestInstance.getCheckpointLock()) {
			snapshot = initTestInstance.snapshot(0L, 0L);
		}

		ContinuousFileReaderOperator<FileInputSplit> restoredReader = new ContinuousFileReaderOperator<>(
			new BlockingFileInputFormat(latch, new Path(testBasePath)));
		restoredReader.setOutputType(typeInfo, new ExecutionConfig());

		OneInputStreamOperatorTestHarness<TimestampedFileInputSplit, FileInputSplit> restoredTestInstance  =
			new OneInputStreamOperatorTestHarness<>(restoredReader);
		restoredTestInstance.setTimeCharacteristic(TimeCharacteristic.EventTime);

		restoredTestInstance.initializeState(snapshot);
		restoredTestInstance.open();

		// now let computation start
		latch.trigger();

		// ... and wait for the operators to close gracefully

		synchronized (initTestInstance.getCheckpointLock()) {
			initTestInstance.close();
		}

		synchronized (restoredTestInstance.getCheckpointLock()) {
			restoredTestInstance.close();
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

		Assert.assertArrayEquals(
			initTestInstance.getOutput().toArray(),
			restoredTestInstance.getOutput().toArray()
		);
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
	public void testFilePathFiltering() throws Exception {
		String testBasePath = hdfsURI + "/" + UUID.randomUUID() + "/";

		Set<org.apache.hadoop.fs.Path> filesCreated = new HashSet<>();
		Set<String> filesKept = new TreeSet<>();

		// create the files to be discarded
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file = createFileAndFillWithData(testBasePath, "**file", i, "This is test line.");
			filesCreated.add(file.f0);
		}

		// create the files to be kept
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file =
				createFileAndFillWithData(testBasePath, "file", i, "This is test line.");
			filesCreated.add(file.f0);
			filesKept.add(file.f0.getName());
		}

		TextInputFormat format = new TextInputFormat(new Path(testBasePath));
		format.setFilesFilter(new FilePathFilter() {

			private static final long serialVersionUID = 2611449927338589804L;

			@Override
			public boolean filterPath(Path filePath) {
				return filePath.getName().startsWith("**");
			}
		});

		ContinuousFileMonitoringFunction<String> monitoringFunction =
			createTestContinuousFileMonitoringFunction(format, FileProcessingMode.PROCESS_ONCE);

		final FileVerifyingSourceContext context =
			new FileVerifyingSourceContext(new OneShotLatch(), monitoringFunction);

		monitoringFunction.open(new Configuration());
		monitoringFunction.run(context);

		Assert.assertArrayEquals(filesKept.toArray(), context.getSeenFiles().toArray());

		// finally delete the files created for the test.
		for (org.apache.hadoop.fs.Path file: filesCreated) {
			hdfs.delete(file, false);
		}
	}

	@Test
	public void testNestedFilesProcessing() throws Exception {
		String testBasePath = hdfsURI + "/" + UUID.randomUUID() + "/";

		final Set<org.apache.hadoop.fs.Path> filesCreated = new HashSet<>();
		final Set<String> filesToBeRead = new TreeSet<>();

		// create two nested directories
		org.apache.hadoop.fs.Path firstLevelDir = new org.apache.hadoop.fs.Path(testBasePath + "/" + "firstLevelDir");
		org.apache.hadoop.fs.Path secondLevelDir = new org.apache.hadoop.fs.Path(testBasePath + "/" + "firstLevelDir" + "/" + "secondLevelDir");
		Assert.assertFalse(hdfs.exists(firstLevelDir));
		hdfs.mkdirs(firstLevelDir);
		hdfs.mkdirs(secondLevelDir);

		// create files in the base dir, the first level dir and the second level dir
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file = createFileAndFillWithData(testBasePath, "firstLevelFile", i, "This is test line.");
			filesCreated.add(file.f0);
			filesToBeRead.add(file.f0.getName());
		}
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file = createFileAndFillWithData(firstLevelDir.toString(), "secondLevelFile", i, "This is test line.");
			filesCreated.add(file.f0);
			filesToBeRead.add(file.f0.getName());
		}
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file = createFileAndFillWithData(secondLevelDir.toString(), "thirdLevelFile", i, "This is test line.");
			filesCreated.add(file.f0);
			filesToBeRead.add(file.f0.getName());
		}

		TextInputFormat format = new TextInputFormat(new Path(testBasePath));
		format.setFilesFilter(FilePathFilter.createDefaultFilter());
		format.setNestedFileEnumeration(true);

		ContinuousFileMonitoringFunction<String> monitoringFunction =
			createTestContinuousFileMonitoringFunction(format, FileProcessingMode.PROCESS_ONCE);

		final FileVerifyingSourceContext context =
			new FileVerifyingSourceContext(new OneShotLatch(), monitoringFunction);

		monitoringFunction.open(new Configuration());
		monitoringFunction.run(context);

		Assert.assertArrayEquals(filesToBeRead.toArray(), context.getSeenFiles().toArray());

		// finally delete the dirs and the files created for the test.
		for (org.apache.hadoop.fs.Path file: filesCreated) {
			hdfs.delete(file, false);
		}
		hdfs.delete(secondLevelDir, false);
		hdfs.delete(firstLevelDir, false);
	}

	@Test
	public void testSortingOnModTime() throws Exception {
		String testBasePath = hdfsURI + "/" + UUID.randomUUID() + "/";

		final long[] modTimes = new long[NO_OF_FILES];
		final org.apache.hadoop.fs.Path[] filesCreated = new org.apache.hadoop.fs.Path[NO_OF_FILES];

		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file =
				createFileAndFillWithData(testBasePath, "file", i, "This is test line.");
			Thread.sleep(400);

			filesCreated[i] = file.f0;
			modTimes[i] = hdfs.getFileStatus(file.f0).getModificationTime();
		}

		TextInputFormat format = new TextInputFormat(new Path(testBasePath));
		format.setFilesFilter(FilePathFilter.createDefaultFilter());

		// this is just to verify that all splits have been forwarded later.
		FileInputSplit[] splits = format.createInputSplits(1);

		ContinuousFileMonitoringFunction<String> monitoringFunction =
			createTestContinuousFileMonitoringFunction(format, FileProcessingMode.PROCESS_ONCE);

		ModTimeVerifyingSourceContext context = new ModTimeVerifyingSourceContext(modTimes);

		monitoringFunction.open(new Configuration());
		monitoringFunction.run(context);
		Assert.assertEquals(splits.length, context.getCounter());

		// delete the created files.
		for (int i = 0; i < NO_OF_FILES; i++) {
			hdfs.delete(filesCreated[i], false);
		}
	}

	@Test
	public void testProcessOnce() throws Exception {
		String testBasePath = hdfsURI + "/" + UUID.randomUUID() + "/";

		final OneShotLatch latch = new OneShotLatch();

		// create a single file in the directory
		Tuple2<org.apache.hadoop.fs.Path, String> bootstrap =
			createFileAndFillWithData(testBasePath, "file", NO_OF_FILES + 1, "This is test line.");
		Assert.assertTrue(hdfs.exists(bootstrap.f0));

		// the source is supposed to read only this file.
		final Set<String> filesToBeRead = new TreeSet<>();
		filesToBeRead.add(bootstrap.f0.getName());

		TextInputFormat format = new TextInputFormat(new Path(testBasePath));
		format.setFilesFilter(FilePathFilter.createDefaultFilter());

		final ContinuousFileMonitoringFunction<String> monitoringFunction =
			createTestContinuousFileMonitoringFunction(format, FileProcessingMode.PROCESS_ONCE);

		final FileVerifyingSourceContext context = new FileVerifyingSourceContext(latch, monitoringFunction);

		final Thread t = new Thread() {
			@Override
			public void run() {
				try {
					monitoringFunction.open(new Configuration());
					monitoringFunction.run(context);

					// we would never arrive here if we were in
					// PROCESS_CONTINUOUSLY mode.

					// this will trigger the latch
					context.close();

				} catch (Exception e) {
					Assert.fail(e.getMessage());
				}
			}
		};
		t.start();

		if (!latch.isTriggered()) {
			latch.await();
		}

		// create some additional files that should be processed in the case of PROCESS_CONTINUOUSLY
		final org.apache.hadoop.fs.Path[] filesCreated = new org.apache.hadoop.fs.Path[NO_OF_FILES];
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> ignoredFile =
				createFileAndFillWithData(testBasePath, "file", i, "This is test line.");
			filesCreated[i] = ignoredFile.f0;
		}

		// wait until the monitoring thread exits
		t.join();

		Assert.assertArrayEquals(filesToBeRead.toArray(), context.getSeenFiles().toArray());

		// finally delete the files created for the test.
		hdfs.delete(bootstrap.f0, false);
		for (org.apache.hadoop.fs.Path path: filesCreated) {
			hdfs.delete(path, false);
		}
	}

	@Test
	public void testFunctionRestore() throws Exception {
		String testBasePath = hdfsURI + "/" + UUID.randomUUID() + "/";

		org.apache.hadoop.fs.Path path = null;
		long fileModTime = Long.MIN_VALUE;
		for (int i = 0; i < 1; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file = createFileAndFillWithData(testBasePath, "file", i, "This is test line.");
			path = file.f0;
			fileModTime = hdfs.getFileStatus(file.f0).getModificationTime();
		}

		TextInputFormat format = new TextInputFormat(new Path(testBasePath));

		final ContinuousFileMonitoringFunction<String> monitoringFunction =
			createTestContinuousFileMonitoringFunction(format, FileProcessingMode.PROCESS_CONTINUOUSLY);

		StreamSource<TimestampedFileInputSplit, ContinuousFileMonitoringFunction<String>> src =
			new StreamSource<>(monitoringFunction);

		final AbstractStreamOperatorTestHarness<TimestampedFileInputSplit> testHarness =
			new AbstractStreamOperatorTestHarness<>(src, 1, 1, 0);
		testHarness.open();

		final Throwable[] error = new Throwable[1];

		final OneShotLatch latch = new OneShotLatch();

		final DummySourceContext sourceContext = new DummySourceContext() {
			@Override
			public void collect(TimestampedFileInputSplit element) {
				latch.trigger();
			}
		};

		// run the source asynchronously
		Thread runner = new Thread() {
			@Override
			public void run() {
				try {
					monitoringFunction.run(sourceContext);
				}
				catch (Throwable t) {
					t.printStackTrace();
					error[0] = t;
				}
			}
		};
		runner.start();

		// first condition for the source to have updated its state: emit at least one element
		if (!latch.isTriggered()) {
			latch.await();
		}

		// second condition for the source to have updated its state: it's not on the lock anymore,
		// this means it has processed all the splits and updated its state.
		synchronized (sourceContext.getCheckpointLock()) {}

		OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);
		monitoringFunction.cancel();
		runner.join();

		testHarness.close();

		final ContinuousFileMonitoringFunction<String> monitoringFunctionCopy =
			createTestContinuousFileMonitoringFunction(format, FileProcessingMode.PROCESS_CONTINUOUSLY);

		StreamSource<TimestampedFileInputSplit, ContinuousFileMonitoringFunction<String>> srcCopy =
			new StreamSource<>(monitoringFunctionCopy);

		AbstractStreamOperatorTestHarness<TimestampedFileInputSplit> testHarnessCopy =
			new AbstractStreamOperatorTestHarness<>(srcCopy, 1, 1, 0);
		testHarnessCopy.initializeState(snapshot);
		testHarnessCopy.open();

		Assert.assertNull(error[0]);
		Assert.assertEquals(fileModTime, monitoringFunctionCopy.getGlobalModificationTime());

		hdfs.delete(path, false);
	}

	@Test
	public void testProcessContinuously() throws Exception {
		String testBasePath = hdfsURI + "/" + UUID.randomUUID() + "/";

		final OneShotLatch latch = new OneShotLatch();

		// create a single file in the directory
		Tuple2<org.apache.hadoop.fs.Path, String> bootstrap =
			createFileAndFillWithData(testBasePath, "file", NO_OF_FILES + 1, "This is test line.");
		Assert.assertTrue(hdfs.exists(bootstrap.f0));

		final Set<String> filesToBeRead = new TreeSet<>();
		filesToBeRead.add(bootstrap.f0.getName());

		TextInputFormat format = new TextInputFormat(new Path(testBasePath));
		format.setFilesFilter(FilePathFilter.createDefaultFilter());

		final ContinuousFileMonitoringFunction<String> monitoringFunction =
			createTestContinuousFileMonitoringFunction(format, FileProcessingMode.PROCESS_CONTINUOUSLY);

		final int totalNoOfFilesToBeRead = NO_OF_FILES + 1; // 1 for the bootstrap + NO_OF_FILES
		final FileVerifyingSourceContext context = new FileVerifyingSourceContext(latch,
			monitoringFunction, 1, totalNoOfFilesToBeRead);

		final Thread t = new Thread() {

			@Override
			public void run() {
				try {
					monitoringFunction.open(new Configuration());
					monitoringFunction.run(context);
				} catch (Exception e) {
					Assert.fail(e.getMessage());
				}
			}
		};
		t.start();

		if (!latch.isTriggered()) {
			latch.await();
		}

		// create some additional files that will be processed in the case of PROCESS_CONTINUOUSLY
		final org.apache.hadoop.fs.Path[] filesCreated = new org.apache.hadoop.fs.Path[NO_OF_FILES];
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file =
				createFileAndFillWithData(testBasePath, "file", i, "This is test line.");
			filesCreated[i] = file.f0;
			filesToBeRead.add(file.f0.getName());
		}

		// wait until the monitoring thread exits
		t.join();

		Assert.assertArrayEquals(filesToBeRead.toArray(), context.getSeenFiles().toArray());

		// finally delete the files created for the test.
		hdfs.delete(bootstrap.f0, false);
		for (org.apache.hadoop.fs.Path path: filesCreated) {
			hdfs.delete(path, false);
		}
	}

	///////////				Source Contexts Used by the tests				/////////////////

	private static class FileVerifyingSourceContext extends DummySourceContext {

		private final ContinuousFileMonitoringFunction src;
		private final OneShotLatch latch;
		private final Set<String> seenFiles;

		private int elementsBeforeNotifying = -1;
		private int elementsBeforeCanceling = -1;

		FileVerifyingSourceContext(OneShotLatch latch, ContinuousFileMonitoringFunction src) {
			this(latch, src, -1, -1);
		}

		FileVerifyingSourceContext(OneShotLatch latch,
								ContinuousFileMonitoringFunction src,
								int elementsBeforeNotifying,
								int elementsBeforeCanceling) {
			this.latch = latch;
			this.seenFiles = new TreeSet<>();
			this.src = src;
			this.elementsBeforeNotifying = elementsBeforeNotifying;
			this.elementsBeforeCanceling = elementsBeforeCanceling;
		}

		Set<String> getSeenFiles() {
			return this.seenFiles;
		}

		@Override
		public void collect(TimestampedFileInputSplit element) {
			String seenFileName = element.getPath().getName();
			this.seenFiles.add(seenFileName);

			if (seenFiles.size() == elementsBeforeNotifying && !latch.isTriggered()) {
				latch.trigger();
			}

			if (seenFiles.size() == elementsBeforeCanceling) {
				src.cancel();
			}
		}

		@Override
		public void close() {
			// the context was terminated so trigger so
			// that all threads that were waiting for this
			// are un-blocked.
			if (!latch.isTriggered()) {
				latch.trigger();
			}
			src.cancel();
		}
	}

	private static class ModTimeVerifyingSourceContext extends DummySourceContext {

		final long[] expectedModificationTimes;
		int splitCounter;
		long lastSeenModTime;

		ModTimeVerifyingSourceContext(long[] modTimes) {
			this.expectedModificationTimes = modTimes;
			this.splitCounter = 0;
			this.lastSeenModTime = Long.MIN_VALUE;
		}

		int getCounter() {
			return splitCounter;
		}

		@Override
		public void collect(TimestampedFileInputSplit element) {
			try {
				long modTime = hdfs.getFileStatus(new org.apache.hadoop.fs.Path(element.getPath().getPath())).getModificationTime();

				Assert.assertTrue(modTime >= lastSeenModTime);
				Assert.assertEquals(expectedModificationTimes[splitCounter], modTime);

				lastSeenModTime = modTime;
				splitCounter++;
			} catch (IOException e) {
				Assert.fail(e.getMessage());
			}
		}
	}

	private abstract static class DummySourceContext
			implements SourceFunction.SourceContext<TimestampedFileInputSplit> {

		private final Object lock = new Object();

		@Override
		public void collectWithTimestamp(TimestampedFileInputSplit element, long timestamp) {
		}

		@Override
		public void emitWatermark(Watermark mark) {
		}

		@Override
		public void markAsTemporarilyIdle() {
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

	private static int getLineNo(String line) {
		String[] tkns = line.split("\\s");
		Assert.assertEquals(6, tkns.length);
		return Integer.parseInt(tkns[tkns.length - 1]);
	}

	/**
	 * Create a file with pre-determined String format of the form:
	 * {@code fileIdx +": "+ sampleLine +" "+ lineNo}.
	 * */
	private static Tuple2<org.apache.hadoop.fs.Path, String> createFileAndFillWithData(
				String base, String fileName, int fileIdx, String sampleLine) throws IOException {

		assert (hdfs != null);

		final String fileRandSuffix = UUID.randomUUID().toString();

		org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(base + "/" + fileName + fileRandSuffix);
		Assert.assertFalse(hdfs.exists(file));

		org.apache.hadoop.fs.Path tmp = new org.apache.hadoop.fs.Path(base + "/." + fileName + fileRandSuffix);
		FSDataOutputStream stream = hdfs.create(tmp);
		StringBuilder str = new StringBuilder();
		for (int i = 0; i < LINES_PER_FILE; i++) {
			String line = fileIdx + ": " + sampleLine + " " + i + "\n";
			str.append(line);
			stream.write(line.getBytes(ConfigConstants.DEFAULT_CHARSET));
		}
		stream.close();

		hdfs.rename(tmp, file);

		Assert.assertTrue("No result file present", hdfs.exists(file));
		return new Tuple2<>(file, str.toString());
	}

	/**
	 * Create continuous monitoring function with 1 reader-parallelism and interval: {@link #INTERVAL}.
	 */
	private <OUT> ContinuousFileMonitoringFunction<OUT> createTestContinuousFileMonitoringFunction(FileInputFormat<OUT> format, FileProcessingMode fileProcessingMode) {
		ContinuousFileMonitoringFunction<OUT> monitoringFunction =
			new ContinuousFileMonitoringFunction<>(format, fileProcessingMode, 1, INTERVAL);
		monitoringFunction.setRuntimeContext(Mockito.mock(RuntimeContext.class));
		return monitoringFunction;
	}
}
