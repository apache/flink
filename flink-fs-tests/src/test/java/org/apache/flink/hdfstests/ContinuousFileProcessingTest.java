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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperator;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
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
import java.util.concurrent.ConcurrentLinkedQueue;

public class ContinuousFileProcessingTest {

	private static final int NO_OF_FILES = 5;
	private static final int LINES_PER_FILE = 10;

	private static final long INTERVAL = 100;

	private static File baseDir;

	private static org.apache.hadoop.fs.FileSystem hdfs;
	private static String hdfsURI;
	private static MiniDFSCluster hdfsCluster;

	//						PREPARING FOR THE TESTS

	@BeforeClass
	public static void createHDFS() {
		try {
			baseDir = new File("./target/hdfs/hdfsTesting").getAbsoluteFile();
			FileUtil.fullyDelete(baseDir);

			org.apache.hadoop.conf.Configuration hdConf = new org.apache.hadoop.conf.Configuration();
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

	//						END OF PREPARATIONS

	//						TESTS

	@Test
	public void testFileReadingOperatorWithIngestionTime() throws Exception {
		Set<org.apache.hadoop.fs.Path> filesCreated = new HashSet<>();
		Map<Integer, String> expectedFileContents = new HashMap<>();
		Map<String, Long> modTimes = new HashMap<>();
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file = createFileAndFillWithData(hdfsURI, "file", i, "This is test line.");
			filesCreated.add(file.f0);
			modTimes.put(file.f0.getName(), hdfs.getFileStatus(file.f0).getModificationTime());
			expectedFileContents.put(i, file.f1);
		}

		TextInputFormat format = new TextInputFormat(new Path(hdfsURI));
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
		Set<org.apache.hadoop.fs.Path> filesCreated = new HashSet<>();
		Map<String, Long> modTimes = new HashMap<>();
		Map<Integer, String> expectedFileContents = new HashMap<>();
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file = createFileAndFillWithData(hdfsURI, "file", i, "This is test line.");
			modTimes.put(file.f0.getName(), hdfs.getFileStatus(file.f0).getModificationTime());
			filesCreated.add(file.f0);
			expectedFileContents.put(i, file.f1);
		}

		TextInputFormat format = new TextInputFormat(new Path(hdfsURI));
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

	////				Monitoring Function Tests				//////

	@Test
	public void testFilePathFiltering() throws Exception {
		Set<org.apache.hadoop.fs.Path> filesCreated = new HashSet<>();
		Set<String> filesKept = new TreeSet<>();

		// create the files to be discarded
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file = createFileAndFillWithData(hdfsURI, "**file", i, "This is test line.");
			filesCreated.add(file.f0);
		}

		// create the files to be kept
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file =
				createFileAndFillWithData(hdfsURI, "file", i, "This is test line.");
			filesCreated.add(file.f0);
			filesKept.add(file.f0.getName());
		}

		TextInputFormat format = new TextInputFormat(new Path(hdfsURI));
		format.setFilesFilter(new FilePathFilter() {
			@Override
			public boolean filterPath(Path filePath) {
				return filePath.getName().startsWith("**");
			}
		});

		ContinuousFileMonitoringFunction<String> monitoringFunction =
			new ContinuousFileMonitoringFunction<>(format, hdfsURI,
				FileProcessingMode.PROCESS_ONCE, 1, INTERVAL);

		final FileVerifyingSourceContext context =
			new FileVerifyingSourceContext(new OneShotLatch(), monitoringFunction, 0, -1);

		monitoringFunction.open(new Configuration());
		monitoringFunction.run(context);

		Assert.assertArrayEquals(filesKept.toArray(), context.getSeenFiles().toArray());

		// finally delete the files created for the test.
		for (org.apache.hadoop.fs.Path file: filesCreated) {
			hdfs.delete(file, false);
		}
	}

	@Test
	public void testSortingOnModTime() throws Exception {
		final long[] modTimes = new long[NO_OF_FILES];
		final org.apache.hadoop.fs.Path[] filesCreated = new org.apache.hadoop.fs.Path[NO_OF_FILES];

		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file =
				createFileAndFillWithData(hdfsURI, "file", i, "This is test line.");
			Thread.sleep(400);

			filesCreated[i] = file.f0;
			modTimes[i] = hdfs.getFileStatus(file.f0).getModificationTime();
		}

		TextInputFormat format = new TextInputFormat(new Path(hdfsURI));
		format.setFilesFilter(FilePathFilter.createDefaultFilter());

		// this is just to verify that all splits have been forwarded later.
		FileInputSplit[] splits = format.createInputSplits(1);

		ContinuousFileMonitoringFunction<String> monitoringFunction =
			new ContinuousFileMonitoringFunction<>(format, hdfsURI,
				FileProcessingMode.PROCESS_ONCE, 1, INTERVAL);

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
		final OneShotLatch latch = new OneShotLatch();

		// create a single file in the directory
		Tuple2<org.apache.hadoop.fs.Path, String> bootstrap =
			createFileAndFillWithData(hdfsURI, "file", NO_OF_FILES + 1, "This is test line.");
		Assert.assertTrue(hdfs.exists(bootstrap.f0));

		// the source is supposed to read only this file.
		final Set<String> filesToBeRead = new TreeSet<>();
		filesToBeRead.add(bootstrap.f0.getName());

		TextInputFormat format = new TextInputFormat(new Path(hdfsURI));
		format.setFilesFilter(FilePathFilter.createDefaultFilter());

		final ContinuousFileMonitoringFunction<String> monitoringFunction =
			new ContinuousFileMonitoringFunction<>(format, hdfsURI,
				FileProcessingMode.PROCESS_ONCE, 1, INTERVAL);

		final FileVerifyingSourceContext context =
			new FileVerifyingSourceContext(latch, monitoringFunction, 1, -1);

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

		// create some additional files that should be processed in the case of PROCESS_CONTINUOUSLY
		final org.apache.hadoop.fs.Path[] filesCreated = new org.apache.hadoop.fs.Path[NO_OF_FILES];
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> ignoredFile =
				createFileAndFillWithData(hdfsURI, "file", i, "This is test line.");
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
	public void testProcessContinuously() throws Exception {
		final OneShotLatch latch = new OneShotLatch();

		// create a single file in the directory
		Tuple2<org.apache.hadoop.fs.Path, String> bootstrap =
			createFileAndFillWithData(hdfsURI, "file", NO_OF_FILES + 1, "This is test line.");
		Assert.assertTrue(hdfs.exists(bootstrap.f0));

		final Set<String> filesToBeRead = new TreeSet<>();
		filesToBeRead.add(bootstrap.f0.getName());

		TextInputFormat format = new TextInputFormat(new Path(hdfsURI));
		format.setFilesFilter(FilePathFilter.createDefaultFilter());

		final ContinuousFileMonitoringFunction<String> monitoringFunction =
			new ContinuousFileMonitoringFunction<>(format, hdfsURI,
				FileProcessingMode.PROCESS_CONTINUOUSLY, 1, INTERVAL);

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
				createFileAndFillWithData(hdfsURI, "file", i, "This is test line.");
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
		private final int elementsBeforeNotifying;

		private int elementsBeforeCanceling = -1;

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
			if (seenFiles.size() == elementsBeforeNotifying) {
				latch.trigger();
			}

			if (elementsBeforeCanceling != -1 && seenFiles.size() == elementsBeforeCanceling) {
				src.cancel();
			}
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

	private int getLineNo(String line) {
		String[] tkns = line.split("\\s");
		Assert.assertEquals(6, tkns.length);
		return Integer.parseInt(tkns[tkns.length - 1]);
	}

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
