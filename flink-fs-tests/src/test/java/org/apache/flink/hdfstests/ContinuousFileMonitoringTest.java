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
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperator;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.TestTimeServiceProvider;
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

public class ContinuousFileMonitoringTest {

	private static final int NO_OF_FILES = 10;
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
		for(int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file = fillWithData(hdfsURI, "file", i, "This is test line.");
			filesCreated.add(file.f0);
			expectedFileContents.put(i, file.f1);
		}

		TextInputFormat format = new TextInputFormat(new Path(hdfsURI));
		TypeInformation<String> typeInfo = TypeExtractor.getInputFormatTypes(format);

		ContinuousFileReaderOperator<String, ?> reader = new ContinuousFileReaderOperator<>(format);

		ExecutionConfig executionConfig = new ExecutionConfig();
		executionConfig.setAutoWatermarkInterval(100);

		TestTimeServiceProvider timeServiceProvider = new TestTimeServiceProvider();
		OneInputStreamOperatorTestHarness<FileInputSplit, String> tester =
			new OneInputStreamOperatorTestHarness<>(reader, executionConfig,
				timeServiceProvider, TimeCharacteristic.IngestionTime);

		reader.setOutputType(typeInfo, executionConfig);
		tester.open();

		// test that watermarks are correctly emitted

		timeServiceProvider.setCurrentTime(201);
		timeServiceProvider.setCurrentTime(301);
		timeServiceProvider.setCurrentTime(401);
		timeServiceProvider.setCurrentTime(501);

		int i = 0;
		for(Object line: tester.getOutput()) {
			if (!(line instanceof Watermark)) {
				Assert.fail("Only watermarks are expected here ");
			}
			Watermark w = (Watermark) line;
			Assert.assertEquals(w.getTimestamp(), 200 + (i * 100));
			i++;
		}

		// clear the output to get the elements only and the final watermark
		tester.getOutput().clear();
		Assert.assertEquals(tester.getOutput().size(), 0);

		// create the necessary splits for the test
		FileInputSplit[] splits = format.createInputSplits(
			reader.getRuntimeContext().getNumberOfParallelSubtasks());

		// and feed them to the operator
		for(FileInputSplit split: splits) {
			tester.processElement(new StreamRecord<>(split));
		}

		// then close the reader gracefully so that
		// we wait until all input is read
		synchronized (tester.getCheckpointLock()) {
			tester.close();
		}

		for(org.apache.hadoop.fs.Path file: filesCreated) {
			hdfs.delete(file, false);
		}

		// the lines received must be the elements in the files +1 for the Long.MAX_VALUE watermark
		Assert.assertEquals(tester.getOutput().size(), NO_OF_FILES * LINES_PER_FILE + 1);

		// put the elements read in a map by file they belong to
		Map<Integer, List<String>> actualFileContents = new HashMap<>();
		for(Object line: tester.getOutput()) {
			if (line instanceof StreamRecord) {
				StreamRecord<String> element = (StreamRecord<String>) line;
				Assert.assertEquals(element.getTimestamp(), 501);

				int fileIdx = Character.getNumericValue(element.getValue().charAt(0));
				List<String> content = actualFileContents.get(fileIdx);
				if (content == null) {
					content = new ArrayList<>();
					actualFileContents.put(fileIdx, content);
				}
				content.add(element.getValue() + "\n");
			} else if (line instanceof Watermark) {
				Assert.assertEquals(((Watermark) line).getTimestamp(), Long.MAX_VALUE);
			} else {
				Assert.fail("Unknown element in the list.");
			}
		}

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
	public void testFileReadingOperator() throws Exception {
		Set<org.apache.hadoop.fs.Path> filesCreated = new HashSet<>();
		Map<Integer, String> expectedFileContents = new HashMap<>();
		for(int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file = fillWithData(hdfsURI, "file", i, "This is test line.");
			filesCreated.add(file.f0);
			expectedFileContents.put(i, file.f1);
		}

		TextInputFormat format = new TextInputFormat(new Path(hdfsURI));
		TypeInformation<String> typeInfo = TypeExtractor.getInputFormatTypes(format);

		ContinuousFileReaderOperator<String, ?> reader = new ContinuousFileReaderOperator<>(format);
		OneInputStreamOperatorTestHarness<FileInputSplit, String> tester =
			new OneInputStreamOperatorTestHarness<>(reader);

		reader.setOutputType(typeInfo, new ExecutionConfig());
		tester.open();

		// create the necessary splits for the test
		FileInputSplit[] splits = format.createInputSplits(
			reader.getRuntimeContext().getNumberOfParallelSubtasks());

		// and feed them to the operator
		for(FileInputSplit split: splits) {
			tester.processElement(new StreamRecord<>(split));
		}

		// then close the reader gracefully (and wait to finish reading)
		synchronized (tester.getCheckpointLock()) {
			tester.close();
		}

		// the lines received must be the elements in the files, no final watermark because
		// by default we are in processing time, which emits no watermarks.

		Assert.assertEquals(tester.getOutput().size(), NO_OF_FILES * LINES_PER_FILE);

		Map<Integer, List<String>> actualFileContents = new HashMap<>();
		for(Object line: tester.getOutput()) {
			StreamRecord<String> element = (StreamRecord<String>) line;

			int fileIdx = Character.getNumericValue(element.getValue().charAt(0));
			List<String> content = actualFileContents.get(fileIdx);
			if (content == null) {
				content = new ArrayList<>();
				actualFileContents.put(fileIdx, content);
			}
			content.add(element.getValue() +"\n");
		}

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

		for(org.apache.hadoop.fs.Path file: filesCreated) {
			hdfs.delete(file, false);
		}
	}

	private static class PathFilter extends FilePathFilter {

		@Override
		public boolean filterPath(Path filePath) {
			return filePath.getName().startsWith("**");
		}
	}

	@Test
	public void testFilePathFiltering() throws Exception {
		Set<String> uniqFilesFound = new HashSet<>();
		Set<org.apache.hadoop.fs.Path> filesCreated = new HashSet<>();

		// create the files to be discarded
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file = fillWithData(hdfsURI, "**file", i, "This is test line.");
			filesCreated.add(file.f0);
		}

		// create the files to be kept
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file = fillWithData(hdfsURI, "file", i, "This is test line.");
			filesCreated.add(file.f0);
		}

		TextInputFormat format = new TextInputFormat(new Path(hdfsURI));
		format.setFilesFilter(new PathFilter());
		ContinuousFileMonitoringFunction<String> monitoringFunction =
			new ContinuousFileMonitoringFunction<>(format, hdfsURI,
				FileProcessingMode.PROCESS_ONCE, 1, INTERVAL);

		monitoringFunction.open(new Configuration());
		monitoringFunction.run(new TestingSourceContext(monitoringFunction, uniqFilesFound));

		Assert.assertEquals(uniqFilesFound.size(), NO_OF_FILES);
		for(int i = 0; i < NO_OF_FILES; i++) {
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(hdfsURI + "/file" + i);
			Assert.assertTrue(uniqFilesFound.contains(file.toString()));
		}

		for(org.apache.hadoop.fs.Path file: filesCreated) {
			hdfs.delete(file, false);
		}
	}

	@Test
	public void testFileSplitMonitoringReprocessWithAppended() throws Exception {
		final Set<String> uniqFilesFound = new HashSet<>();

		FileCreator fc = new FileCreator(INTERVAL, NO_OF_FILES);
		fc.start();

		Thread t = new Thread(new Runnable() {
			@Override
			public void run() {
				TextInputFormat format = new TextInputFormat(new Path(hdfsURI));
				format.setFilesFilter(FilePathFilter.createDefaultFilter());
				ContinuousFileMonitoringFunction<String> monitoringFunction =
					new ContinuousFileMonitoringFunction<>(format, hdfsURI,
						FileProcessingMode.PROCESS_CONTINUOUSLY, 1, INTERVAL);

				try {
					monitoringFunction.open(new Configuration());
					monitoringFunction.run(new TestingSourceContext(monitoringFunction, uniqFilesFound));
				} catch (Exception e) {
					// do nothing as we interrupted the thread.
				}
			}
		});
		t.start();

		// wait until the sink also sees all the splits.
		synchronized (uniqFilesFound) {
			uniqFilesFound.wait();
		}
		t.interrupt();
		fc.join();

		Assert.assertEquals(fc.getFilesCreated().size(), NO_OF_FILES);
		Assert.assertEquals(uniqFilesFound.size(), NO_OF_FILES);

		Set<org.apache.hadoop.fs.Path> filesCreated = fc.getFilesCreated();
		Set<String> fileNamesCreated = new HashSet<>();
		for (org.apache.hadoop.fs.Path path: fc.getFilesCreated()) {
			fileNamesCreated.add(path.toString());
		}

		for(String file: uniqFilesFound) {
			Assert.assertTrue(fileNamesCreated.contains(file));
		}

		for(org.apache.hadoop.fs.Path file: filesCreated) {
			hdfs.delete(file, false);
		}
	}

	@Test
	public void testFileSplitMonitoringProcessOnce() throws Exception {
		Set<String> uniqFilesFound = new HashSet<>();

		FileCreator fc = new FileCreator(INTERVAL, 1);
		Set<org.apache.hadoop.fs.Path> filesCreated = fc.getFilesCreated();
		fc.start();

		// to make sure that at least one file is created
		if (filesCreated.size() == 0) {
			synchronized (filesCreated) {
				if (filesCreated.size() == 0) {
					filesCreated.wait();
				}
			}
		}
		Assert.assertTrue(fc.getFilesCreated().size() >= 1);

		TextInputFormat format = new TextInputFormat(new Path(hdfsURI));
		format.setFilesFilter(FilePathFilter.createDefaultFilter());
		ContinuousFileMonitoringFunction<String> monitoringFunction =
			new ContinuousFileMonitoringFunction<>(format, hdfsURI,
				FileProcessingMode.PROCESS_ONCE, 1, INTERVAL);

		monitoringFunction.open(new Configuration());
		monitoringFunction.run(new TestingSourceContext(monitoringFunction, uniqFilesFound));

		// wait until all the files are created
		fc.join();

		Assert.assertEquals(filesCreated.size(), NO_OF_FILES);

		Set<String> fileNamesCreated = new HashSet<>();
		for (org.apache.hadoop.fs.Path path: fc.getFilesCreated()) {
			fileNamesCreated.add(path.toString());
		}

		Assert.assertTrue(uniqFilesFound.size() >= 1 && uniqFilesFound.size() < fileNamesCreated.size());
		for(String file: uniqFilesFound) {
			Assert.assertTrue(fileNamesCreated.contains(file));
		}

		for(org.apache.hadoop.fs.Path file: filesCreated) {
			hdfs.delete(file, false);
		}
	}

	// -------------		End of Tests

	private int getLineNo(String line) {
		String[] tkns = line.split("\\s");
		Assert.assertEquals(tkns.length, 6);
		return Integer.parseInt(tkns[tkns.length - 1]);
	}

	/**
	 * A separate thread creating {@link #NO_OF_FILES} files, one file every {@link #INTERVAL} milliseconds.
	 * It serves for testing the file monitoring functionality of the {@link ContinuousFileMonitoringFunction}.
	 * The files are filled with data by the {@link #fillWithData(String, String, int, String)} method.
	 * */
	private class FileCreator extends Thread {

		private final long interval;
		private final int noOfFilesBeforeNotifying;

		private final Set<org.apache.hadoop.fs.Path> filesCreated = new HashSet<>();

		FileCreator(long interval, int notificationLim) {
			this.interval = interval;
			this.noOfFilesBeforeNotifying = notificationLim;
		}

		public void run() {
			try {
				for(int i = 0; i < NO_OF_FILES; i++) {
					Tuple2<org.apache.hadoop.fs.Path, String> file =
						fillWithData(hdfsURI, "file", i, "This is test line.");

					synchronized (filesCreated) {
						filesCreated.add(file.f0);
						if (filesCreated.size() == noOfFilesBeforeNotifying) {
							filesCreated.notifyAll();
						}
					}
					Thread.sleep(interval);
				}
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}

		Set<org.apache.hadoop.fs.Path> getFilesCreated() {
			return this.filesCreated;
		}
	}

	private class TestingSourceContext implements SourceFunction.SourceContext<FileInputSplit> {

		private final ContinuousFileMonitoringFunction src;
		private final Set<String> filesFound;
		private final Object lock = new Object();

		TestingSourceContext(ContinuousFileMonitoringFunction monitoringFunction, Set<String> uniqFilesFound) {
			this.filesFound = uniqFilesFound;
			this.src = monitoringFunction;
		}

		@Override
		public void collect(FileInputSplit element) {

			String filePath = element.getPath().toString();
			if (filesFound.contains(filePath)) {
				// check if we have duplicate splits that are open during the first time
				// the monitor sees them, and they then close, so the modification time changes.
				Assert.fail("Duplicate file: " + filePath);
			}

			synchronized (filesFound) {
				filesFound.add(filePath);
				try {
					if (filesFound.size() == NO_OF_FILES) {
						this.src.cancel();
						this.src.close();
						filesFound.notifyAll();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		@Override
		public void collectWithTimestamp(FileInputSplit element, long timestamp) {
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

	/**
	 * Fill the file with content.
	 * */
	private Tuple2<org.apache.hadoop.fs.Path, String> fillWithData(String base, String fileName, int fileIdx, String sampleLine) throws IOException {
		assert (hdfs != null);

		org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(base + "/" + fileName + fileIdx);
		Assert.assertFalse(hdfs.exists(file));

		org.apache.hadoop.fs.Path tmp = new org.apache.hadoop.fs.Path(base + "/." + fileName + fileIdx);
		FSDataOutputStream stream = hdfs.create(tmp);
		StringBuilder str = new StringBuilder();
		for(int i = 0; i < LINES_PER_FILE; i++) {
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
