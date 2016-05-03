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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

public class FileSplitMonitoringFunctionTest {

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
		FileUtil.fullyDelete(baseDir);
		hdfsCluster.shutdown();
	}

	//						END OF PREPARATIONS

	//						TESTS

	@Test
	public void testFileReadingOperator() throws Exception {
		Set<org.apache.hadoop.fs.Path> filesCreated = new HashSet<>();
		Map<Integer, String> fileContents = new HashMap<>();
		for(int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> file = fillWithData(hdfsURI, "file", i, "This is test line.");
			filesCreated.add(file.f0);
			fileContents.put(i, file.f1);
		}

		TextInputFormat format = new TextInputFormat(new Path(hdfsURI));
		Configuration config = new Configuration();
		config.setString("input.file.path", hdfsURI);

		TypeInformation<String> typeInfo = TypeExtractor.getInputFormatTypes(format);

		FileSplitReadOperator<String> reader = new FileSplitReadOperator<>(format, typeInfo, config);

		OneInputStreamOperatorTestHarness<FileInputSplit, String> tester =
			new OneInputStreamOperatorTestHarness<>(reader);
		tester.open();

		FileInputSplit[] splits = format.createInputSplits(
			reader.getRuntimeContext().getNumberOfParallelSubtasks());

		for(FileInputSplit split: splits) {
			tester.processElement(new StreamRecord<>(split));
		}

		/*
		* Given that the reader is multithreaded, the test finishes before the reader finishes
		* reading. This results in files being deleted before they are read, thus throwing an exception.
		* In addition, even if file deletion happens at the end, the results are not ready for testing.
		* To faces this, we wait until all the output is collected or until the waiting time exceeds 1000 ms, or 1s.
		* */
		long start = System.currentTimeMillis();
		Queue<Object> output;
		do {
			output = tester.getOutput();
		} while ((output == null || output.size() != NO_OF_FILES * LINES_PER_FILE) && (System.currentTimeMillis() - start) < 1000);

		tester.close();

		Map<Integer, String> resFileContents = new HashMap<>();
		for(Object line: tester.getOutput()) {
			StreamRecord<String> element = (StreamRecord<String>) line;

			int fileIdx = Character.getNumericValue(element.getValue().charAt(0));
			String content = resFileContents.get(fileIdx);
			if(content == null) {
				content = "";
			}
			resFileContents.put(fileIdx, content + element.getValue() +"\n");
		}

		// test if all the file contents are the expected ones.
		for(int fileIdx: fileContents.keySet()) {
			Assert.assertEquals(resFileContents.get(fileIdx), fileContents.get(fileIdx));
		}

		for(org.apache.hadoop.fs.Path file: filesCreated) {
			hdfs.delete(file, false);
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

		Configuration config = new Configuration();
		config.setString("input.file.path", hdfsURI);

		FileSplitMonitoringFunction<String> monitoringFunction =
			new FileSplitMonitoringFunction<>(format, hdfsURI, config, new PathFilter(),
				FileSplitMonitoringFunction.WatchType.REPROCESS_WITH_APPENDED, 1, INTERVAL);

		monitoringFunction.open(config);
		monitoringFunction.run(new TestingSourceContext(null, monitoringFunction, uniqFilesFound));

		Assert.assertTrue(uniqFilesFound.size() == NO_OF_FILES);
		for(int i = 0; i < NO_OF_FILES; i++) {
			org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(hdfsURI + "/file" + i);
			Assert.assertTrue(uniqFilesFound.contains(file.toString()));
		}

		for(org.apache.hadoop.fs.Path file: filesCreated) {
			hdfs.delete(file, false);
		}
	}

	private static class PathFilter implements FilePathFilter {

		@Override
		public boolean filterPath(Path filePath) {
			return filePath.getName().startsWith("**");
		}
	}

	@Test
	public void testFileSplitMonitoring() throws Exception {
		Set<String> uniqFilesFound = new HashSet<>();

		FileCreator fc = new FileCreator(INTERVAL);
		fc.start();

		TextInputFormat format = new TextInputFormat(new Path(hdfsURI));
		Configuration config = new Configuration();
		config.setString("input.file.path", hdfsURI);

		FileSplitMonitoringFunction<String> monitoringFunction =
			new FileSplitMonitoringFunction<>(format, hdfsURI, config,
				FileSplitMonitoringFunction.WatchType.REPROCESS_WITH_APPENDED, 1, INTERVAL);
		monitoringFunction.open(config);
		monitoringFunction.run(new TestingSourceContext(fc, monitoringFunction, uniqFilesFound));

		Set<String> fileNamesCreated = new HashSet<>();
		for (org.apache.hadoop.fs.Path path: fc.getFilesCreated()) {
			fileNamesCreated.add(path.toString());
		}

		Assert.assertTrue(uniqFilesFound.size() == NO_OF_FILES);
		for(String file: uniqFilesFound) {
			Assert.assertTrue(fileNamesCreated.contains(file));
		}
	}

	// -------------		End of Tests

	/**
	 * A separate thread creating {@link #NO_OF_FILES} files, one file every {@link #INTERVAL} milliseconds.
	 * It serves for testing the file monitoring functionality of the {@link FileSplitMonitoringFunction}.
	 * The files are filled with data by the {@link #fillWithData(String, String, int, String)} method.
	 * */
	private class FileCreator extends Thread {

		private final long interval;

		private final Set<org.apache.hadoop.fs.Path> filesCreated = new HashSet<>();

		FileCreator(long interval) {
			this.interval = interval;
		}

		public void run() {
			try {
				for(int i = 0; i < NO_OF_FILES; i++) {
					Tuple2<org.apache.hadoop.fs.Path, String> file = fillWithData(hdfsURI, "file", i, "This is test line.");
					filesCreated.add(file.f0);
					Thread.sleep(interval);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				// we just close without any message.
			}
		}

		Set<org.apache.hadoop.fs.Path> getFilesCreated() {
			return this.filesCreated;
		}
	}

	private class TestingSourceContext implements SourceFunction.SourceContext<FileInputSplit> {

		private final FileCreator fileCreator;
		private final FileSplitMonitoringFunction src;
		private final Set<String> filesFound;

		TestingSourceContext(FileCreator fileCreationThread, FileSplitMonitoringFunction monitoringFunction, Set<String> uniqFilesFound) {
			this.filesFound = uniqFilesFound;
			this.src = monitoringFunction;
			this.fileCreator = fileCreationThread;
		}

		@Override
		public void collect(FileInputSplit element) {

			String filePath = element.getPath().toString();

			if (filesFound.contains(filePath)) {
				// check if we have duplicate splits that are open during the first time
				// the monitor sees them, and they then close, so the modification time changes.
				Assert.fail("Duplicate file: " + filePath);
			}

			filesFound.add(filePath);
			try {
				if (filesFound.size() == NO_OF_FILES) {
					this.src.cancel();
					this.src.close();
					if (this.fileCreator != null) {
						this.fileCreator.interrupt();
						for(org.apache.hadoop.fs.Path file : this.fileCreator.getFilesCreated()) {
							hdfs.delete(file, false);
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
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
			return null;
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
