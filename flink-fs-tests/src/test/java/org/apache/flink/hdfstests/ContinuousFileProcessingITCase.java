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

import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileReaderOperator;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.TimestampedFileInputSplit;
import org.apache.flink.test.util.AbstractTestBase;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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

import static org.junit.Assert.assertEquals;

/**
 * IT cases for the {@link ContinuousFileMonitoringFunction} and {@link ContinuousFileReaderOperator}.
 */
public class ContinuousFileProcessingITCase extends AbstractTestBase {

	private static final int NO_OF_FILES = 5;
	private static final int LINES_PER_FILE = 100;

	private static final int PARALLELISM = 4;
	private static final long INTERVAL = 100;

	private File baseDir;

	private org.apache.hadoop.fs.FileSystem hdfs;
	private String hdfsURI;
	private MiniDFSCluster hdfsCluster;

	private static Map<Integer, String> expectedContents = new HashMap<>();

	//						PREPARING FOR THE TESTS

	@Before
	public void createHDFS() {
		try {
			baseDir = new File("./target/hdfs/hdfsTesting").getAbsoluteFile();
			FileUtil.fullyDelete(baseDir);

			org.apache.hadoop.conf.Configuration hdConf = new org.apache.hadoop.conf.Configuration();
			hdConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());
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

	@After
	public void destroyHDFS() {
		try {
			FileUtil.fullyDelete(baseDir);
			hdfsCluster.shutdown();
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}

	//						END OF PREPARATIONS

	@Test
	public void testProgram() throws Exception {

		/*
		* This test checks the interplay between the monitor and the reader
		* and also the failExternally() functionality. To test the latter we
		* set the parallelism to 1 so that we have the chaining between the sink,
		* which throws the SuccessException to signal the end of the test, and the
		* reader.
		* */

		TextInputFormat format = new TextInputFormat(new Path(hdfsURI));
		format.setFilePath(hdfsURI);
		format.setFilesFilter(FilePathFilter.createDefaultFilter());

		// create the stream execution environment with a parallelism > 1 to test
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(PARALLELISM);

		ContinuousFileMonitoringFunction<String> monitoringFunction =
			new ContinuousFileMonitoringFunction<>(format,
				FileProcessingMode.PROCESS_CONTINUOUSLY,
				env.getParallelism(), INTERVAL);

		// the monitor has always DOP 1
		DataStream<TimestampedFileInputSplit> splits = env.addSource(monitoringFunction);
		Assert.assertEquals(1, splits.getParallelism());

		ContinuousFileReaderOperator<String> reader = new ContinuousFileReaderOperator<>(format);
		TypeInformation<String> typeInfo = TypeExtractor.getInputFormatTypes(format);

		// the readers can be multiple
		DataStream<String> content = splits.transform("FileSplitReader", typeInfo, reader);
		Assert.assertEquals(PARALLELISM, content.getParallelism());

		// finally for the sink we set the parallelism to 1 so that we can verify the output
		TestingSinkFunction sink = new TestingSinkFunction();
		content.addSink(sink).setParallelism(1);

		Thread job = new Thread() {

			@Override
			public void run() {
				try {
					env.execute("ContinuousFileProcessingITCase Job.");
				} catch (Exception e) {
					Throwable th = e;
					for (int depth = 0; depth < 20; depth++) {
						if (th instanceof SuccessException) {
							return;
						} else if (th.getCause() != null) {
							th = th.getCause();
						} else {
							break;
						}
					}
					e.printStackTrace();
					Assert.fail(e.getMessage());
				}
			}
		};
		job.start();

		// The modification time of the last created file.
		long lastCreatedModTime = Long.MIN_VALUE;

		// create the files to be read
		for (int i = 0; i < NO_OF_FILES; i++) {
			Tuple2<org.apache.hadoop.fs.Path, String> tmpFile;
			long modTime;
			do {

				// give it some time so that the files have
				// different modification timestamps.
				Thread.sleep(50);

				tmpFile = fillWithData(hdfsURI, "file", i, "This is test line.");

				modTime = hdfs.getFileStatus(tmpFile.f0).getModificationTime();
				if (modTime <= lastCreatedModTime) {
					// delete the last created file to recreate it with a different timestamp
					hdfs.delete(tmpFile.f0, false);
				}
			} while (modTime <= lastCreatedModTime);
			lastCreatedModTime = modTime;

			// put the contents in the expected results list before the reader picks them
			// this is to guarantee that they are in before the reader finishes (avoid race conditions)
			expectedContents.put(i, tmpFile.f1);

			org.apache.hadoop.fs.Path file =
				new org.apache.hadoop.fs.Path(hdfsURI + "/file" + i);
			hdfs.rename(tmpFile.f0, file);
			Assert.assertTrue(hdfs.exists(file));
		}

		// wait for the job to finish.
		job.join();
	}

	private static class TestingSinkFunction extends RichSinkFunction<String> {

		private int elementCounter = 0;
		private Map<Integer, Set<String>> actualContent = new HashMap<>();

		private transient Comparator<String> comparator;

		@Override
		public void open(Configuration parameters) throws Exception {
			// this sink can only work with DOP 1
			assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());

			comparator = new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					return getLineNo(o1) - getLineNo(o2);
				}
			};
		}

		@Override
		public void invoke(String value) throws Exception {
			int fileIdx = getFileIdx(value);

			Set<String> content = actualContent.get(fileIdx);
			if (content == null) {
				content = new HashSet<>();
				actualContent.put(fileIdx, content);
			}

			if (!content.add(value + "\n")) {
				Assert.fail("Duplicate line: " + value);
				System.exit(0);
			}

			elementCounter++;
			if (elementCounter == NO_OF_FILES * LINES_PER_FILE) {
				throw new SuccessException();
			}
		}

		@Override
		public void close() {
			// check if the data that we collected are the ones they are supposed to be.
			Assert.assertEquals(expectedContents.size(), actualContent.size());
			for (Integer fileIdx: expectedContents.keySet()) {
				Assert.assertTrue(actualContent.keySet().contains(fileIdx));

				List<String> cntnt = new ArrayList<>(actualContent.get(fileIdx));
				Collections.sort(cntnt, comparator);

				StringBuilder cntntStr = new StringBuilder();
				for (String line: cntnt) {
					cntntStr.append(line);
				}
				Assert.assertEquals(expectedContents.get(fileIdx), cntntStr.toString());
			}
			expectedContents.clear();
		}

		private int getLineNo(String line) {
			String[] tkns = line.split("\\s");
			return Integer.parseInt(tkns[tkns.length - 1]);
		}

		private int getFileIdx(String line) {
			String[] tkns = line.split(":");
			return Integer.parseInt(tkns[0]);
		}
	}

	/** Create a file and fill it with content. */
	private Tuple2<org.apache.hadoop.fs.Path, String> fillWithData(
		String base, String fileName, int fileIdx, String sampleLine) throws IOException, InterruptedException {

		assert (hdfs != null);

		org.apache.hadoop.fs.Path tmp =
			new org.apache.hadoop.fs.Path(base + "/." + fileName + fileIdx);

		FSDataOutputStream stream = hdfs.create(tmp);
		StringBuilder str = new StringBuilder();
		for (int i = 0; i < LINES_PER_FILE; i++) {
			String line = fileIdx + ": " + sampleLine + " " + i + "\n";
			str.append(line);
			stream.write(line.getBytes(ConfigConstants.DEFAULT_CHARSET));
		}
		stream.close();
		return new Tuple2<>(tmp, str.toString());
	}

	private static class SuccessException extends Exception {
		private static final long serialVersionUID = -7011865671593955887L;
	}
}
