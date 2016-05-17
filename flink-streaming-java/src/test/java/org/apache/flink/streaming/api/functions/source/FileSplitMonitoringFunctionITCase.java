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
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FileSplitMonitoringFunctionITCase extends StreamingProgramTestBase {

	private static final int NO_OF_FILES = 10;
	private static final int LINES_PER_FILE = 10;

	private static final long INTERVAL = 100;

	private File baseDir;

	private org.apache.hadoop.fs.FileSystem hdfs;
	private String hdfsURI;
	private MiniDFSCluster hdfsCluster;

	private static Map<Integer, String> hdPathContents = new HashMap<>();

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

			hdfsURI = "hdfs://" + hdfsCluster.getURI().getHost() + ":" + hdfsCluster.getNameNodePort() +"/";
			hdfs = new org.apache.hadoop.fs.Path(hdfsURI).getFileSystem(hdConf);

		} catch(Throwable e) {
			e.printStackTrace();
			Assert.fail("Test failed " + e.getMessage());
		}
	}

	@After
	public void destroyHDFS() {
		FileUtil.fullyDelete(baseDir);
		hdfsCluster.shutdown();
	}

	//						END OF PREPARATIONS

	@Override
	protected void testProgram() throws Exception {
		FileCreator fileCreator = new FileCreator(INTERVAL);
		Thread t = new Thread(fileCreator);
		t.start();

		TextInputFormat format = new TextInputFormat(new Path(hdfsURI));
		format.setFilePath(hdfsURI);

		Configuration config = new Configuration();
		config.setString("input.file.path", hdfsURI);

		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

			FileSplitMonitoringFunction<String> monitoringFunction =
				new FileSplitMonitoringFunction<>(format, hdfsURI,
				config, FileSplitMonitoringFunction.WatchType.REPROCESS_WITH_APPENDED,
				env.getParallelism(), INTERVAL);

			TypeInformation<String> typeInfo = TypeExtractor.getInputFormatTypes(format);
			FileSplitReadOperator<String> reader = new FileSplitReadOperator<>(format, typeInfo, config);
			TestingSinkFunction sink = new TestingSinkFunction(monitoringFunction);

			DataStream<FileInputSplit> splits = env.addSource(monitoringFunction);
			splits.transform("FileSplitReader", typeInfo, reader).addSink(sink).setParallelism(1);
			env.execute();

		} catch (Exception e) {
			if (!(e.getCause() instanceof SuccessException)) {
				e.printStackTrace();
				fail();
			}
		}
	}

	private static class TestingSinkFunction extends RichSinkFunction<String> {

		private final FileSplitMonitoringFunction src;

		private int elementCounter = 0;
		private Map<Integer, Integer> elementCounters = new HashMap<>();
		private Map<Integer, String> collectedContent = new HashMap<>();

		TestingSinkFunction(FileSplitMonitoringFunction monitoringFunction) {
			this.src = monitoringFunction;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			// this sink can only work with DOP 1
			assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());
		}

		@Override
		public void close() {

			// check if the data that we collected are the ones they are supposed to be.
			Assert.assertEquals(collectedContent.size(), hdPathContents.size());
			for (Integer fileIdx: collectedContent.keySet()) {
				Assert.assertEquals(collectedContent.get(fileIdx), hdPathContents.get(fileIdx));
			}
			hdPathContents.clear();

			src.cancel();
			try {
				src.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void invoke(String value) throws Exception {
			int fileIdx = Character.getNumericValue(value.charAt(0));

			Integer counter = elementCounters.get(fileIdx);
			if (counter == null) {
				counter = 0;
			} else if (counter == LINES_PER_FILE) {
				// ignore duplicate lines.
				Assert.fail("Duplicate line: " + value);
			}
			elementCounters.put(fileIdx, ++counter);

			String content = collectedContent.get(fileIdx);
			if (content == null) {
				content = "";
			}
			collectedContent.put(fileIdx, content + value + "\n");

			elementCounter++;
			if (elementCounter == NO_OF_FILES * LINES_PER_FILE) {
				throw new SuccessException();
			}
		}
	}

	/**
	 * A separate thread creating {@link #NO_OF_FILES} files, one file every {@link #INTERVAL} milliseconds.
	 * It serves for testing the file monitoring functionality of the {@link FileSplitMonitoringFunction}.
	 * The files are filled with data by the {@link #fillWithData(String, String, int, String)} method.
	 * */
	private class FileCreator implements Runnable {

		private final long interval;

		FileCreator(long interval) {
			this.interval = interval;
		}

		public void run() {
			try {
				for (int i = 0; i < NO_OF_FILES; i++) {
					fillWithData(hdfsURI, "file", i, "This is test line.");
					Thread.sleep(interval);
				}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				// we just close without any message.
			}
		}
	}

	/**
	 * Fill the file with content.
	 * */
	private void fillWithData(String base, String fileName, int fileIdx, String sampleLine) throws IOException {
		assert (hdfs != null);

		org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(base + "/" + fileName + fileIdx);

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

		hdPathContents.put(fileIdx, str.toString());

		Assert.assertTrue("No result file present", hdfs.exists(file));
	}

	public static class SuccessException extends Exception {
		private static final long serialVersionUID = -7011865671593955887L;
	}
}
