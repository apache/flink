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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.ContinuousFileMonitoringFunction;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ContinuousFileProcessingCheckpointITCase extends StreamFaultToleranceTestBase {

	private static final int NO_OF_FILES = 9;
	private static final int LINES_PER_FILE = 200;
	private static final int NO_OF_RETRIES = 3;
	private static final int PARALLELISM = 4;
	private static final long INTERVAL = 2000;

	private static File baseDir;
	private static org.apache.hadoop.fs.FileSystem fs;
	private static String localFsURI;
	private FileCreator fc;

	private static  Map<Integer, List<String>> finalCollectedContent = new HashMap<>();

	@BeforeClass
	public static void createHDFS() {
		try {
			baseDir = new File("./target/localfs/fs_tests").getAbsoluteFile();
			FileUtil.fullyDelete(baseDir);

			org.apache.hadoop.conf.Configuration hdConf = new org.apache.hadoop.conf.Configuration();

			localFsURI = "file:///" + baseDir +"/";
			fs = new org.apache.hadoop.fs.Path(localFsURI).getFileSystem(hdConf);

		} catch(Throwable e) {
			e.printStackTrace();
			Assert.fail("Test failed " + e.getMessage());
		}
	}

	@AfterClass
	public static void destroyHDFS() {
		try {
			FileUtil.fullyDelete(baseDir);
		} catch (Throwable t) {
			throw new RuntimeException(t);
		}
	}

	@Override
	public void testProgram(StreamExecutionEnvironment env) {

		// set the restart strategy.
		env.getConfig().setRestartStrategy(
			RestartStrategies.fixedDelayRestart(NO_OF_RETRIES, 0));
		env.enableCheckpointing(20);
		env.setParallelism(PARALLELISM);

		// create and start the file creating thread.
		fc = new FileCreator();
		fc.start();

		// create the monitoring source along with the necessary readers.
		TestingSinkFunction sink = new TestingSinkFunction();
		TextInputFormat format = new TextInputFormat(new org.apache.flink.core.fs.Path(localFsURI));
		format.setFilesFilter(FilePathFilter.createDefaultFilter());
		DataStream<String> inputStream = env.readFile(format, localFsURI,
			FileProcessingMode.PROCESS_CONTINUOUSLY, INTERVAL);

		inputStream.flatMap(new FlatMapFunction<String, String>() {
			@Override
			public void flatMap(String value, Collector<String> out) throws Exception {
				out.collect(value);
			}
		}).addSink(sink).setParallelism(1);
	}

	@Override
	public void postSubmit() throws Exception {
		Map<Integer, List<String>> collected = finalCollectedContent;
		Assert.assertEquals(collected.size(), fc.getFileContent().size());
		for (Integer fileIdx: fc.getFileContent().keySet()) {
			Assert.assertTrue(collected.keySet().contains(fileIdx));

			List<String> cntnt = collected.get(fileIdx);
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
			Assert.assertEquals(fc.getFileContent().get(fileIdx), cntntStr.toString());
		}

		collected.clear();
		finalCollectedContent.clear();
		fc.clean();
	}

	private int getLineNo(String line) {
		String[] tkns = line.split("\\s");
		Assert.assertTrue(tkns.length == 6);
		return Integer.parseInt(tkns[tkns.length - 1]);
	}

	// --------------------------------------------------------------------------------------------
	//  Custom Functions
	// --------------------------------------------------------------------------------------------

	// -------------------------			FILE CREATION			-------------------------------

	/**
	 * A separate thread creating {@link #NO_OF_FILES} files, one file every {@link #INTERVAL} milliseconds.
	 * It serves for testing the file monitoring functionality of the {@link ContinuousFileMonitoringFunction}.
	 * The files are filled with data by the {@link #fillWithData(String, String, int, String)} method.
	 * */
	private class FileCreator extends Thread {

		private final Set<Path> filesCreated = new HashSet<>();
		private final Map<Integer, String> fileContents = new HashMap<>();

		public void run() {
			try {
				for(int i = 0; i < NO_OF_FILES; i++) {
					Tuple2<org.apache.hadoop.fs.Path, String> file =
						fillWithData(localFsURI, "file", i, "This is test line.");
					filesCreated.add(file.f0);
					fileContents.put(i, file.f1);

					Thread.sleep((int) (INTERVAL / (3.0/2)));
				}
			} catch (IOException | InterruptedException e) {
				e.printStackTrace();
			}
		}

		void clean() throws IOException {
			assert (fs != null);
			for (org.apache.hadoop.fs.Path path: filesCreated) {
				fs.delete(path, false);
			}
			fileContents.clear();
		}

		Map<Integer, String> getFileContent() {
			return this.fileContents;
		}
	}

	/**
	 * Fill the file with content and put the content in the {@code hdPathContents} list.
	 * */
	private Tuple2<Path, String> fillWithData(
		String base, String fileName, int fileIdx, String sampleLine) throws IOException {

		assert (fs != null);

		org.apache.hadoop.fs.Path file = new org.apache.hadoop.fs.Path(base + "/" + fileName + fileIdx);

		org.apache.hadoop.fs.Path tmp = new org.apache.hadoop.fs.Path(base + "/." + fileName + fileIdx);
		FSDataOutputStream stream = fs.create(tmp);
		StringBuilder str = new StringBuilder();
		for(int i = 0; i < LINES_PER_FILE; i++) {
			String line = fileIdx +": "+ sampleLine + " " + i +"\n";
			str.append(line);
			stream.write(line.getBytes());
		}
		stream.close();

		Assert.assertTrue("Result file present", !fs.exists(file));
		fs.rename(tmp, file);
		Assert.assertTrue("No result file present", fs.exists(file));
		return new Tuple2<>(file, str.toString());
	}

	// --------------------------			Task Sink			------------------------------

	private static class TestingSinkFunction extends RichSinkFunction<String>
		implements Checkpointed<Tuple2<Long, Map<Integer, Set<String>>>>, CheckpointListener {

		private static volatile boolean hasFailed = false;

		private volatile int numSuccessfulCheckpoints;

		private long count;

		private long elementsToFailure;

		private long elementCounter = 0;

		private  Map<Integer, Set<String>> collectedContent = new HashMap<>();

		TestingSinkFunction() {
			hasFailed = false;
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			// this sink can only work with DOP 1
			assertEquals(1, getRuntimeContext().getNumberOfParallelSubtasks());

			long failurePosMin = (long) (0.4 * LINES_PER_FILE);
			long failurePosMax = (long) (0.7 * LINES_PER_FILE);

			elementsToFailure = (new Random().nextLong() % (failurePosMax - failurePosMin)) + failurePosMin;

			if (elementCounter >= NO_OF_FILES * LINES_PER_FILE) {
				finalCollectedContent = new HashMap<>();
				for (Map.Entry<Integer, Set<String>> result: collectedContent.entrySet()) {
					finalCollectedContent.put(result.getKey(), new ArrayList<>(result.getValue()));
				}
				throw new SuccessException();
			}
		}

		@Override
		public void close() {
			try {
				super.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		@Override
		public void invoke(String value) throws Exception {
			int fileIdx = Character.getNumericValue(value.charAt(0));

			Set<String> content = collectedContent.get(fileIdx);
			if (content == null) {
				content = new HashSet<>();
				collectedContent.put(fileIdx, content);
			}

			if (!content.add(value + "\n")) {
				fail("Duplicate line: " + value);
				System.exit(0);
			}


			elementCounter++;
			if (elementCounter >= NO_OF_FILES * LINES_PER_FILE) {
				finalCollectedContent = new HashMap<>();
				for (Map.Entry<Integer, Set<String>> result: collectedContent.entrySet()) {
					finalCollectedContent.put(result.getKey(), new ArrayList<>(result.getValue()));
				}
				throw new SuccessException();
			}

			count++;
			if (!hasFailed) {
				Thread.sleep(2);
				if (numSuccessfulCheckpoints >= 1 && count >= elementsToFailure) {
					hasFailed = true;
					throw new Exception("Task Failure");
				}
			}
		}

		@Override
		public Tuple2<Long, Map<Integer, Set<String>>> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return new Tuple2<>(elementCounter, collectedContent);
		}

		@Override
		public void restoreState(Tuple2<Long, Map<Integer, Set<String>>> state) throws Exception {
			this.elementCounter = state.f0;
			this.collectedContent = state.f1;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			numSuccessfulCheckpoints++;
		}
	}
}
