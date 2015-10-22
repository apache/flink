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

package org.apache.flink.examples.java.distcp;

import org.apache.commons.io.IOUtils;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A main class of the Flink distcp utility.
 * It's a simple reimplementation of Hadoop distcp
 * (see <a href="http://hadoop.apache.org/docs/r1.2.1/distcp.html">http://hadoop.apache.org/docs/r1.2.1/distcp.html</a>)
 * with a dynamic input format
 * Note that this tool does not deal with retriability. Additionally, empty directories are not copied over.
 * <p>
 * When running locally, local file systems paths can be used.
 * However, in a distributed environment HDFS paths must be provided both as input and output.
 */
public class DistCp {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DistCp.class);
	public static final String BYTES_COPIED_CNT_NAME = "BYTES_COPIED";
	public static final String FILES_COPIED_CNT_NAME = "FILES_COPIED";

	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			printHelp();
			return;
		}

		final Path sourcePath = new Path(args[0]);
		final Path targetPath = new Path(args[1]);
		int parallelism = Integer.valueOf(args[2], 10);

		// set up the execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		checkInputParams(env, sourcePath, targetPath, parallelism);
		env.setParallelism(parallelism);

		long startTime = System.currentTimeMillis();
		LOGGER.info("Initializing copy tasks");
		List<FileCopyTask> tasks = getCopyTasks(sourcePath);
		LOGGER.info("Copy task initialization took " + (System.currentTimeMillis() - startTime) + "ms");

		DataSet<FileCopyTask> inputTasks = new DataSource<>(env,
				new FileCopyTaskInputFormat(tasks),
				new GenericTypeInfo<>(FileCopyTask.class), "fileCopyTasks");


		FlatMapOperator<FileCopyTask, Object> res = inputTasks.flatMap(new RichFlatMapFunction<FileCopyTask, Object>() {
			
			private LongCounter fileCounter;
			private LongCounter bytesCounter;

			@Override
			public void open(Configuration parameters) throws Exception {
				bytesCounter = getRuntimeContext().getLongCounter(BYTES_COPIED_CNT_NAME);
				fileCounter = getRuntimeContext().getLongCounter(FILES_COPIED_CNT_NAME);
			}

			@Override
			public void flatMap(FileCopyTask task, Collector<Object> out) throws Exception {
				LOGGER.info("Processing task: " + task);
				Path outPath = new Path(targetPath, task.getRelativePath());

				FileSystem targetFs = targetPath.getFileSystem();
				// creating parent folders in case of a local FS
				if (!targetFs.isDistributedFS()) {
					//dealing with cases like file:///tmp or just /tmp
					File outFile = outPath.toUri().isAbsolute() ? new File(outPath.toUri()) : new File(outPath.toString());
					File parentFile = outFile.getParentFile();
					if (!parentFile.mkdirs() && !parentFile.exists()) {
						throw new RuntimeException("Cannot create local file system directories: " + parentFile);
					}
				}
				FSDataOutputStream outputStream = null;
				FSDataInputStream inputStream = null;
				try {
					outputStream = targetFs.create(outPath, true);
					inputStream = task.getPath().getFileSystem().open(task.getPath());
					int bytes = IOUtils.copy(inputStream, outputStream);
					bytesCounter.add(bytes);
				} finally {
					IOUtils.closeQuietly(inputStream);
					IOUtils.closeQuietly(outputStream);
				}
				fileCounter.add(1l);
			}
		});

		// no data sinks are needed, therefore just printing an empty result
		res.print();

		Map<String, Object> accumulators = env.getLastJobExecutionResult().getAllAccumulatorResults();
		LOGGER.info("== COUNTERS ==");
		for (Map.Entry<String, Object> e : accumulators.entrySet()) {
			LOGGER.info(e.getKey() + ": " + e.getValue());
		}
	}


	// -----------------------------------------------------------------------------------------
	// HELPER METHODS
	// -----------------------------------------------------------------------------------------

	private static void checkInputParams(ExecutionEnvironment env, Path sourcePath, Path targetPath, int parallelism) throws IOException {
		if (parallelism <= 0) {
			throw new IllegalArgumentException("Parallelism should be greater than 0");
		}

		boolean isLocal = env instanceof LocalEnvironment;
		if (!isLocal &&
				!(sourcePath.getFileSystem().isDistributedFS() && targetPath.getFileSystem().isDistributedFS())) {
			throw new IllegalArgumentException("In a distributed mode only HDFS input/output paths are supported");
		}
	}

	private static void printHelp() {
		System.err.println("Usage: <input_path> <output_path> <level_of_parallelism>");
	}

	private static List<FileCopyTask> getCopyTasks(Path sourcePath) throws IOException {
		List<FileCopyTask> tasks = new ArrayList<>();
		getCopyTasks(sourcePath, "", tasks);
		return tasks;
	}

	private static void getCopyTasks(Path p, String rel, List<FileCopyTask> tasks) throws IOException {
		FileStatus[] res = p.getFileSystem().listStatus(p);
		if (res == null) {
			return;
		}
		for (FileStatus fs : res) {
			if (fs.isDir()) {
				getCopyTasks(fs.getPath(), rel + fs.getPath().getName() + "/", tasks);
			} else {
				Path cp = fs.getPath();
				tasks.add(new FileCopyTask(cp, rel + cp.getName()));
			}
		}
	}

}
