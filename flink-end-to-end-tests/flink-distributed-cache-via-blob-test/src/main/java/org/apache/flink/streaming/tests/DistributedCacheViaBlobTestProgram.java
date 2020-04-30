/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * End-to-end test program for verifying that files are distributed via BlobServer and later accessible through
 * DistribitutedCache. We verify that via uploading file and later on accessing it in map function. To be sure we read
 * version read from cache, we delete the initial file.
 */
public class DistributedCacheViaBlobTestProgram {

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		final Path inputFile = Paths.get(params.getRequired("inputFile"));
		final Path inputDir = Paths.get(params.getRequired("inputDir"));

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		env.registerCachedFile(inputFile.toString(), "test_data", false);
		env.registerCachedFile(inputDir.toString(), "test_dir", false);

		final Path containedFile;
		try (Stream<Path> files = Files.list(inputDir)) {
			containedFile = files.findAny().orElseThrow(() -> new RuntimeException("Input directory must not be empty."));
		}

		env.fromElements(1)
			.map(new TestMapFunction(
				inputFile.toAbsolutePath().toString(),
				Files.size(inputFile),
				inputDir.toAbsolutePath().toString(),
				containedFile.getFileName().toString()))
			.writeAsText(params.getRequired("output"), FileSystem.WriteMode.OVERWRITE);

		env.execute("Distributed Cache Via Blob Test Program");
	}

	static class TestMapFunction extends RichMapFunction<Integer, String> {

		private final String initialPath;
		private final long fileSize;

		private final String initialDirPath;
		private final String containedFileName;

		public TestMapFunction(String initialPath, long fileSize, String initialDirPath, String containedFileName) {
			this.initialPath = initialPath;
			this.fileSize = fileSize;
			this.initialDirPath = initialDirPath;
			this.containedFileName = containedFileName;
		}

		@Override
		public String map(Integer value) throws Exception {
			final Path testFile = getRuntimeContext().getDistributedCache().getFile("test_data").toPath();
			final Path testDir = getRuntimeContext().getDistributedCache().getFile("test_dir").toPath();

			if (testFile.toAbsolutePath().toString().equals(initialPath)) {
				throw new RuntimeException(String.format("Operator should access copy from cache rather than the " +
					"initial file. Input file path: %s. Cache file path: %s", initialPath, testFile));
			}

			long testFileSize = Files.size(testFile);
			if (testFileSize != fileSize) {
				throw new RuntimeException(String.format("File size does not match. Expected:%s Actual:%s", fileSize, testFileSize));
			}

			if (testDir.toAbsolutePath().toString().equals(initialDirPath)) {
				throw new RuntimeException(String.format("Operator should access copy from cache rather than the " +
					"initial dir. Input dir path: %s. Cache dir path: %s", initialDirPath, testDir));
			}

			try (Stream<Path> files = Files.list(testDir)) {
				if (files.map(Path::getFileName).map(Path::toString).noneMatch(path -> path.equals(containedFileName))) {
					throw new RuntimeException(String.format("Cached directory %s should not be empty.", testDir));
				}
			}

			return Files.readAllLines(testFile)
				.stream()
				.collect(Collectors.joining("\n"));
		}
	}
}
