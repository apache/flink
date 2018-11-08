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

package org.apache.flink.api.tests;

import org.apache.flink.tests.util.FlinkDistribution;
import org.apache.flink.util.OperatingSystem;

import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * End-to-end test for basic batch-job executions.
 */
public class BatchWordCountITCase {

	@BeforeClass
	public static void checkOS() {
		Assume.assumeFalse("This test does not run on Windows.", OperatingSystem.isWindows());
	}

	@Rule
	public final FlinkDistribution dist = new FlinkDistribution();

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@Test
	public void testBatchWordcount() throws IOException {
		dist.startFlinkCluster();

		final Path tmpInputFile = tmp.newFolder("input").toPath().resolve("words");
		final Path tmpOutputFile = tmp.newFolder("output").toPath().resolve("wc_out");

		Files.write(
			tmpInputFile,
			"Hello World how are you, my dear dear world".getBytes(StandardCharsets.UTF_8));

		dist.prepareExampleJob(Paths.get("batch/WordCount.jar"))
			.setParallelism(1)
			.addArgument("--input", tmpInputFile.toAbsolutePath().toString())
			.addArgument("--output", tmpOutputFile.toAbsolutePath().toString())
			.run();

		final List<String> output = Files.readAllLines(tmpOutputFile);

		Assert.assertEquals(1139286315, output.hashCode());
	}
}
