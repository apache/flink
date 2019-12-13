/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.test.example.client;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.deployment.executors.LocalExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.testfunctions.Tokenizer;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;

/**
 * Integration tests for {@link LocalExecutor}.
 */
public class LocalExecutorITCase extends TestLogger {

	private static final int parallelism = 4;

	@Test
	public void testLocalExecutorWithWordCount() {
		try {
			// set up the files
			File inFile = File.createTempFile("wctext", ".in");
			File outFile = File.createTempFile("wctext", ".out");
			inFile.deleteOnExit();
			outFile.deleteOnExit();

			try (FileWriter fw = new FileWriter(inFile)) {
				fw.write(WordCountData.TEXT);
			}

			final Configuration config = new Configuration();
			config.setBoolean(CoreOptions.FILESYTEM_DEFAULT_OVERRIDE, true);
			config.setBoolean(DeploymentOptions.ATTACHED, true);

			final LocalExecutor executor = new LocalExecutor();

			Plan wcPlan = getWordCountPlan(inFile, outFile, parallelism);
			wcPlan.setExecutionConfig(new ExecutionConfig());
			executor.execute(wcPlan, config);
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	private Plan getWordCountPlan(File inFile, File outFile, int parallelism) {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.readTextFile(inFile.getAbsolutePath())
			.flatMap(new Tokenizer())
			.groupBy(0)
			.sum(1)
			.writeAsCsv(outFile.getAbsolutePath());
		return env.createProgramPlan();
	}
}
