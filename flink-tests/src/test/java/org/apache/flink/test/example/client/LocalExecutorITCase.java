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
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.client.deployment.executors.LocalExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.testfunctions.Tokenizer;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for {@link LocalExecutor}.
 */
public class LocalExecutorITCase extends TestLogger {

	private static final int parallelism = 4;

	private MiniCluster miniCluster;
	private LocalExecutor executor;

	@Before
	public void before() {
		executor = LocalExecutor.createWithFactory(new Configuration(), config -> {
			miniCluster = new MiniCluster(config);
			return miniCluster;
		});
	}

	@Test(timeout = 60_000)
	public void testLocalExecutorWithWordCount() throws InterruptedException {
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

			Plan wcPlan = getWordCountPlan(inFile, outFile, parallelism);
			wcPlan.setExecutionConfig(new ExecutionConfig());
			JobClient jobClient = executor.execute(wcPlan, config).get();
			jobClient.getJobExecutionResult(getClass().getClassLoader()).get();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}

		assertThat(miniCluster.isRunning(), is(false));
	}

	@Test(timeout = 60_000)
	public void testMiniClusterShutdownOnErrors() throws Exception {
		Plan runtimeExceptionPlan = getRuntimeExceptionPlan();
		runtimeExceptionPlan.setExecutionConfig(new ExecutionConfig());

		Configuration config = new Configuration();
		config.setBoolean(DeploymentOptions.ATTACHED, true);

		JobClient jobClient = executor.execute(runtimeExceptionPlan, config).get();

		assertThrows(
			"Job execution failed.",
			Exception.class,
			() -> jobClient.getJobExecutionResult(getClass().getClassLoader()).get());

		assertThat(miniCluster.isRunning(), is(false));
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

	private Plan getRuntimeExceptionPlan() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.fromElements(1)
			.map(element -> {
				if (element == 1) {
					throw new RuntimeException("oups");
				}
				return element;
			})
			.output(new DiscardingOutputFormat<>());
		return env.createProgramPlan();
	}
}
