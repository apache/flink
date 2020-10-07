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

package org.apache.flink.client.cli;

import org.apache.flink.client.deployment.executors.LocalExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

/**
 * Integration tests for {@link CliFrontend}.
 */
public class CliFrontendITCase {

	private PrintStream originalPrintStream;

	private ByteArrayOutputStream testOutputStream;

	@Before
	public void before() {
		originalPrintStream = System.out;
		testOutputStream = new ByteArrayOutputStream();
		System.setOut(new PrintStream(testOutputStream));
	}

	@After
	public void finalize() {
		System.setOut(originalPrintStream);
	}

	private String getStdoutString() {
		return testOutputStream.toString();
	}

	@Test
	public void configurationIsForwarded() throws Exception {
		Configuration config = new Configuration();
		CustomCommandLine commandLine = new DefaultCLI();

		config.set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ofMillis(42L));

		CliFrontend cliFrontend = new CliFrontend(config, Collections.singletonList(commandLine));

		cliFrontend.parseAndRun(new String[]{"run", "-c", TestingJob.class.getName(), CliFrontendTestUtils.getTestJarPath()});

		assertThat(getStdoutString(), containsString("Watermark interval is 42"));
	}

	@Test
	public void commandlineOverridesConfiguration() throws Exception {
		Configuration config = new Configuration();

		// we use GenericCli because it allows specifying arbitrary options via "-Dfoo=bar" syntax
		CustomCommandLine commandLine = new GenericCLI(config, "/dev/null");

		config.set(PipelineOptions.AUTO_WATERMARK_INTERVAL, Duration.ofMillis(42L));

		CliFrontend cliFrontend = new CliFrontend(config, Collections.singletonList(commandLine));

		cliFrontend.parseAndRun(new String[]{
				"run",
				"-t", LocalExecutor.NAME,
				"-c", TestingJob.class.getName(),
				"-D" + PipelineOptions.AUTO_WATERMARK_INTERVAL.key() + "=142",
				CliFrontendTestUtils.getTestJarPath()});

		assertThat(getStdoutString(), containsString("Watermark interval is 142"));
	}

	/**
	 * Testing job that the watermark interval from the {@link org.apache.flink.api.common.ExecutionConfig}.
	 */
	public static class TestingJob {
		public static void main(String[] args) {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			System.out.println(
					"Watermark interval is " + env.getConfig().getAutoWatermarkInterval());
		}
	}
}
