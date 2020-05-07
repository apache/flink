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

package org.apache.flink.dist;

import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.util.bash.BashJavaUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for BashJavaUtils.
 *
 * <p>This test requires the distribution to be assembled and is hence marked as an IT case which run after packaging.
 */
public class BashJavaUtilsITCase extends JavaBashTestBase {

	private static final String RUN_BASH_JAVA_UTILS_CMD_SCRIPT = "src/test/bin/runBashJavaUtilsCmd.sh";
	private static final String RUN_EXTRACT_LOGGING_OUTPUTS_SCRIPT = "src/test/bin/runExtractLoggingOutputs.sh";

	@Test
	public void testGetTmResourceParamsConfigs() throws Exception {
		int expectedResultLines = 2;
		String[] commands = {RUN_BASH_JAVA_UTILS_CMD_SCRIPT, BashJavaUtils.Command.GET_TM_RESOURCE_PARAMS.toString(), String.valueOf(expectedResultLines)};
		List<String> lines = Arrays.asList(executeScript(commands).split(System.lineSeparator()));

		assertThat(lines.size(), is(expectedResultLines));
		ConfigurationUtils.parseJvmArgString(lines.get(0));
		ConfigurationUtils.parseTmResourceDynamicConfigs(lines.get(1));
	}

	@Test
	public void testGetTmResourceParamsConfigsWithDynamicProperties() throws Exception {
		int expectedResultLines = 2;
		double cpuCores = 39.0;
		String[] commands = {
			RUN_BASH_JAVA_UTILS_CMD_SCRIPT,
			BashJavaUtils.Command.GET_TM_RESOURCE_PARAMS.toString(),
			String.valueOf(expectedResultLines),
			"-D" + TaskManagerOptions.CPU_CORES.key() + "=" + cpuCores};
		List<String> lines = Arrays.asList(executeScript(commands).split(System.lineSeparator()));

		assertThat(lines.size(), is(expectedResultLines));
		Map<String, String> configs = ConfigurationUtils.parseTmResourceDynamicConfigs(lines.get(1));
		assertThat(Double.valueOf(configs.get(TaskManagerOptions.CPU_CORES.key())), is(cpuCores));
	}

	@Test
	public void testGetJmResourceParams() throws Exception {
		int expectedResultLines = 1;
		String[] commands = {
			RUN_BASH_JAVA_UTILS_CMD_SCRIPT,
			BashJavaUtils.Command.GET_JM_RESOURCE_PARAMS.toString(),
			String.valueOf(expectedResultLines)};
		List<String> lines = Arrays.asList(executeScript(commands).split(System.lineSeparator()));

		assertThat(lines.size(), is(expectedResultLines));
		ConfigurationUtils.parseJvmArgString(lines.get(0));
	}

	@Test
	public void testGetJmResourceParamsWithDynamicProperties() throws Exception {
		int expectedResultLines = 1;
		long metaspace = 123456789L;
		String[] commands = {
			RUN_BASH_JAVA_UTILS_CMD_SCRIPT,
			BashJavaUtils.Command.GET_JM_RESOURCE_PARAMS.toString(),
			String.valueOf(expectedResultLines),
			"-D" + JobManagerOptions.JVM_METASPACE.key() + "=" + metaspace + "b"};
		List<String> lines = Arrays.asList(executeScript(commands).split(System.lineSeparator()));

		assertThat(lines.size(), is(expectedResultLines));
		Map<String, String> params = ConfigurationUtils.parseJvmArgString(lines.get(0));
		assertThat(Long.valueOf(params.get("-XX:MaxMetaspaceSize=")), is(metaspace));
	}

	@Test
	public void testExtractLoggingOutputs() throws Exception {
		StringBuilder input = new StringBuilder();
		List<String> expectedOutput = new ArrayList<>();

		for (int i = 0; i < 5; ++i) {
			String line = "BashJavaUtils output line " + i + " `~!@#$%^&*()-_=+;:,.'\"\\\t/?";
			if (i % 2 == 0) {
				expectedOutput.add(line);
			} else {
				line = BashJavaUtils.EXECUTION_PREFIX + line;
			}
			input.append(line + "\n");
		}

		String[] commands = {RUN_EXTRACT_LOGGING_OUTPUTS_SCRIPT, input.toString()};
		List<String> actualOutput = Arrays.asList(executeScript(commands).split(System.lineSeparator()));

		assertThat(actualOutput, is(expectedOutput));
	}
}
