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

package org.apache.flink.client.cli;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.DeploymentOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Unit tests for the {@link ExecutorCLI}.
 */
public class ExecutorCLITest {

	private Options testOptions;

	@Before
	public void initOptions() {
		testOptions = new Options();

		final ExecutorCLI cliUnderTest = new ExecutorCLI(new Configuration());
		cliUnderTest.addGeneralOptions(testOptions);
	}

	@Test(expected = IllegalStateException.class)
	public void testExceptionWithUnspecifiedExecutor() throws CliArgsException {
		final ExecutorCLI cliUnderTest = new ExecutorCLI(new Configuration());

		final String[] args = {"-Dtest=tsetD-"};
		final CommandLine commandLine = CliFrontendParser.parse(testOptions, args, true);

		cliUnderTest.applyCommandLineOptionsToConfiguration(commandLine);
	}

	@Test
	public void testExecutorInBaseConfigIsPickedUp() throws CliArgsException {
		final String expectedExecutorName = "test-executor";
		final Configuration loadedConfig = new Configuration();
		loadedConfig.set(DeploymentOptions.TARGET, expectedExecutorName);

		final ExecutorCLI cliUnderTest = new ExecutorCLI(loadedConfig);
		final CommandLine emptyCommandLine = CliFrontendParser.parse(testOptions, new String[0], true);

		final Configuration configuration = cliUnderTest.applyCommandLineOptionsToConfiguration(emptyCommandLine);
		assertEquals(expectedExecutorName, configuration.get(DeploymentOptions.TARGET));
	}

	@Test
	public void testWithPreexistingConfigurationInConstructor() throws CliArgsException {
		final Configuration loadedConfig = new Configuration();
		loadedConfig.setInteger(CoreOptions.DEFAULT_PARALLELISM, 2);
		loadedConfig.setBoolean(DeploymentOptions.ATTACHED, false);

		final ConfigOption<List<Integer>> listOption = key("test.list")
				.intType()
				.asList()
				.noDefaultValue();

		final List<Integer> listValue = Arrays.asList(41, 42, 23);
		final String encodedListValue = listValue
				.stream()
				.map(Object::toString)
				.collect(Collectors.joining(";"));

		final String[] args = {
				"-e", "test-executor",
				"-D" + listOption.key() + "=" + encodedListValue,
				"-D" + CoreOptions.DEFAULT_PARALLELISM.key() + "=5"
		};

		final ExecutorCLI cliUnderTest = new ExecutorCLI(loadedConfig);
		final CommandLine commandLine = CliFrontendParser.parse(testOptions, args, true);

		final Configuration configuration = cliUnderTest.applyCommandLineOptionsToConfiguration(commandLine);

		assertEquals("test-executor", configuration.getString(DeploymentOptions.TARGET));
		assertEquals(5, configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM));
		assertFalse(configuration.getBoolean(DeploymentOptions.ATTACHED));
		assertEquals(listValue, configuration.get(listOption));
	}

	@Test
	public void testIsActiveLong() throws CliArgsException {
		testIsActiveHelper("--executor");
	}

	@Test
	public void testIsActiveShort() throws CliArgsException {
		testIsActiveHelper("-e");
	}

	private void testIsActiveHelper(final String executorOption) throws CliArgsException {
		final String expectedExecutorName = "test-executor";
		final ConfigOption<Integer> configOption = key("test.int").intType().noDefaultValue();
		final int expectedValue = 42;

		final ExecutorCLI cliUnderTest = new ExecutorCLI(new Configuration());

		final String[] args = {executorOption, expectedExecutorName, "-D" + configOption.key() + "=" + expectedValue};
		final CommandLine commandLine = CliFrontendParser.parse(testOptions, args, true);

		final Configuration configuration = cliUnderTest.applyCommandLineOptionsToConfiguration(commandLine);
		assertEquals(expectedExecutorName, configuration.get(DeploymentOptions.TARGET));
		assertEquals(expectedValue, configuration.getInteger(configOption));
	}
}
