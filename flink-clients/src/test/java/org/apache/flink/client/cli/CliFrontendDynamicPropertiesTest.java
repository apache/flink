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

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders.ParentFirstClassLoader;
import org.apache.flink.util.ChildFirstClassLoader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.net.URL;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.client.cli.CliFrontendTestUtils.TEST_JAR_MAIN_CLASS;
import static org.apache.flink.client.cli.CliFrontendTestUtils.getTestJarPath;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;


/**
 * Tests for the RUN command with Dynamic Properties.
 */
public class CliFrontendDynamicPropertiesTest {

	private CliFrontend frontend;

	private Options testOptions;

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}

	@AfterClass
	public static void shutdown() {
		CliFrontendTestUtils.restoreSystemOut();
	}

	@Before
	public void setup() throws Exception {
		testOptions = new Options();

		final Configuration configuration = new Configuration();
		final GenericCLI cliUnderTest = new GenericCLI(
			configuration,
			tmp.getRoot().getAbsolutePath());

		frontend = new CliFrontend(
			configuration,
			Collections.singletonList(cliUnderTest));

		cliUnderTest.addGeneralOptions(testOptions);
	}

	@Test
	public void testDynamicPropertiesWithParentFirstClassloader() throws Exception {

		String[] args = {
			"-e", "test-executor",
			"-D" + CoreOptions.DEFAULT_PARALLELISM.key() + "=5",
			"-D" + "classloader.resolve-order=parent-first",
			getTestJarPath(), "-a", "--debug", "true", "arg1", "arg2"
		};

		CommandLine commandLine = CliFrontendParser.parse(testOptions, args, true);

		ProgramOptions programOptions = ProgramOptions.create(commandLine);

		assertEquals(getTestJarPath(), programOptions.getJarFilePath());

		PackagedProgram program = frontend.buildProgram(programOptions, false);

		final List<URL> jobJars = program.getJobJarAndDependencies();
		CustomCommandLine activeCommandLine = frontend
			.validateAndGetActiveCommandLine(checkNotNull(commandLine));

		final Configuration effectiveConfiguration = frontend.getEffectiveConfiguration(
			activeCommandLine, commandLine, programOptions, jobJars);

		program.initClassLoadOnEffectiveConfiguration(effectiveConfiguration);

		Assert.assertEquals(TEST_JAR_MAIN_CLASS, program.getMainClassName());
		Assert.assertEquals(ParentFirstClassLoader.class.getName(),
			program.getUserCodeClassLoader().getClass().getName());
	}

	@Test
	public void testDynamicPropertiesWithDefaultChildFirstClassloader() throws Exception {

		String[] args = {
			"-e", "test-executor",
			"-D" + CoreOptions.DEFAULT_PARALLELISM.key() + "=5",
			getTestJarPath(), "-a", "--debug", "true", "arg1", "arg2"
		};

		CommandLine commandLine = CliFrontendParser.parse(testOptions, args, true);

		ProgramOptions programOptions = ProgramOptions.create(commandLine);

		assertEquals(getTestJarPath(), programOptions.getJarFilePath());

		PackagedProgram program = frontend.buildProgram(programOptions, true);

		Assert.assertEquals(TEST_JAR_MAIN_CLASS, program.getMainClassName());
		Assert.assertEquals(ChildFirstClassLoader.class.getName(),
			program.getUserCodeClassLoader().getClass().getName());
	}

	@Test
	public void testDynamicPropertiesWithChildFirstClassloader() throws Exception {

		String[] args = {
			"-e", "test-executor",
			"-D" + CoreOptions.DEFAULT_PARALLELISM.key() + "=5",
			"-D" + "classloader.resolve-order=child-first",
			getTestJarPath(), "-a", "--debug", "true", "arg1", "arg2"
		};

		CommandLine commandLine = CliFrontendParser.parse(testOptions, args, true);

		ProgramOptions programOptions = ProgramOptions.create(commandLine);

		assertEquals(getTestJarPath(), programOptions.getJarFilePath());

		PackagedProgram program = frontend.buildProgram(programOptions, false);

		final List<URL> jobJars = program.getJobJarAndDependencies();
		CustomCommandLine activeCommandLine = frontend
			.validateAndGetActiveCommandLine(checkNotNull(commandLine));

		final Configuration effectiveConfiguration = frontend.getEffectiveConfiguration(
			activeCommandLine, commandLine, programOptions, jobJars);

		program.initClassLoadOnEffectiveConfiguration(effectiveConfiguration);

		Assert.assertEquals(TEST_JAR_MAIN_CLASS, program.getMainClassName());
		Assert.assertEquals(ChildFirstClassLoader.class.getName(),
			program.getUserCodeClassLoader().getClass().getName());
	}

}
