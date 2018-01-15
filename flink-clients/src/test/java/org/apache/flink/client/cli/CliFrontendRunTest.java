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

import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;

import static org.apache.flink.client.cli.CliFrontendTestUtils.getTestJarPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the RUN command.
 */
public class CliFrontendRunTest {

	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}

	@Test
	public void testRun() throws Exception {
		final Configuration configuration = GlobalConfiguration.loadConfiguration(CliFrontendTestUtils.getConfigDir());
		// test without parallelism
		{
			String[] parameters = {"-v", getTestJarPath()};
			RunTestingCliFrontend testFrontend = new RunTestingCliFrontend(configuration, 1, true, false);
			testFrontend.run(parameters);
		}

		// test configure parallelism
		{
			String[] parameters = {"-v", "-p", "42",  getTestJarPath()};
			RunTestingCliFrontend testFrontend = new RunTestingCliFrontend(configuration, 42, true, false);
			testFrontend.run(parameters);
		}

		// test configure sysout logging
		{
			String[] parameters = {"-p", "2", "-q", getTestJarPath()};
			RunTestingCliFrontend testFrontend = new RunTestingCliFrontend(configuration, 2, false, false);
			testFrontend.run(parameters);
		}

		// test detached mode
		{
			String[] parameters = {"-p", "2", "-d", getTestJarPath()};
			RunTestingCliFrontend testFrontend = new RunTestingCliFrontend(configuration, 2, true, true);
			testFrontend.run(parameters);
		}

		// test configure savepoint path (no ignore flag)
		{
			String[] parameters = {"-s", "expectedSavepointPath", getTestJarPath()};
			RunOptions options = CliFrontendParser.parseRunCommand(parameters);
			SavepointRestoreSettings savepointSettings = options.getSavepointRestoreSettings();
			assertTrue(savepointSettings.restoreSavepoint());
			assertEquals("expectedSavepointPath", savepointSettings.getRestorePath());
			assertFalse(savepointSettings.allowNonRestoredState());
		}

		// test configure savepoint path (with ignore flag)
		{
			String[] parameters = {"-s", "expectedSavepointPath", "-n", getTestJarPath()};
			RunOptions options = CliFrontendParser.parseRunCommand(parameters);
			SavepointRestoreSettings savepointSettings = options.getSavepointRestoreSettings();
			assertTrue(savepointSettings.restoreSavepoint());
			assertEquals("expectedSavepointPath", savepointSettings.getRestorePath());
			assertTrue(savepointSettings.allowNonRestoredState());
		}

		// test jar arguments
		{
			String[] parameters =
				{ getTestJarPath(), "-arg1", "value1", "justavalue", "--arg2", "value2"};
			RunOptions options = CliFrontendParser.parseRunCommand(parameters);
			assertEquals("-arg1", options.getProgramArgs()[0]);
			assertEquals("value1", options.getProgramArgs()[1]);
			assertEquals("justavalue", options.getProgramArgs()[2]);
			assertEquals("--arg2", options.getProgramArgs()[3]);
			assertEquals("value2", options.getProgramArgs()[4]);
		}
	}

	@Test(expected = CliArgsException.class)
	public void testUnrecognizedOption() throws Exception {
		// test unrecognized option
		String[] parameters = {"-v", "-l", "-a", "some", "program", "arguments"};
		Configuration configuration = new Configuration();
		CliFrontend testFrontend = new CliFrontend(
			configuration,
			Collections.singletonList(new DefaultCLI(configuration)));
		testFrontend.run(parameters);
	}

	@Test(expected = CliArgsException.class)
	public void testInvalidParallelismOption() throws Exception {
		// test configure parallelism with non integer value
		String[] parameters = {"-v", "-p", "text",  getTestJarPath()};
		Configuration configuration = new Configuration();
		CliFrontend testFrontend = new CliFrontend(
			configuration,
			Collections.singletonList(new DefaultCLI(configuration)));
		testFrontend.run(parameters);
	}

	@Test(expected = CliArgsException.class)
	public void testParallelismWithOverflow() throws Exception {
		// test configure parallelism with overflow integer value
		String[] parameters = {"-v", "-p", "475871387138",  getTestJarPath()};
		Configuration configuration = new Configuration();
		CliFrontend testFrontend = new CliFrontend(
			configuration,
			Collections.singletonList(new DefaultCLI(configuration)));
		testFrontend.run(parameters);
	}

	// --------------------------------------------------------------------------------------------

	private static final class RunTestingCliFrontend extends CliFrontend {

		private final int expectedParallelism;
		private final boolean sysoutLogging;
		private final boolean isDetached;

		public RunTestingCliFrontend(Configuration configuration, int expectedParallelism, boolean logging, boolean isDetached) throws Exception {
			super(
				configuration,
				Collections.singletonList(new DefaultCLI(configuration)));
			this.expectedParallelism = expectedParallelism;
			this.sysoutLogging = logging;
			this.isDetached = isDetached;
		}

		@Override
		protected void executeProgram(PackagedProgram program, ClusterClient client, int parallelism) {
			assertEquals(isDetached, client.isDetached());
			assertEquals(sysoutLogging, client.getPrintStatusDuringExecution());
			assertEquals(expectedParallelism, parallelism);
		}
	}
}
