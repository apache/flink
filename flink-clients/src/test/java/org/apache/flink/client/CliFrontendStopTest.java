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

package org.apache.flink.client;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.Flip6DefaultCLI;
import org.apache.flink.client.cli.StopOptions;
import org.apache.flink.client.util.MockedCliFrontend;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.flink.client.CliFrontendTestUtils.pipeSystemOutToNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.doThrow;

/**
 * Tests for the STOP command.
 */
public class CliFrontendStopTest extends TestLogger {

	@BeforeClass
	public static void setup() {
		pipeSystemOutToNull();
	}

	@Test
	public void testStop() throws Exception {
		// test unrecognized option
		{
			String[] parameters = { "-v", "-l" };
			CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
			int retCode = testFrontend.stop(parameters);
			assertTrue(retCode != 0);
		}

		// test missing job id
		{
			String[] parameters = {};
			CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
			int retCode = testFrontend.stop(parameters);
			assertTrue(retCode != 0);
		}

		// test stop properly
		{
			JobID jid = new JobID();
			String jidString = jid.toString();

			String[] parameters = { jidString };
			StopTestCliFrontend testFrontend = new StopTestCliFrontend(false);

			int retCode = testFrontend.stop(parameters);
			assertEquals(0, retCode);

			Mockito.verify(testFrontend.client, times(1)).stop(any(JobID.class));
		}

		// test unknown job Id
		{
			JobID jid = new JobID();

			String[] parameters = { jid.toString() };
			StopTestCliFrontend testFrontend = new StopTestCliFrontend(true);

			assertTrue(testFrontend.stop(parameters) != 0);

			Mockito.verify(testFrontend.client, times(1)).stop(any(JobID.class));
		}

		// test flip6 switch
		{
			String[] parameters =
				{"-flip6", String.valueOf(new JobID())};
			StopOptions options = CliFrontendParser.parseStopCommand(parameters);
			assertTrue(options.getCommandLine().hasOption(Flip6DefaultCLI.FLIP_6.getOpt()));
		}
	}

	private static final class StopTestCliFrontend extends MockedCliFrontend {

		StopTestCliFrontend(boolean reject) throws Exception {
			if (reject) {
				doThrow(new IllegalArgumentException("Test exception")).when(client).stop(any(JobID.class));
			}
		}
	}
}
