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

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.util.MockedCliFrontend;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.flink.client.cli.CliFrontendTestUtils.pipeSystemOutToNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
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

		// test flip6 switch
		{
			String[] parameters =
				{"-flip6", String.valueOf(new JobID())};
			StopOptions options = CliFrontendParser.parseStopCommand(parameters);
			assertTrue(options.getCommandLine().hasOption(Flip6DefaultCLI.FLIP_6.getOpt()));
		}
	}

	@Test(expected = CliArgsException.class)
	public void testUnrecognizedOption() throws Exception {
		// test unrecognized option
		String[] parameters = { "-v", "-l" };
		CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
		testFrontend.stop(parameters);

		fail("Should have failed.");
	}

	@Test(expected = CliArgsException.class)
	public void testMissingJobId() throws Exception {
		// test missing job id
		String[] parameters = {};
		CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
		testFrontend.stop(parameters);

		fail("Should have failed.");
	}

	@Test(expected = TestException.class)
	public void testUnknownJobId() throws Exception {
		// test unknown job Id
		JobID jid = new JobID();

		String[] parameters = { jid.toString() };
		StopTestCliFrontend testFrontend = new StopTestCliFrontend(true);

		testFrontend.stop(parameters);
		fail("Should have failed.");
	}

	private static final class TestException extends FlinkException {
		private static final long serialVersionUID = -2650760898729937583L;

		TestException(String message) {
			super(message);
		}
	}

	private static final class StopTestCliFrontend extends MockedCliFrontend {

		StopTestCliFrontend(boolean reject) throws Exception {
			if (reject) {
				doThrow(new TestException("Test Exception")).when(client).stop(any(JobID.class));
			}
		}
	}
}
