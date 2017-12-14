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
import org.apache.flink.client.cli.CancelOptions;
import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.client.cli.Flip6DefaultCLI;
import org.apache.flink.client.util.MockedCliFrontend;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isNull;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.doThrow;

/**
 * Tests for the CANCEL command.
 */
public class CliFrontendCancelTest {

	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}

	@Test
	public void testCancel() {
		try {
			// test unrecognized option
			{
				String[] parameters = {"-v", "-l"};
				CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode != 0);
			}

			// test missing job id
			{
				String[] parameters = {};
				CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode != 0);
			}

			// test cancel properly
			{
				JobID jid = new JobID();

				String[] parameters = { jid.toString() };
				CancelTestCliFrontend testFrontend = new CancelTestCliFrontend(false);

				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode == 0);

				Mockito.verify(testFrontend.client, times(1)).cancel(any(JobID.class));
			}

			// test cancel properly
			{
				JobID jid = new JobID();

				String[] parameters = { jid.toString() };
				CancelTestCliFrontend testFrontend = new CancelTestCliFrontend(true);

				int retCode = testFrontend.cancel(parameters);
				assertTrue(retCode != 0);

				Mockito.verify(testFrontend.client, times(1)).cancel(any(JobID.class));
			}

			// test flip6 switch
			{
				String[] parameters =
					{"-flip6", String.valueOf(new JobID())};
				CancelOptions options = CliFrontendParser.parseCancelCommand(parameters);
				assertTrue(options.getCommandLine().hasOption(Flip6DefaultCLI.FLIP_6.getOpt()));
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Program caused an exception: " + e.getMessage());
		}
	}

	/**
	 * Tests cancelling with the savepoint option.
	 */
	@Test
	public void testCancelWithSavepoint() throws Exception {
		{
			// Cancel with savepoint (no target directory)
			JobID jid = new JobID();

			String[] parameters = { "-s", jid.toString() };
			CancelTestCliFrontend testFrontend = new CancelTestCliFrontend(false);
			assertEquals(0, testFrontend.cancel(parameters));

			Mockito.verify(testFrontend.client, times(1))
				.cancelWithSavepoint(any(JobID.class), isNull(String.class));
		}

		{
			// Cancel with savepoint (with target directory)
			JobID jid = new JobID();

			String[] parameters = { "-s", "targetDirectory", jid.toString() };
			CancelTestCliFrontend testFrontend = new CancelTestCliFrontend(false);
			assertEquals(0, testFrontend.cancel(parameters));

			Mockito.verify(testFrontend.client, times(1))
				.cancelWithSavepoint(any(JobID.class), notNull(String.class));
		}

		{
			// Cancel with savepoint (with target directory), but no job ID
			String[] parameters = { "-s", "targetDirectory" };
			CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
			assertNotEquals(0, testFrontend.cancel(parameters));
		}

		{
			// Cancel with savepoint (no target directory) and no job ID
			String[] parameters = { "-s" };
			CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
			assertNotEquals(0, testFrontend.cancel(parameters));
		}
	}

	private static final class CancelTestCliFrontend extends MockedCliFrontend {

		CancelTestCliFrontend(boolean reject) throws Exception {
			if (reject) {
				doThrow(new IllegalArgumentException("Test exception")).when(client).cancel(any(JobID.class));
				doThrow(new IllegalArgumentException("Test exception")).when(client).cancelWithSavepoint(any(JobID.class), anyString());
			}
		}
	}
}
