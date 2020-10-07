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
import org.apache.flink.client.program.TestingClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.messages.Acknowledge;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Tests for the CANCEL command.
 */
public class CliFrontendCancelTest extends CliFrontendTestBase {

	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}

	@AfterClass
	public static void shutdown() {
		CliFrontendTestUtils.restoreSystemOut();
	}

	@Test
	public void testCancel() throws Exception {
		// test cancel properly
		JobID jid = new JobID();

		OneShotLatch cancelLatch = new OneShotLatch();

		String[] parameters = { jid.toString() };

		TestingClusterClient<String> clusterClient = new TestingClusterClient<>();

		clusterClient.setCancelFunction(jobID -> {
			cancelLatch.trigger();
			return CompletableFuture.completedFuture(Acknowledge.get());
		});

		MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
		testFrontend.cancel(parameters);
		cancelLatch.await();
	}

	@Test(expected = CliArgsException.class)
	public void testMissingJobId() throws Exception {
		String[] parameters = {};
		Configuration configuration = getConfiguration();
		CliFrontend testFrontend = new CliFrontend(
			configuration,
			Collections.singletonList(getCli()));
		testFrontend.cancel(parameters);
	}

	@Test(expected = CliArgsException.class)
	public void testUnrecognizedOption() throws Exception {
		String[] parameters = {"-v", "-l"};
		Configuration configuration = getConfiguration();
		CliFrontend testFrontend = new CliFrontend(
			configuration,
			Collections.singletonList(getCli()));
		testFrontend.cancel(parameters);
	}

	/**
	 * Tests cancelling with the savepoint option.
	 */
	@Test
	public void testCancelWithSavepoint() throws Exception {
		{
			// Cancel with savepoint (no target directory)
			JobID jid = new JobID();

			OneShotLatch cancelWithSavepointLatch = new OneShotLatch();

			String[] parameters = { "-s", jid.toString() };
			TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
			clusterClient.setCancelWithSavepointFunction((jobID, savepointDirectory) -> {
				assertNull(savepointDirectory);
				cancelWithSavepointLatch.trigger();
				return CompletableFuture.completedFuture(savepointDirectory);
			});
			MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
			testFrontend.cancel(parameters);
			cancelWithSavepointLatch.await();
		}

		{
			// Cancel with savepoint (with target directory)
			JobID jid = new JobID();

			OneShotLatch cancelWithSavepointLatch = new OneShotLatch();

			String[] parameters = { "-s", "targetDirectory", jid.toString() };
			TestingClusterClient<String> clusterClient = new TestingClusterClient<>();
			clusterClient.setCancelWithSavepointFunction((jobID, savepointDirectory) -> {
				assertNotNull(savepointDirectory);
				cancelWithSavepointLatch.trigger();
				return CompletableFuture.completedFuture(savepointDirectory);
			});
			MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
			testFrontend.cancel(parameters);
			cancelWithSavepointLatch.await();
		}
	}

	@Test(expected = CliArgsException.class)
	public void testCancelWithSavepointWithoutJobId() throws Exception {
		// Cancel with savepoint (with target directory), but no job ID
		String[] parameters = { "-s", "targetDirectory" };
		Configuration configuration = getConfiguration();
		CliFrontend testFrontend = new CliFrontend(
			configuration,
			Collections.singletonList(getCli()));
		testFrontend.cancel(parameters);
	}

	@Test(expected = CliArgsException.class)
	public void testCancelWithSavepointWithoutParameters() throws Exception {
		// Cancel with savepoint (no target directory) and no job ID
		String[] parameters = { "-s" };
		Configuration configuration = getConfiguration();
		CliFrontend testFrontend = new CliFrontend(
			configuration,
			Collections.singletonList(getCli()));
		testFrontend.cancel(parameters);
	}
}
