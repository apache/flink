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
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;

import java.util.Collections;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.doThrow;

/**
 * Tests for the STOP command.
 */
public class CliFrontendStopWithSavepointTest extends CliFrontendTestBase {

	@BeforeClass
	public static void setup() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}

	@AfterClass
	public static void shutdown() {
		CliFrontendTestUtils.restoreSystemOut();
	}

	@Test
	public void testStopWithOnlyJobId() throws Exception {
		// test stop properly
		JobID jid = new JobID();
		String jidString = jid.toString();

		String[] parameters = { jidString };
		final ClusterClient<String> clusterClient = createClusterClient(null);
		MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);

		testFrontend.stop(parameters);

		Mockito.verify(clusterClient, times(1))
				.stopWithSavepoint(eq(jid), eq(false), isNull());
	}

	@Test
	public void testStopWithDefaultSavepointDir() throws Exception {
		JobID jid = new JobID();

		String[] parameters = {jid.toString() };
		final ClusterClient<String> clusterClient = createClusterClient(null);
		MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
		testFrontend.stop(parameters);

		Mockito.verify(clusterClient, times(1))
				.stopWithSavepoint(eq(jid), eq(false), isNull());
	}

	@Test
	public void testStopWithExplicitSavepointDir() throws Exception {
		JobID jid = new JobID();

		String[] parameters = { "-p", "test-target-dir", jid.toString() };
		final ClusterClient<String> clusterClient = createClusterClient(null);
		MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
		testFrontend.stop(parameters);

		Mockito.verify(clusterClient, times(1))
				.stopWithSavepoint(eq(jid), eq(false), eq("test-target-dir"));
	}

	@Test
	public void testStopOnlyWithMaxWM() throws Exception {
		JobID jid = new JobID();

		String[] parameters = { "-d", jid.toString() };
		final ClusterClient<String> clusterClient = createClusterClient(null);
		MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
		testFrontend.stop(parameters);

		Mockito.verify(clusterClient, times(1))
				.stopWithSavepoint(eq(jid), eq(true), isNull());
	}

	@Test
	public void testStopWithMaxWMAndDefaultSavepointDir() throws Exception {
		JobID jid = new JobID();

		String[] parameters = { "-p", "-d", jid.toString() };
		final ClusterClient<String> clusterClient = createClusterClient(null);
		MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
		testFrontend.stop(parameters);

		Mockito.verify(clusterClient, times(1))
				.stopWithSavepoint(eq(jid), eq(true), isNull());
	}

	@Test
	public void testStopWithMaxWMAndExplicitSavepointDir() throws Exception {
		JobID jid = new JobID();

		String[] parameters = { "-d", "-p", "test-target-dir", jid.toString() };
		final ClusterClient<String> clusterClient = createClusterClient(null);
		MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
		testFrontend.stop(parameters);

		Mockito.verify(clusterClient, times(1))
				.stopWithSavepoint(eq(jid), eq(true), eq("test-target-dir"));
	}

	@Test(expected = CliArgsException.class)
	public void testUnrecognizedOption() throws Exception {
		// test unrecognized option
		String[] parameters = { "-v", "-l" };
		Configuration configuration = getConfiguration();
		CliFrontend testFrontend = new CliFrontend(
			configuration,
			Collections.singletonList(getCli(configuration)));
		testFrontend.stop(parameters);
	}

	@Test(expected = CliArgsException.class)
	public void testMissingJobId() throws Exception {
		// test missing job id
		String[] parameters = {};
		Configuration configuration = getConfiguration();
		CliFrontend testFrontend = new CliFrontend(
			configuration,
			Collections.singletonList(getCli(configuration)));
		testFrontend.stop(parameters);
	}

	@Test(expected = CliArgsException.class)
	public void testWrongSavepointDirOrder() throws Exception {
		JobID jid = new JobID();

		String[] parameters = { "-s", "-d", "test-target-dir", jid.toString() };
		final ClusterClient<String> clusterClient = createClusterClient(null);
		MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
		testFrontend.stop(parameters);

		Mockito.verify(clusterClient, times(1))
				.stopWithSavepoint(eq(jid), eq(false), eq("test-target-dir"));
	}

	@Test
	public void testUnknownJobId() throws Exception {
		// test unknown job Id
		JobID jid = new JobID();

		String[] parameters = { "-p", "test-target-dir", jid.toString() };
		String expectedMessage = "Test exception";
		FlinkException testException = new FlinkException(expectedMessage);
		final ClusterClient<String> clusterClient = createClusterClient(testException);
		MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);

		try {
			testFrontend.stop(parameters);
			fail("Should have failed.");
		} catch (FlinkException e) {
			assertTrue(ExceptionUtils.findThrowableWithMessage(e, expectedMessage).isPresent());
		}
	}

	private static ClusterClient<String> createClusterClient(@Nullable Exception exception) throws Exception {
		final ClusterClient<String> clusterClient = mock(ClusterClient.class);

		if (exception != null) {
			doThrow(exception).when(clusterClient).stopWithSavepoint(any(JobID.class), anyBoolean(), anyString());
		}

		return clusterClient;
	}
}
