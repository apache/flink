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

import org.apache.flink.client.cli.util.MockedCliFrontend;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Tests for the LIST command.
 */
public class CliFrontendListTest extends CliFrontendTestBase {

	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}

	@AfterClass
	public static void shutdown() {
		CliFrontendTestUtils.restoreSystemOut();
	}

	@Test
	public void testList() throws Exception {
		// test list properly
		{
			String[] parameters = {"-r", "-s"};
			ClusterClient<String> clusterClient = createClusterClient();
			MockedCliFrontend testFrontend = new MockedCliFrontend(clusterClient);
			testFrontend.list(parameters);
			Mockito.verify(clusterClient, times(1))
				.listJobs();
		}
	}

	@Test(expected = CliArgsException.class)
	public void testUnrecognizedOption() throws Exception {
		String[] parameters = {"-v", "-k"};
		Configuration configuration = getConfiguration();
		CliFrontend testFrontend = new CliFrontend(
			configuration,
			Collections.singletonList(getCli(configuration)));
		testFrontend.list(parameters);
	}

	private static ClusterClient<String> createClusterClient() throws Exception {
		final ClusterClient<String> clusterClient = mock(ClusterClient.class);

		when(clusterClient.listJobs()).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));

		return clusterClient;
	}
}
