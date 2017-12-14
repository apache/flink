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

import org.apache.flink.client.util.MockedCliFrontend;
import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for the LIST command.
 */
public class CliFrontendListTest extends TestLogger {

	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}

	@Test
	public void testList() throws Exception {
		// test unrecognized option
		{
			String[] parameters = {"-v", "-k"};
			CliFrontend testFrontend = new CliFrontend(CliFrontendTestUtils.getConfigDir());
			int retCode = testFrontend.list(parameters);
			assertTrue(retCode != 0);
		}

		// test list properly
		{
			String[] parameters = {"-r", "-s"};
			ListTestCliFrontend testFrontend = new ListTestCliFrontend();
			int retCode = testFrontend.list(parameters);
			assertTrue(retCode == 0);
			Mockito.verify(testFrontend.client, times(1))
				.listJobs();
		}
	}

	private static final class ListTestCliFrontend extends MockedCliFrontend {

		ListTestCliFrontend() throws Exception {
			when(client.listJobs()).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
		}
	}
}
