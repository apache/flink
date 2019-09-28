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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.when;

/**
 * Tests for the {@link RemoteStreamEnvironment}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({RemoteStreamEnvironment.class})
public class RemoteStreamExecutionEnvironmentTest extends TestLogger {

	/**
	 * Verifies that the port passed to the RemoteStreamEnvironment is used for connecting to the cluster.
	 */
	@Test
	public void testPortForwarding() throws Exception {

		String host = "fakeHost";
		int port = 99;
		JobExecutionResult expectedResult = new JobExecutionResult(null, 0, null);

		RestClusterClient mockedClient = Mockito.mock(RestClusterClient.class);
		when(mockedClient.run(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
			.thenReturn(expectedResult);

		PowerMockito.whenNew(RestClusterClient.class).withAnyArguments().thenAnswer((invocation) -> {
				Object[] args = invocation.getArguments();
				Configuration config = (Configuration) args[0];

				Assert.assertEquals(host, config.getString(RestOptions.ADDRESS));
				Assert.assertEquals(port, config.getInteger(RestOptions.PORT));
				return mockedClient;
			}
		);

		final Configuration clientConfiguration = new Configuration();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
			host, port, clientConfiguration);
		env.fromElements(1).map(x -> x * 2);
		JobExecutionResult actualResult = env.execute("fakeJobName");
		Assert.assertEquals(expectedResult, actualResult);
	}

	@Test
	public void testRemoteExecutionWithSavepoint() throws Exception {
		SavepointRestoreSettings restoreSettings = SavepointRestoreSettings.forPath("fakePath");
		RemoteStreamEnvironment env = new RemoteStreamEnvironment("fakeHost", 1,
			null, new String[]{}, null, restoreSettings);
		env.fromElements(1).map(x -> x * 2);

		RestClusterClient mockedClient = Mockito.mock(RestClusterClient.class);
		JobExecutionResult expectedResult = new JobExecutionResult(null, 0, null);

		PowerMockito.whenNew(RestClusterClient.class).withAnyArguments().thenReturn(mockedClient);
		when(mockedClient.run(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.eq(restoreSettings)))
			.thenReturn(expectedResult);

		JobExecutionResult actualResult = env.execute("fakeJobName");
		Assert.assertEquals(expectedResult, actualResult);
	}
}
