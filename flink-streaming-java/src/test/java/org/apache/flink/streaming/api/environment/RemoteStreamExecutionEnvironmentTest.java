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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.RemoteExecutor;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link RemoteStreamEnvironment}.
 */
@RunWith(PowerMockRunner.class)
// TODO: I don't like that I have to do this
@PrepareForTest({RemoteStreamEnvironment.class, RemoteExecutor.class})
public class RemoteStreamExecutionEnvironmentTest extends TestLogger {

	/**
	 * Verifies that the port passed to the RemoteStreamEnvironment is used for connecting to the cluster.
	 */
	@Test
	public void testPortForwarding() throws Exception {

		String host = "fakeHost";
		int port = 99;
		JobID jobID = new JobID();
		JobResult jobResult = (new JobResult.Builder())
			.jobId(jobID)
			.netRuntime(0)
			.applicationStatus(ApplicationStatus.SUCCEEDED)
			.build();

		RestClusterClient mockedClient = Mockito.mock(RestClusterClient.class);
		when(mockedClient.submitJob(any())).thenReturn(CompletableFuture.completedFuture(new JobSubmissionResult(jobID)));
		when(mockedClient.requestJobResult(any())).thenReturn(CompletableFuture.completedFuture(jobResult));

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
		Assert.assertEquals(jobID, actualResult.getJobID());
	}

	@Test
	public void testRemoteExecutionWithSavepoint() throws Exception {
		SavepointRestoreSettings restoreSettings = SavepointRestoreSettings.forPath("fakePath");
		RemoteStreamEnvironment env = new RemoteStreamEnvironment("fakeHost", 1,
			null, new String[]{}, null, restoreSettings);
		env.fromElements(1).map(x -> x * 2);

		JobID jobID = new JobID();
		JobResult jobResult = (new JobResult.Builder())
			.jobId(jobID)
			.netRuntime(0)
			.applicationStatus(ApplicationStatus.SUCCEEDED)
			.build();

		RestClusterClient mockedClient = Mockito.mock(RestClusterClient.class);

		PowerMockito.whenNew(RestClusterClient.class).withAnyArguments().thenReturn(mockedClient);
		when(mockedClient.submitJob(any())).thenReturn(CompletableFuture.completedFuture(new JobSubmissionResult(jobID)));
		when(mockedClient.requestJobResult(eq(jobID))).thenReturn(CompletableFuture.completedFuture(jobResult));

		JobExecutionResult actualResult = env.execute("fakeJobName");
		Assert.assertEquals(jobID, actualResult.getJobID());
	}
}
