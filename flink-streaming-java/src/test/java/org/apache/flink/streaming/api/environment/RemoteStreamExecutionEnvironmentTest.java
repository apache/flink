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
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.Executor;
import org.apache.flink.core.execution.ExecutorFactory;
import org.apache.flink.core.execution.ExecutorServiceLoader;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link RemoteStreamEnvironment}.
 */
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
		when(mockedClient.submitJob(any())).thenReturn(CompletableFuture.completedFuture(jobID));
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
		final StreamExecutionEnvironment env = new RemoteStreamEnvironment(
				new TestExecutorServiceLoader(jobID, mockedClient, SavepointRestoreSettings.none()),
				host,
				port,
				clientConfiguration,
				null,
				null,
				null
		);
		env.fromElements(1).map(x -> x * 2);
		JobExecutionResult actualResult = env.execute("fakeJobName");
		Assert.assertEquals(jobID, actualResult.getJobID());
	}

	@Test
	public void testRemoteExecutionWithSavepoint() throws Exception {
		SavepointRestoreSettings restoreSettings = SavepointRestoreSettings.forPath("fakePath");

		JobID jobID = new JobID();
		JobResult jobResult = (new JobResult.Builder())
			.jobId(jobID)
			.netRuntime(0)
			.applicationStatus(ApplicationStatus.SUCCEEDED)
			.build();

		RestClusterClient mockedClient = Mockito.mock(RestClusterClient.class);

		PowerMockito.whenNew(RestClusterClient.class).withAnyArguments().thenReturn(mockedClient);
		when(mockedClient.submitJob(any())).thenReturn(CompletableFuture.completedFuture(jobID));
		when(mockedClient.requestJobResult(eq(jobID))).thenReturn(CompletableFuture.completedFuture(jobResult));

		RemoteStreamEnvironment env = new RemoteStreamEnvironment(
				new TestExecutorServiceLoader(jobID, mockedClient, restoreSettings),
				"fakeHost",
				1,
				null,
				new String[]{},
				null,
				restoreSettings);

		env.fromElements(1).map(x -> x * 2);

		JobExecutionResult actualResult = env.execute("fakeJobName");
		Assert.assertEquals(jobID, actualResult.getJobID());
	}

	private static final class TestExecutorServiceLoader implements ExecutorServiceLoader {

		private final JobID jobID;
		private final ClusterClient<?> clusterClient;
		private final SavepointRestoreSettings expectedSavepointRestoreSettings;

		TestExecutorServiceLoader(
				final JobID jobID,
				final ClusterClient<?> clusterClient,
				final SavepointRestoreSettings savepointRestoreSettings) {
			this.jobID = checkNotNull(jobID);
			this.clusterClient = checkNotNull(clusterClient);
			this.expectedSavepointRestoreSettings = checkNotNull(savepointRestoreSettings);
		}

		@Override
		public ExecutorFactory getExecutorFactory(@Nonnull Configuration configuration) {
			return new ExecutorFactory() {
				@Override
				public boolean isCompatibleWith(@Nonnull Configuration configuration) {
					return true;
				}

				@Override
				public Executor getExecutor(@Nonnull Configuration configuration) {
					return (pipeline, config) -> {
						assertTrue(pipeline instanceof StreamGraph);

						final SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.fromConfiguration(config);
						assertEquals(expectedSavepointRestoreSettings, savepointRestoreSettings);

						return CompletableFuture.completedFuture(
								new ClusterClientJobClientAdapter<>(clusterClient, jobID));
					};
				}
			};
		}
	}
}
