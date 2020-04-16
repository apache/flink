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

package org.apache.flink.test.streaming.api.environment;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.core.execution.PipelineExecutorFactory;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.JobResult.Builder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link RemoteStreamEnvironment}.
 */
public class RemoteStreamEnvironmentTest extends TestLogger {

	/**
	 * Verifies that the port passed to the RemoteStreamEnvironment is used for connecting to the
	 * cluster.
	 */
	@Test
	public void testPortForwarding() throws Exception {
		String host = "fakeHost";
		int port = 99;
		JobID jobId = new JobID();

		final Configuration clientConfiguration = new Configuration();
		TestExecutorServiceLoader testExecutorServiceLoader = new TestExecutorServiceLoader(jobId);
		final StreamExecutionEnvironment env = new RemoteStreamEnvironment(
				testExecutorServiceLoader,
				host,
				port,
				clientConfiguration,
				null,
				null,
				null
		);
		env.fromElements(1).map(x -> x * 2);

		JobExecutionResult actualResult = env.execute("fakeJobName");
		TestClusterClient testClient = testExecutorServiceLoader.getCreatedClusterClient();
		assertThat(actualResult.getJobID(), is(jobId));
		assertThat(testClient.getConfiguration().getString(RestOptions.ADDRESS), is(host));
		assertThat(testClient.getConfiguration().getInteger(RestOptions.PORT), is(99));
	}

	@Test
	public void testRemoteExecutionWithSavepoint() throws Exception {
		SavepointRestoreSettings restoreSettings = SavepointRestoreSettings.forPath("fakePath");
		JobID jobID = new JobID();

		TestExecutorServiceLoader testExecutorServiceLoader = new TestExecutorServiceLoader(jobID);
		RemoteStreamEnvironment env = new RemoteStreamEnvironment(
				testExecutorServiceLoader,
				"fakeHost",
				1,
				null,
				new String[]{},
				null,
				restoreSettings);

		env.fromElements(1).map(x -> x * 2);

		JobExecutionResult actualResult = env.execute("fakeJobName");
		assertThat(actualResult.getJobID(), is(jobID));
		assertThat(
				testExecutorServiceLoader.getActualSavepointRestoreSettings(), is(restoreSettings));
	}

	private static final class TestExecutorServiceLoader implements PipelineExecutorServiceLoader {

		private final JobID jobID;

		private TestClusterClient clusterClient;
		private SavepointRestoreSettings actualSavepointRestoreSettings;

		TestExecutorServiceLoader(final JobID jobID) {
			this.jobID = checkNotNull(jobID);
		}

		public TestClusterClient getCreatedClusterClient() {
			return clusterClient;
		}

		public SavepointRestoreSettings getActualSavepointRestoreSettings() {
			return actualSavepointRestoreSettings;
		}

		@Override
		public PipelineExecutorFactory getExecutorFactory(@Nonnull Configuration configuration) {
			return new PipelineExecutorFactory() {

				@Override
				public String getName() {
					return "my-name";
				}

				@Override
				public boolean isCompatibleWith(@Nonnull Configuration configuration) {
					return true;
				}

				@Override
				public PipelineExecutor getExecutor(@Nonnull Configuration configuration) {
					return (pipeline, config) -> {
						assertTrue(pipeline instanceof StreamGraph);

						actualSavepointRestoreSettings =
								SavepointRestoreSettings.fromConfiguration(config);

						clusterClient = new TestClusterClient(configuration, jobID);

						return CompletableFuture.completedFuture(
								new ClusterClientJobClientAdapter<>(() -> clusterClient, jobID));
					};
				}
			};
		}

		@Override
		public Stream<String> getExecutorNames() {
			throw new UnsupportedOperationException("not implemented");
		}
	}

	private static final class TestClusterClient implements ClusterClient<Object> {

		private final Configuration configuration;
		private final JobID jobId;

		public TestClusterClient(Configuration config, JobID jobId) {
			this.configuration = config;
			this.jobId = jobId;
		}

		public Configuration getConfiguration() {
			return configuration;
		}

		@Override
		public CompletableFuture<JobResult> requestJobResult(@Nonnull JobID jobId) {
			assertThat(jobId, is(this.jobId));
			JobResult jobResult = new Builder()
					.jobId(this.jobId)
					.netRuntime(0)
					.applicationStatus(ApplicationStatus.SUCCEEDED)
					.build();
			return CompletableFuture.completedFuture(jobResult);
		}

		@Override
		public CompletableFuture<JobID> submitJob(@Nonnull JobGraph jobGraph) {
			return CompletableFuture.completedFuture(jobId);
		}

		@Override
		public void close() {

		}

		@Override
		public Object getClusterId() {
			return null;
		}

		@Override
		public Configuration getFlinkConfiguration() {
			return null;
		}

		@Override
		public void shutDownCluster() {

		}

		@Override
		public String getWebInterfaceURL() {
			return null;
		}

		@Override
		public CompletableFuture<Collection<JobStatusMessage>> listJobs() throws Exception {
			return null;
		}

		@Override
		public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath)
				throws FlinkException {
			return null;
		}

		@Override
		public CompletableFuture<JobStatus> getJobStatus(JobID jobId) {
			return null;
		}

		@Override
		public CompletableFuture<Map<String, Object>> getAccumulators(
				JobID jobID,
				ClassLoader loader) {
			return null;
		}

		@Override
		public CompletableFuture<Acknowledge> cancel(JobID jobId) {
			return null;
		}

		@Override
		public CompletableFuture<String> cancelWithSavepoint(
				JobID jobId,
				@Nullable String savepointDirectory) {
			return null;
		}

		@Override
		public CompletableFuture<String> stopWithSavepoint(
				JobID jobId,
				boolean advanceToEndOfEventTime,
				@Nullable String savepointDirectory) {
			return null;
		}

		@Override
		public CompletableFuture<String> triggerSavepoint(
				JobID jobId,
				@Nullable String savepointDirectory) {
			return null;
		}
	}

}
