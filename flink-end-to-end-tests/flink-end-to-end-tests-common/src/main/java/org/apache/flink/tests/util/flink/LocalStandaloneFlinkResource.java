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

package org.apache.flink.tests.util.flink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.queryablestate.FutureUtils;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.tests.util.FlinkDistribution;
import org.apache.flink.util.ConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Flink resource that start local standalone clusters.
 */
public class LocalStandaloneFlinkResource implements FlinkResource {

	private static final Logger LOG = LoggerFactory.getLogger(LocalStandaloneFlinkResource.class);

	private final FlinkDistribution distribution = new FlinkDistribution();

	@Override
	public void before() throws Exception {
		distribution.before();
	}

	@Override
	public void afterTestSuccess() {
		distribution.afterTestSuccess();
	}

	@Override
	public void afterTestFailure() {
		distribution.afterTestFailure();
	}

	@Override
	public void addConfiguration(final Configuration config) throws IOException {
		distribution.appendConfiguration(config);
	}

	@Override
	public ClusterController startCluster(int numTaskManagers) throws IOException {
		distribution.startJobManager();
		for (int x = 0; x < numTaskManagers; x++) {
			distribution.startTaskManager();
		}

		try (final RestClient restClient = new RestClient(RestClientConfiguration.fromConfiguration(new Configuration()), Executors.directExecutor())) {
			for (int retryAttempt = 0; retryAttempt < 30; retryAttempt++) {
				final CompletableFuture<TaskManagersInfo> localhost = restClient.sendRequest(
					"localhost",
					8081,
					TaskManagersHeaders.getInstance(),
					EmptyMessageParameters.getInstance(),
					EmptyRequestBody.getInstance());

				try {
					final TaskManagersInfo taskManagersInfo = localhost.get(1, TimeUnit.SECONDS);

					final int numRunningTaskManagers = taskManagersInfo.getTaskManagerInfos().size();
					if (numRunningTaskManagers == numTaskManagers) {
						return new StandaloneClusterController(distribution);
					} else {
						LOG.info("Waiting for task managers to come up. {}/{} are currently running.", numRunningTaskManagers, numTaskManagers);
					}
				} catch (InterruptedException e) {
					LOG.info("Waiting for dispatcher REST endpoint to come up...");
					Thread.currentThread().interrupt();
				} catch (TimeoutException | ExecutionException e) {
					// ExecutionExceptions may occur if leader election is still going on
					LOG.info("Waiting for dispatcher REST endpoint to come up...");
				}

				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		} catch (ConfigurationException e) {
			throw new RuntimeException("Could not create RestClient.", e);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		throw new RuntimeException("Cluster did not start in expected time-frame.");
	}

	private static class StandaloneClusterController implements ClusterController {

		private final FlinkDistribution distribution;

		StandaloneClusterController(FlinkDistribution distribution) {
			this.distribution = distribution;
		}

		@Override
		public JobController submitJob(JobSubmission job) throws IOException {
			final JobID run = distribution.submitJob(job);

			return new StandaloneJobController(run);
		}

		@Override
		public CompletableFuture<Void> closeAsync() {
			try {
				distribution.stopFlinkCluster();
				return CompletableFuture.completedFuture(null);
			} catch (IOException e) {
				return FutureUtils.getFailedFuture(e);
			}
		}
	}

	private static class StandaloneJobController implements JobController {
		private final JobID jobId;

		StandaloneJobController(JobID jobId) {
			this.jobId = jobId;
		}
	}
}
