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

package org.apache.flink.client.program;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * Starts a {@link MiniCluster} for every submitted job.
 * This class guarantees to tear down the MiniCluster in case of normal or exceptional job completion.
 * */
public final class PerJobMiniClusterFactory {

	private static final Logger LOG = LoggerFactory.getLogger(PerJobMiniClusterFactory.class);

	private final Configuration configuration;
	private final Function<? super MiniClusterConfiguration, ? extends MiniCluster> miniClusterFactory;

	public static PerJobMiniClusterFactory create() {
		return new PerJobMiniClusterFactory(new Configuration(), MiniCluster::new);
	}

	public static PerJobMiniClusterFactory createWithFactory(
			Configuration configuration,
			Function<? super MiniClusterConfiguration, ? extends MiniCluster> miniClusterFactory) {
		return new PerJobMiniClusterFactory(configuration, miniClusterFactory);
	}

	private PerJobMiniClusterFactory(
			Configuration configuration,
			Function<? super MiniClusterConfiguration, ? extends MiniCluster> miniClusterFactory) {
		this.configuration = configuration;
		this.miniClusterFactory = miniClusterFactory;
	}

	/**
	 * Starts a {@link MiniCluster} and submits a job.
	 */
	public CompletableFuture<? extends JobClient> submitJob(JobGraph jobGraph) throws Exception {
		MiniClusterConfiguration miniClusterConfig = getMiniClusterConfig(jobGraph.getMaximumParallelism());
		MiniCluster miniCluster = miniClusterFactory.apply(miniClusterConfig);
		miniCluster.start();

		return miniCluster
			.submitJob(jobGraph)
			.thenApply(result -> new PerJobMiniClusterJobClient(result.getJobID(), miniCluster))
			.whenComplete((ignored, throwable) -> {
				if (throwable != null) {
					// We failed to create the JobClient and must shutdown to ensure cleanup.
					shutDownCluster(miniCluster);
				}
			});
	}

	private MiniClusterConfiguration getMiniClusterConfig(int maximumParallelism) {
		Configuration configuration = new Configuration(this.configuration);

		if (!configuration.contains(RestOptions.BIND_PORT)) {
			configuration.setString(RestOptions.BIND_PORT, "0");
		}

		int numTaskManagers = configuration.getInteger(
			ConfigConstants.LOCAL_NUMBER_TASK_MANAGER,
			ConfigConstants.DEFAULT_LOCAL_NUMBER_TASK_MANAGER);

		// we have to use the maximum parallelism as a default here, otherwise streaming pipelines would not run
		int numSlotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS, maximumParallelism);

		return new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumTaskManagers(numTaskManagers)
			.setRpcServiceSharing(RpcServiceSharing.SHARED)
			.setNumSlotsPerTaskManager(numSlotsPerTaskManager)
			.build();
	}

	private static void shutDownCluster(MiniCluster miniCluster) {
		miniCluster.closeAsync()
			.whenComplete((ignored, throwable) -> {
				if (throwable != null) {
					LOG.warn("Shutdown of MiniCluster failed.", throwable);
				}
			});
	}

	/**
	 * A {@link JobClient} for a {@link PerJobMiniClusterFactory}.
	 */
	private static final class PerJobMiniClusterJobClient implements JobClient {

		private final JobID jobID;
		private final MiniCluster miniCluster;
		private final CompletableFuture<JobResult> jobResultFuture;

		private PerJobMiniClusterJobClient(JobID jobID, MiniCluster miniCluster) {
			this.jobID = jobID;
			this.miniCluster = miniCluster;
			this.jobResultFuture = miniCluster
				.requestJobResult(jobID)
				// Make sure to shutdown the cluster when the job completes.
				.whenComplete((result, throwable) -> shutDownCluster(miniCluster));
		}

		@Override
		public JobID getJobID() {
			return jobID;
		}

		@Override
		public CompletableFuture<JobStatus> getJobStatus() {
			return miniCluster.getJobStatus(jobID);
		}

		@Override
		public CompletableFuture<Void> cancel() {
			return miniCluster.cancelJob(jobID).thenAccept(result -> {});
		}

		@Override
		public CompletableFuture<String> stopWithSavepoint(boolean advanceToEndOfEventTime, @Nullable String savepointDirectory) {
			return miniCluster.stopWithSavepoint(jobID, savepointDirectory, advanceToEndOfEventTime);
		}

		@Override
		public CompletableFuture<String> triggerSavepoint(@Nullable String savepointDirectory) {
			return miniCluster.triggerSavepoint(jobID, savepointDirectory, false);
		}

		@Override
		public CompletableFuture<Map<String, Object>> getAccumulators(ClassLoader classLoader) {
			return getJobExecutionResult(classLoader).thenApply(JobExecutionResult::getAllAccumulatorResults);
		}

		@Override
		public CompletableFuture<JobExecutionResult> getJobExecutionResult(ClassLoader classLoader) {
			return jobResultFuture.thenApply(result -> {
				try {
					return result.toJobExecutionResult(classLoader);
				} catch (Exception e) {
					throw new CompletionException("Failed to convert JobResult to JobExecutionResult.", e);
				}
			});
		}
	}
}
