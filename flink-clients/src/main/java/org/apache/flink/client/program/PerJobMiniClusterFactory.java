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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.MiniClusterJobClient;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
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
	public CompletableFuture<JobClient> submitJob(JobGraph jobGraph, ClassLoader userCodeClassloader) throws Exception {
		MiniClusterConfiguration miniClusterConfig = getMiniClusterConfig(jobGraph.getMaximumParallelism());
		MiniCluster miniCluster = miniClusterFactory.apply(miniClusterConfig);
		miniCluster.start();

		return miniCluster
			.submitJob(jobGraph)
			.thenApplyAsync(FunctionUtils.uncheckedFunction(submissionResult -> {
				org.apache.flink.client.ClientUtils.waitUntilJobInitializationFinished(
					() -> miniCluster.getJobStatus(submissionResult.getJobID()).get(),
					() -> miniCluster.requestJobResult(submissionResult.getJobID()).get(),
					userCodeClassloader);
				return submissionResult;
			}))
			.thenApply(result -> new MiniClusterJobClient(
					result.getJobID(),
					miniCluster,
					userCodeClassloader,
					MiniClusterJobClient.JobFinalizationBehavior.SHUTDOWN_CLUSTER))
			.whenComplete((ignored, throwable) -> {
				if (throwable != null) {
					// We failed to create the JobClient and must shutdown to ensure cleanup.
					shutDownCluster(miniCluster);
				}
			})
			.thenApply(Function.identity());
	}

	private MiniClusterConfiguration getMiniClusterConfig(int maximumParallelism) {
		Configuration configuration = new Configuration(this.configuration);

		if (!configuration.contains(RestOptions.BIND_PORT)) {
			configuration.setString(RestOptions.BIND_PORT, "0");
		}

		int numTaskManagers = configuration.getInteger(
			ConfigConstants.LOCAL_NUMBER_TASK_MANAGER,
			ConfigConstants.DEFAULT_LOCAL_NUMBER_TASK_MANAGER);

		int numSlotsPerTaskManager = configuration.getOptional(TaskManagerOptions.NUM_TASK_SLOTS)
			.orElseGet(() -> maximumParallelism > 0 ?
				MathUtils.divideRoundUp(maximumParallelism, numTaskManagers) :
				TaskManagerOptions.NUM_TASK_SLOTS.defaultValue());

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

}
