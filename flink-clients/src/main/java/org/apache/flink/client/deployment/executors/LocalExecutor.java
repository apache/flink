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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.FlinkPipelineTranslationUtil;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An {@link PipelineExecutor} for executing a {@link Pipeline} locally.
 */
@Internal
public class LocalExecutor implements PipelineExecutor {

	public static final String NAME = "local";

	@Override
	public CompletableFuture<JobClient> execute(Pipeline pipeline, Configuration configuration) throws Exception {
		checkNotNull(pipeline);
		checkNotNull(configuration);

		// we only support attached execution with the local executor.
		checkState(configuration.getBoolean(DeploymentOptions.ATTACHED));

		final JobGraph jobGraph = getJobGraph(pipeline, configuration);
		final MiniCluster miniCluster = startMiniCluster(jobGraph, configuration);
		final MiniClusterClient clusterClient = new MiniClusterClient(configuration, miniCluster);

		CompletableFuture<JobID> jobIdFuture = clusterClient.submitJob(jobGraph);

		jobIdFuture
				.thenCompose(clusterClient::requestJobResult)
				.thenAccept((jobResult) -> clusterClient.shutDownCluster());

		return jobIdFuture.thenApply(jobID ->
				new ClusterClientJobClientAdapter<>(() -> clusterClient, jobID));
	}

	private JobGraph getJobGraph(Pipeline pipeline, Configuration configuration) {
		// This is a quirk in how LocalEnvironment used to work. It sets the default parallelism
		// to <num taskmanagers> * <num task slots>. Might be questionable but we keep the behaviour
		// for now.
		if (pipeline instanceof Plan) {
			Plan plan = (Plan) pipeline;
			final int slotsPerTaskManager = configuration.getInteger(
					TaskManagerOptions.NUM_TASK_SLOTS, plan.getMaximumParallelism());
			final int numTaskManagers = configuration.getInteger(
					ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);

			plan.setDefaultParallelism(slotsPerTaskManager * numTaskManagers);
		}

		return FlinkPipelineTranslationUtil.getJobGraph(pipeline, configuration, 1);
	}

	private MiniCluster startMiniCluster(final JobGraph jobGraph, final Configuration configuration) throws Exception {
		if (!configuration.contains(RestOptions.BIND_PORT)) {
			configuration.setString(RestOptions.BIND_PORT, "0");
		}

		int numTaskManagers = configuration.getInteger(
				ConfigConstants.LOCAL_NUMBER_TASK_MANAGER,
				ConfigConstants.DEFAULT_LOCAL_NUMBER_TASK_MANAGER);

		// we have to use the maximum parallelism as a default here, otherwise streaming
		// pipelines would not run
		int numSlotsPerTaskManager = configuration.getInteger(
				TaskManagerOptions.NUM_TASK_SLOTS,
				jobGraph.getMaximumParallelism());

		final MiniClusterConfiguration miniClusterConfiguration =
				new MiniClusterConfiguration.Builder()
						.setConfiguration(configuration)
						.setNumTaskManagers(numTaskManagers)
						.setRpcServiceSharing(RpcServiceSharing.SHARED)
						.setNumSlotsPerTaskManager(numSlotsPerTaskManager)
						.build();

		final MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration);
		miniCluster.start();

		configuration.setInteger(RestOptions.PORT, miniCluster.getRestAddress().get().getPort());

		return miniCluster;
	}

	private void shutdownMiniCluster(final MiniCluster miniCluster) {
		try {
			if (miniCluster != null) {
				miniCluster.close();
			}
		} catch (Exception e) {
			throw new CompletionException(e);
		}
	}
}
