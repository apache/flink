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

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.JobExecutorService;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;

import java.net.URL;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A PlanExecutor that runs Flink programs on a local embedded Flink runtime instance.
 *
 * <p>By simply calling the {@link #executePlan(Pipeline, List, List)} method,
 * this executor still start up and shut down again immediately after the program finished.</p>
 *
 * <p>To use this executor to execute many dataflow programs that constitute one job together,
 * then this executor needs to be explicitly started, to keep running across several executions.</p>
 */
public class LocalExecutor extends PlanExecutor {

	/** Custom user configuration for the execution. */
	private final Configuration baseConfiguration;

	public LocalExecutor() {
		this(new Configuration());
	}

	public LocalExecutor(Configuration conf) {
		this.baseConfiguration = checkNotNull(conf);
	}

	private JobExecutorService createJobExecutorService(
			JobGraph jobGraph, Configuration configuration) throws Exception {
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

	@Override
	public JobExecutionResult executePlan(
			Pipeline pipeline,
			List<URL> jarFiles,
			List<URL> globalClasspaths) throws Exception {
		checkNotNull(pipeline);

		// This is a quirk in how LocalEnvironment used to work. It sets the default parallelism
		// to <num taskmanagers> * <num task slots>. Might be questionable but we keep the behaviour
		// for now.
		if (pipeline instanceof Plan) {
			Plan plan = (Plan) pipeline;
			final int slotsPerTaskManager = baseConfiguration.getInteger(
					TaskManagerOptions.NUM_TASK_SLOTS, plan.getMaximumParallelism());
			final int numTaskManagers = baseConfiguration.getInteger(
					ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);

			plan.setDefaultParallelism(slotsPerTaskManager * numTaskManagers);
		}

		JobGraph jobGraph = FlinkPipelineTranslationUtil.getJobGraph(pipeline,
				baseConfiguration,
				1);

		try (final JobExecutorService executorService = createJobExecutorService(jobGraph,
				baseConfiguration)) {
			return executorService.executeJobBlocking(jobGraph);
		}
	}
}
