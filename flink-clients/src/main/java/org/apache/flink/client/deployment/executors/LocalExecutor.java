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
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.program.PerJobMiniClusterFactory;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;

import java.net.MalformedURLException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An {@link PipelineExecutor} for executing a {@link Pipeline} locally.
 */
@Internal
public class LocalExecutor implements PipelineExecutor {

	public static final String NAME = "local";

	private final Configuration configuration;
	private final Function<MiniClusterConfiguration, MiniCluster> miniClusterFactory;

	public static LocalExecutor create(Configuration configuration) {
		return new LocalExecutor(configuration, MiniCluster::new);
	}

	public static LocalExecutor createWithFactory(
			Configuration configuration, Function<MiniClusterConfiguration, MiniCluster> miniClusterFactory) {
		return new LocalExecutor(configuration, miniClusterFactory);
	}

	private LocalExecutor(Configuration configuration, Function<MiniClusterConfiguration, MiniCluster> miniClusterFactory) {
		this.configuration = configuration;
		this.miniClusterFactory = miniClusterFactory;
	}

	@Override
	public CompletableFuture<JobClient> execute(Pipeline pipeline, Configuration configuration) throws Exception {
		checkNotNull(pipeline);
		checkNotNull(configuration);

		Configuration effectiveConfig = new Configuration();
		effectiveConfig.addAll(this.configuration);
		effectiveConfig.addAll(configuration);

		// we only support attached execution with the local executor.
		checkState(configuration.getBoolean(DeploymentOptions.ATTACHED));

		final JobGraph jobGraph = getJobGraph(pipeline, effectiveConfig);

		return PerJobMiniClusterFactory.createWithFactory(effectiveConfig, miniClusterFactory).submitJob(jobGraph);
	}

	private JobGraph getJobGraph(Pipeline pipeline, Configuration configuration) throws MalformedURLException {
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

		return PipelineExecutorUtils.getJobGraph(pipeline, configuration);
	}
}
