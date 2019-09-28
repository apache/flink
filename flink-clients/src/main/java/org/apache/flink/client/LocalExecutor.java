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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.JobExecutorService;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A PlanExecutor that runs Flink programs on a local embedded Flink runtime instance.
 *
 * <p>By simply calling the {@link #executePlan(org.apache.flink.api.common.Plan)} method,
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

	private JobExecutorService createJobExecutorService(Configuration configuration) throws Exception {
		if (!configuration.contains(RestOptions.BIND_PORT)) {
			configuration.setString(RestOptions.BIND_PORT, "0");
		}

		final MiniClusterConfiguration miniClusterConfiguration = new MiniClusterConfiguration.Builder()
			.setConfiguration(configuration)
			.setNumTaskManagers(
				configuration.getInteger(
					ConfigConstants.LOCAL_NUMBER_TASK_MANAGER,
					ConfigConstants.DEFAULT_LOCAL_NUMBER_TASK_MANAGER))
			.setRpcServiceSharing(RpcServiceSharing.SHARED)
			.setNumSlotsPerTaskManager(
				configuration.getInteger(
					TaskManagerOptions.NUM_TASK_SLOTS, 1))
			.build();

		final MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration);
		miniCluster.start();

		configuration.setInteger(RestOptions.PORT, miniCluster.getRestAddress().get().getPort());

		return miniCluster;
	}

	/**
	 * Executes the given program on a local runtime and waits for the job to finish.
	 *
	 * <p>If the executor has not been started before, this starts the executor and shuts it down
	 * after the job finished. If the job runs in session mode, the executor is kept alive until
	 * no more references to the executor exist.</p>
	 *
	 * @param plan The plan of the program to execute.
	 * @return The net runtime of the program, in milliseconds.
	 *
	 * @throws Exception Thrown, if either the startup of the local execution context, or the execution
	 *                   caused an exception.
	 */
	@Override
	public JobExecutionResult executePlan(Plan plan) throws Exception {
		checkNotNull(plan);

		final Configuration jobExecutorServiceConfiguration = configureExecution(plan);

		try (final JobExecutorService executorService = createJobExecutorService(jobExecutorServiceConfiguration)) {

			Optimizer pc = new Optimizer(new DataStatistics(), jobExecutorServiceConfiguration);
			OptimizedPlan op = pc.compile(plan);

			JobGraphGenerator jgg = new JobGraphGenerator(jobExecutorServiceConfiguration);
			JobGraph jobGraph = jgg.compileJobGraph(op, plan.getJobId());

			return executorService.executeJobBlocking(jobGraph);
		}
	}

	private Configuration configureExecution(final Plan plan) {
		final Configuration executorConfiguration = createExecutorServiceConfig(plan);
		setPlanParallelism(plan, executorConfiguration);
		return executorConfiguration;
	}

	private Configuration createExecutorServiceConfig(final Plan plan) {
		final Configuration newConfiguration = new Configuration();
		newConfiguration.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, plan.getMaximumParallelism());
		newConfiguration.addAll(baseConfiguration);
		return newConfiguration;
	}

	private void setPlanParallelism(final Plan plan, final Configuration executorServiceConfig) {
		// TODO: Set job's default parallelism to max number of slots
		final int slotsPerTaskManager = executorServiceConfig.getInteger(
				TaskManagerOptions.NUM_TASK_SLOTS, plan.getMaximumParallelism());
		final int numTaskManagers = executorServiceConfig.getInteger(
				ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);

		plan.setDefaultParallelism(slotsPerTaskManager * numTaskManagers);
	}
}
