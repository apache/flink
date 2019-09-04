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
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;

import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The RemoteExecutor is a {@link org.apache.flink.api.common.PlanExecutor} that takes the program
 * and ships it to a remote Flink cluster for execution.
 *
 * <p>The RemoteExecutor is pointed at the JobManager and gets the program and (if necessary) the
 * set of libraries that need to be shipped together with the program.</p>
 *
 * <p>The RemoteExecutor is used in the {@link org.apache.flink.api.java.RemoteEnvironment} to
 * remotely execute program parts.</p>
 */
public class RemoteExecutor extends PlanExecutor {

	private final List<URL> jarFiles;

	private final List<URL> globalClasspaths;

	private final Configuration clientConfiguration;

	private int defaultParallelism = 1;

	public RemoteExecutor(String hostname, int port) {
		this(hostname, port, new Configuration(), Collections.<URL>emptyList(),
				Collections.<URL>emptyList());
	}

	public RemoteExecutor(String hostname, int port, Configuration clientConfiguration,
			List<URL> jarFiles, List<URL> globalClasspaths) {
		this(new InetSocketAddress(hostname, port), clientConfiguration, jarFiles, globalClasspaths);
	}

	public RemoteExecutor(InetSocketAddress inet, Configuration clientConfiguration,
			List<URL> jarFiles, List<URL> globalClasspaths) {
		this.clientConfiguration = clientConfiguration;
		this.jarFiles = jarFiles;
		this.globalClasspaths = globalClasspaths;

		clientConfiguration.setString(JobManagerOptions.ADDRESS, inet.getHostName());
		clientConfiguration.setInteger(JobManagerOptions.PORT, inet.getPort());
		clientConfiguration.setInteger(RestOptions.PORT, inet.getPort());
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	/**
	 * Sets the parallelism that will be used when neither the program does not define
	 * any parallelism at all.
	 *
	 * @param defaultParallelism The default parallelism for the executor.
	 */
	public void setDefaultParallelism(int defaultParallelism) {
		if (defaultParallelism < 1) {
			throw new IllegalArgumentException("The default parallelism must be at least one");
		}
		this.defaultParallelism = defaultParallelism;
	}

	/**
	 * Gets the parallelism that will be used when neither the program does not define
	 * any parallelism at all.
	 *
	 * @return The default parallelism for the executor.
	 */
	public int getDefaultParallelism() {
		return defaultParallelism;
	}

	// ------------------------------------------------------------------------
	//  Executing programs
	// ------------------------------------------------------------------------

	@Override
	public JobExecutionResult executePlan(Plan plan) throws Exception {
		checkNotNull(plan);

		JobWithJars p = new JobWithJars(plan, this.jarFiles, this.globalClasspaths);
		return executePlanWithJars(p);
	}

	private JobExecutionResult executePlanWithJars(JobWithJars program) throws Exception {
		checkNotNull(program);

		try (ClusterClient<?>  client = new RestClusterClient<>(clientConfiguration, "RemoteExecutor")) {
			return client.run(program, defaultParallelism).getJobExecutionResult();
		}
	}

	@Override
	public String getOptimizerPlanAsJSON(Plan plan) {
		Optimizer opt = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), new Configuration());
		OptimizedPlan optPlan = opt.compile(plan);
		return new PlanJSONDumpGenerator().getOptimizerPlanAsJSON(optPlan);
	}
}
