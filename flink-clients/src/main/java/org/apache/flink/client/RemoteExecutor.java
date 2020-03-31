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
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.net.InetSocketAddress;
import java.net.URL;
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

	private final Configuration clientConfiguration;

	private int defaultParallelism = 1;

	public RemoteExecutor(String hostname, int port) {
		this(hostname, port, new Configuration());
	}

	public RemoteExecutor(String hostname, int port, Configuration clientConfiguration) {
		this(new InetSocketAddress(hostname, port), clientConfiguration);
	}

	public RemoteExecutor(InetSocketAddress inet, Configuration clientConfiguration) {
		this.clientConfiguration = clientConfiguration;

		clientConfiguration.setString(JobManagerOptions.ADDRESS, inet.getHostName());
		clientConfiguration.setInteger(JobManagerOptions.PORT, inet.getPort());
		clientConfiguration.setInteger(RestOptions.PORT, inet.getPort());
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	/**
	 * Sets the parallelism that will be used when neither the program does not define any
	 * parallelism at all.
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
	 * Gets the parallelism that will be used when neither the program does not define any
	 * parallelism at all.
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
	public JobExecutionResult executePlan(
			Pipeline plan,
			List<URL> jarFiles,
			List<URL> globalClasspaths) throws Exception {
		checkNotNull(plan);

		JobGraph jobGraph = FlinkPipelineTranslationUtil.getJobGraph(plan,
				clientConfiguration,
				getDefaultParallelism());

		jobGraph.addJars(jarFiles);
		jobGraph.setClasspaths(globalClasspaths);

		ClassLoader userCodeClassLoader = ClientUtils.buildUserCodeClassLoader(
				jarFiles,
				globalClasspaths,
				getClass().getClassLoader(),
				clientConfiguration);

		return executePlanWithJars(jobGraph, userCodeClassLoader);
	}

	private JobExecutionResult executePlanWithJars(JobGraph jobGraph, ClassLoader classLoader) throws Exception {
		checkNotNull(jobGraph);
		checkNotNull(classLoader);

		try (ClusterClient<?> client = new RestClusterClient<>(
				clientConfiguration,
				"RemoteExecutor")) {
			return ClientUtils.submitJobAndWaitForResult(client, jobGraph, classLoader).getJobExecutionResult();
		}
	}
}
