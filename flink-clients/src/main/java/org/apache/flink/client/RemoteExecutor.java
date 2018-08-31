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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.StandaloneClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
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

	private final Object lock = new Object();

	private final List<URL> jarFiles;

	private final List<URL> globalClasspaths;

	private final Configuration clientConfiguration;

	private ClusterClient<?> client;

	private int defaultParallelism = 1;

	public RemoteExecutor(String hostname, int port) {
		this(hostname, port, new Configuration(), Collections.<URL>emptyList(),
				Collections.<URL>emptyList());
	}

	public RemoteExecutor(String hostname, int port, URL jarFile) {
		this(hostname, port, new Configuration(), Collections.singletonList(jarFile),
				Collections.<URL>emptyList());
	}

	public RemoteExecutor(String hostport, URL jarFile) {
		this(ClientUtils.parseHostPortAddress(hostport), new Configuration(), Collections.singletonList(jarFile),
				Collections.<URL>emptyList());
	}

	public RemoteExecutor(String hostname, int port, List<URL> jarFiles) {
		this(new InetSocketAddress(hostname, port), new Configuration(), jarFiles,
				Collections.<URL>emptyList());
	}

	public RemoteExecutor(String hostname, int port, Configuration clientConfiguration) {
		this(hostname, port, clientConfiguration, Collections.<URL>emptyList(),
				Collections.<URL>emptyList());
	}

	public RemoteExecutor(String hostname, int port, Configuration clientConfiguration, URL jarFile) {
		this(hostname, port, clientConfiguration, Collections.singletonList(jarFile),
				Collections.<URL>emptyList());
	}

	public RemoteExecutor(String hostport, Configuration clientConfiguration, URL jarFile) {
		this(ClientUtils.parseHostPortAddress(hostport), clientConfiguration,
				Collections.singletonList(jarFile), Collections.<URL>emptyList());
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
	//  Startup & Shutdown
	// ------------------------------------------------------------------------

	@Override
	public void start() throws Exception {
		synchronized (lock) {
			if (client == null) {
				if (CoreOptions.LEGACY_MODE.equals(clientConfiguration.getString(CoreOptions.MODE))) {
					client = new StandaloneClusterClient(clientConfiguration);
				} else {
					client = new RestClusterClient<>(clientConfiguration, "RemoteExecutor");
				}
				client.setPrintStatusDuringExecution(isPrintingStatusDuringExecution());
			}
			else {
				throw new IllegalStateException("The remote executor was already started.");
			}
		}
	}

	@Override
	public void stop() throws Exception {
		synchronized (lock) {
			if (client != null) {
				client.shutdown();
				client = null;
			}
		}
	}

	@Override
	public boolean isRunning() {
		return client != null;
	}

	// ------------------------------------------------------------------------
	//  Executing programs
	// ------------------------------------------------------------------------

	@Override
	public JobExecutionResult executePlan(Plan plan) throws Exception {
		if (plan == null) {
			throw new IllegalArgumentException("The plan may not be null.");
		}

		JobWithJars p = new JobWithJars(plan, this.jarFiles, this.globalClasspaths);
		return executePlanWithJars(p);
	}

	public JobExecutionResult executePlanWithJars(JobWithJars program) throws Exception {
		if (program == null) {
			throw new IllegalArgumentException("The job may not be null.");
		}

		synchronized (this.lock) {
			// check if we start a session dedicated for this execution
			final boolean shutDownAtEnd;

			if (client == null) {
				shutDownAtEnd = true;
				// start the executor for us
				start();
			}
			else {
				// we use the existing session
				shutDownAtEnd = false;
			}

			try {
				return client.run(program, defaultParallelism).getJobExecutionResult();
			}
			finally {
				if (shutDownAtEnd) {
					stop();
				}
			}
		}
	}

	@Override
	public String getOptimizerPlanAsJSON(Plan plan) throws Exception {
		Optimizer opt = new Optimizer(new DataStatistics(), new DefaultCostEstimator(), new Configuration());
		OptimizedPlan optPlan = opt.compile(plan);
		return new PlanJSONDumpGenerator().getOptimizerPlanAsJSON(optPlan);
	}

	@Override
	public void endSession(JobID jobID) throws Exception {
		if (jobID == null) {
			throw new NullPointerException("The supplied jobID must not be null.");
		}

		synchronized (this.lock) {
			// check if we start a session dedicated for this execution
			final boolean shutDownAtEnd;

			if (client == null) {
				shutDownAtEnd = true;
				// start the executor for us
				start();
			}
			else {
				// we use the existing session
				shutDownAtEnd = false;
			}

			try {
				client.endSession(jobID);
			}
			finally {
				if (shutDownAtEnd) {
					stop();
				}
			}
		}
	}
}
