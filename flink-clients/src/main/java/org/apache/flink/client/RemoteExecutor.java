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

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.optimizer.costs.DefaultCostEstimator;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.configuration.Configuration;

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
	
	private final List<String> jarFiles;

	private final Configuration clientConfiguration;

	private Client client;
	
	private int defaultParallelism = 1;
	
	
	public RemoteExecutor(String hostname, int port) {
		this(hostname, port, Collections.<String>emptyList(), new Configuration());
	}
	
	public RemoteExecutor(String hostname, int port, String jarFile) {
		this(hostname, port, Collections.singletonList(jarFile), new Configuration());
	}
	
	public RemoteExecutor(String hostport, String jarFile) {
		this(getInetFromHostport(hostport), Collections.singletonList(jarFile), new Configuration());
	}
	
	public RemoteExecutor(String hostname, int port, List<String> jarFiles) {
		this(new InetSocketAddress(hostname, port), jarFiles, new Configuration());
	}

	public RemoteExecutor(String hostname, int port, Configuration clientConfiguration) {
		this(hostname, port, Collections.<String>emptyList(), clientConfiguration);
	}

	public RemoteExecutor(String hostname, int port, String jarFile, Configuration clientConfiguration) {
		this(hostname, port, Collections.singletonList(jarFile), clientConfiguration);
	}

	public RemoteExecutor(String hostport, String jarFile, Configuration clientConfiguration) {
		this(getInetFromHostport(hostport), Collections.singletonList(jarFile), clientConfiguration);
	}

	public RemoteExecutor(String hostname, int port, List<String> jarFiles, Configuration clientConfiguration) {
		this(new InetSocketAddress(hostname, port), jarFiles, clientConfiguration);
	}

	public RemoteExecutor(InetSocketAddress inet, List<String> jarFiles, Configuration clientConfiguration) {
		this.jarFiles = jarFiles;
		this.clientConfiguration = clientConfiguration;

		clientConfiguration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, inet.getHostName());
		clientConfiguration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, inet.getPort());
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
				client = new Client(clientConfiguration);
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

		JobWithJars p = new JobWithJars(plan, this.jarFiles);
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
				return client.runBlocking(program, defaultParallelism);
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

	// --------------------------------------------------------------------------------------------
	//   Utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Utility method that converts a string of the form "host:port" into an {@link InetSocketAddress}.
	 * The returned InetSocketAddress may be unresolved!
	 * 
	 * @param hostport The "host:port" string.
	 * @return The converted InetSocketAddress.
	 */
	private static InetSocketAddress getInetFromHostport(String hostport) {
		// from http://stackoverflow.com/questions/2345063/java-common-way-to-validate-and-convert-hostport-to-inetsocketaddress
		URI uri;
		try {
			uri = new URI("my://" + hostport);
		} catch (URISyntaxException e) {
			throw new RuntimeException("Could not identify hostname and port in '" + hostport + "'.", e);
		}
		String host = uri.getHost();
		int port = uri.getPort();
		if (host == null || port == -1) {
			throw new RuntimeException("Could not identify hostname and port in '" + hostport + "'.");
		}
		return new InetSocketAddress(host, port);
	}
}
