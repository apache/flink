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

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The RemoteExecutor is a {@link org.apache.flink.api.common.PlanExecutor} that takes the program
 * and ships it to a remote Flink cluster for execution.
 * 
 * The RemoteExecutor is pointed at the JobManager and gets the program and (if necessary) the
 * set of libraries that need to be shipped together with the program.
 * 
 * The RemoteExecutor is used in the {@link org.apache.flink.api.java.RemoteEnvironment} to
 * remotely execute program parts.
 */
public class RemoteExecutor extends PlanExecutor {
	
	private static final Logger LOG = LoggerFactory.getLogger(RemoteExecutor.class);

	private final List<String> jarFiles;
	private final Configuration configuration;
	
	public RemoteExecutor(String hostname, int port) {
		this(hostname, port, Collections.<String>emptyList());
	}
	
	public RemoteExecutor(String hostname, int port, String jarFile) {
		this(hostname, port, Collections.singletonList(jarFile));
	}
	
	public RemoteExecutor(String hostport, String jarFile) {
		this(getInetFromHostport(hostport), Collections.singletonList(jarFile));
	}
	
	public RemoteExecutor(String hostname, int port, List<String> jarFiles) {
		this(new InetSocketAddress(hostname, port), jarFiles);
	}

	public RemoteExecutor(InetSocketAddress inet, List<String> jarFiles) {
		this.jarFiles = jarFiles;
		configuration = new Configuration();

		configuration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, inet.getHostName());
		configuration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, inet.getPort());
	}

	@Override
	public JobExecutionResult executePlan(Plan plan) throws Exception {
		JobWithJars p = new JobWithJars(plan, this.jarFiles);
		return executePlanWithJars(p);
	}
	
	public JobExecutionResult executePlanWithJars(JobWithJars p) throws Exception {
		Client c = new Client(configuration, p.getUserCodeClassLoader(), -1);
		c.setPrintStatusDuringExecution(isPrintingStatusDuringExecution());
		
		JobSubmissionResult result = c.run(p, -1, true);
		if (result instanceof JobExecutionResult) {
			return (JobExecutionResult) result;
		} else {
			LOG.warn("The Client didn't return a JobExecutionResult");
			return new JobExecutionResult(result.getJobID(), -1, null);
		}
	}

	public JobExecutionResult executeJar(String jarPath, String assemblerClass, String... args) throws Exception {
		File jarFile = new File(jarPath);
		PackagedProgram program = new PackagedProgram(jarFile, assemblerClass, args);
		
		Client c = new Client(configuration, program.getUserCodeClassLoader(), -1);
		c.setPrintStatusDuringExecution(isPrintingStatusDuringExecution());
		
		JobSubmissionResult result = c.run(program.getPlanWithJars(), -1, true);
		if(result instanceof JobExecutionResult) {
			return (JobExecutionResult) result;
		} else {
			LOG.warn("The Client didn't return a JobExecutionResult");
			return new JobExecutionResult(result.getJobID(), -1, null);
		}
	}

	@Override
	public String getOptimizerPlanAsJSON(Plan plan) throws Exception {
		JobWithJars p = new JobWithJars(plan, this.jarFiles);
		Client c = new Client(configuration, p.getUserCodeClassLoader(), -1);
		
		OptimizedPlan op = (OptimizedPlan) c.getOptimizedPlan(p, -1);
		PlanJSONDumpGenerator jsonGen = new PlanJSONDumpGenerator();
		return jsonGen.getOptimizerPlanAsJSON(op);
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
