/**
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
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.PlanExecutor;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.JobWithJars;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plandump.PlanJSONDumpGenerator;
import org.apache.flink.configuration.Configuration;

public class RemoteExecutor extends PlanExecutor {

	private final List<String> jarFiles;
	
	private final InetSocketAddress address;
	
	
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
		this.address = inet;
	}

	
	public static InetSocketAddress getInetFromHostport(String hostport) {
		// from http://stackoverflow.com/questions/2345063/java-common-way-to-validate-and-convert-hostport-to-inetsocketaddress
		URI uri;
		try {
			uri = new URI("my://" + hostport);
		} catch (URISyntaxException e) {
			throw new RuntimeException("Could not identify hostname and port", e);
		}
		String host = uri.getHost();
		int port = uri.getPort();
		if (host == null || port == -1) {
			throw new RuntimeException("Could not identify hostname and port");
		}
		return new InetSocketAddress(host, port);
	}

	@Override
	public JobExecutionResult executePlan(Plan plan) throws Exception {
		JobWithJars p = new JobWithJars(plan, this.jarFiles);
		
		Client c = new Client(this.address, new Configuration(), p.getUserCodeClassLoader());
		return c.run(p, -1, true);
	}
	
	public JobExecutionResult executePlanWithJars(JobWithJars p) throws Exception {
		Client c = new Client(this.address, new Configuration(), p.getUserCodeClassLoader());
		return c.run(p, -1, true);
	}

	public JobExecutionResult executeJar(String jarPath, String assemblerClass, String[] args) throws Exception {
		File jarFile = new File(jarPath);
		PackagedProgram program = new PackagedProgram(jarFile, assemblerClass, args);
		
		Client c = new Client(this.address, new Configuration(), program.getUserCodeClassLoader());
		return c.run(program.getPlanWithJars(), -1, true);
	}

	@Override
	public String getOptimizerPlanAsJSON(Plan plan) throws Exception {
		JobWithJars p = new JobWithJars(plan, this.jarFiles);
		Client c = new Client(this.address, new Configuration(), p.getUserCodeClassLoader());
		
		OptimizedPlan op = c.getOptimizedPlan(p, -1);
		PlanJSONDumpGenerator jsonGen = new PlanJSONDumpGenerator();
		return jsonGen.getOptimizerPlanAsJSON(op);
	}
}
