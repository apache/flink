/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.client;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.client.program.Client;
import eu.stratosphere.client.program.JobWithJars;
import eu.stratosphere.client.program.PackagedProgram;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.client.JobExecutionResult;

public class RemoteExecutor implements PlanExecutor {

	private Client client;

	private List<String> jarFiles;

	public RemoteExecutor(InetSocketAddress inet, List<String> jarFiles) {
		this.client = new Client(inet, new Configuration());
		this.jarFiles = jarFiles;
	}
	
	public RemoteExecutor(String hostname, int port, List<String> jarFiles) {
		this(new InetSocketAddress(hostname, port), jarFiles);
	}

	public RemoteExecutor(String hostname, int port, String jarFile) {
		this(hostname, port, Collections.singletonList(jarFile));
	}
	
	public RemoteExecutor(String hostport, String jarFile) {
		this(getInetFromHostport(hostport), Collections.singletonList(jarFile));
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

	public JobExecutionResult executePlanWithJars(JobWithJars p) throws Exception {
		return this.client.run(p, true);
	}
	
	@Override
	public JobExecutionResult executePlan(Plan plan) throws Exception {
		JobWithJars p = new JobWithJars(plan, this.jarFiles);
		return this.client.run(p, true);
	}

	public JobExecutionResult executeJar(String jarPath, String assemblerClass, String[] args) throws Exception {
		File jarFile = new File(jarPath);
		PackagedProgram program = new PackagedProgram(jarFile, assemblerClass, args);
		return this.client.run(program.getPlanWithJars(), true);
	}
}
