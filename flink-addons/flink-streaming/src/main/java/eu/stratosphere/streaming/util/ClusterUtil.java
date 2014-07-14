/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/


package eu.stratosphere.streaming.util;

import java.net.InetSocketAddress;

import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.client.program.Client;
import eu.stratosphere.client.program.ProgramInvocationException;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;

public class ClusterUtil {

	/**
	 * Executes the given JobGraph locally, on a NepheleMiniCluster
	 * 
	 * @param jobGraph
	 */
	public static void runOnMiniCluster(JobGraph jobGraph) {
		System.out.println("Running on mini cluster");

		Configuration configuration = jobGraph.getJobConfiguration();

		NepheleMiniCluster exec = new NepheleMiniCluster();
		Client client = new Client(new InetSocketAddress("localhost", 6498), configuration);

		try {
			exec.start();

			client.run(jobGraph, true);

			exec.stop();
		} catch (Exception e) {
		}
	}

	public static void runOnLocalCluster(JobGraph jobGraph, String IP, int port) {
		System.out.println("Running on local cluster");
		Configuration configuration = jobGraph.getJobConfiguration();

		Client client = new Client(new InetSocketAddress(IP, port), configuration);

		try {
			client.run(jobGraph, true);
		} catch (ProgramInvocationException e) {
		}
	}

}
