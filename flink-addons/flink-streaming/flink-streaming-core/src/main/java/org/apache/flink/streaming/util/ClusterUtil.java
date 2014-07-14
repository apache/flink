/***********************************************************************************************************************
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **********************************************************************************************************************/

package org.apache.flink.streaming.util;

import java.net.InetSocketAddress;

import org.apache.flink.client.minicluster.NepheleMiniCluster;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;

public class ClusterUtil {

	/**
	 * Executes the given JobGraph locally, on a NepheleMiniCluster
	 * 
	 * @param jobGraph
	 *            jobGraph
	 * @param numberOfTaskTrackers
	 *            numberOfTaskTrackers
	 * @param memorySize
	 *            memorySize
	 */
	public static void runOnMiniCluster(JobGraph jobGraph, int numberOfTaskTrackers, long memorySize) {

		Configuration configuration = jobGraph.getJobConfiguration();

		NepheleMiniCluster exec = new NepheleMiniCluster();
		exec.setMemorySize(memorySize);
		exec.setNumTaskTracker(numberOfTaskTrackers);
		Client client = new Client(new InetSocketAddress("localhost", 6498), configuration);
		System.out.println("Running on mini cluster");
		try {
			exec.start();

			client.run(jobGraph, true);

			exec.stop();
		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	public static void runOnMiniCluster(JobGraph jobGraph, int numberOfTaskTrackers) {
		runOnMiniCluster(jobGraph, numberOfTaskTrackers, -1);
	}

	public static void runOnLocalCluster(JobGraph jobGraph, String IP, int port) {
		System.out.println("Running on local cluster");
		Configuration configuration = jobGraph.getJobConfiguration();

		Client client = new Client(new InetSocketAddress(IP, port), configuration);

		try {
			client.run(jobGraph, true);
		} catch (ProgramInvocationException e) {
			e.printStackTrace();
		}
	}

}
