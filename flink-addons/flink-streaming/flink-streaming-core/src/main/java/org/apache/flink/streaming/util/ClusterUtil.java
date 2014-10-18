/*
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
 */

package org.apache.flink.streaming.util;

import java.net.InetSocketAddress;

import org.apache.flink.client.minicluster.NepheleMiniCluster;
import org.apache.flink.client.program.Client;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterUtil {

	private static final Logger LOG = LoggerFactory.getLogger(ClusterUtil.class);
	public static final String CANNOT_EXECUTE_EMPTY_JOB = "Cannot execute empty job";

	/**
	 * Executes the given JobGraph locally, on a NepheleMiniCluster
	 * 
	 * @param jobGraph
	 *            jobGraph
	 * @param degreeOfPrallelism
	 *            numberOfTaskTrackers
	 * @param memorySize
	 *            memorySize
	 */
	public static void runOnMiniCluster(JobGraph jobGraph, int degreeOfPrallelism, long memorySize) throws Exception  {

		Configuration configuration = jobGraph.getJobConfiguration();

		NepheleMiniCluster exec = new NepheleMiniCluster();
		exec.setMemorySize(memorySize);
		exec.setNumTaskManager(1);
		exec.setTaskManagerNumSlots(degreeOfPrallelism);
		if (LOG.isInfoEnabled()) {
			LOG.info("Running on mini cluster");
		}

		try {
			exec.start();

			Client client = new Client(new InetSocketAddress("localhost",
					exec.getJobManagerRpcPort()), configuration, ClusterUtil.class.getClassLoader());
			client.run(jobGraph, true);
		} catch (ProgramInvocationException e) {
			if (e.getMessage().contains("GraphConversionException")) {
				throw new Exception(CANNOT_EXECUTE_EMPTY_JOB, e);
			} else {
				throw e;
			}
		} catch (Exception e) {
			throw e;
		} finally {
			try {
				exec.stop();
			} catch (Throwable t) {
			}
		}
	}

	public static void runOnMiniCluster(JobGraph jobGraph, int numberOfTaskTrackers) throws Exception {
		runOnMiniCluster(jobGraph, numberOfTaskTrackers, -1);
	}

}
