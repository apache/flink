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

package org.apache.flink.test.util;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobInitializationException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static org.junit.Assert.fail;

/**
 * Test utilities.
 */
public class TestUtils {

	public static JobExecutionResult tryExecute(StreamExecutionEnvironment see, String name) throws Exception {
		try {
			return see.execute(name);
		}
		catch (ProgramInvocationException | JobExecutionException root) {
			Throwable cause = root.getCause();

			// search for nested SuccessExceptions
			int depth = 0;
			while (!(cause instanceof SuccessException)) {
				if (cause == null || depth++ == 20) {
					root.printStackTrace();
					fail("Test failed: " + root.getMessage());
				}
				else {
					cause = cause.getCause();
				}
			}
		}

		return null;
	}

	public static void submitJobAndWaitForResult(ClusterClient<?> client, JobGraph jobGraph, ClassLoader classLoader) throws Exception {
		client.submitJob(jobGraph)
			.thenCompose(client::requestJobResult)
			.get()
			.toJobExecutionResult(classLoader);
	}

	public static void waitUntilJobInitializationFinished(JobID id, MiniClusterWithClientResource miniCluster, ClassLoader userCodeClassloader) throws
		JobInitializationException {
		ClusterClient<?> clusterClient = miniCluster.getClusterClient();
		ClientUtils.waitUntilJobInitializationFinished(() -> clusterClient.getJobStatus(id).get(), () -> clusterClient.requestJobResult(id).get(), userCodeClassloader);
	}
}
