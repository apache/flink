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

package org.apache.flink.runtime.jobmanager;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.ServerSocket;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.ExecutionMode;
import org.apache.flink.runtime.client.AbstractJobResult;
import org.apache.flink.runtime.client.JobSubmissionResult;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmanager.tasks.NoOpInvokable;
import org.junit.Test;

/**
 * This test is intended to cover the basic functionality of the {@link JobManager}.
 */
public class JobManagerITCase {

	@Test
	public void testSingleVertexJob() {
		try {
			final AbstractJobVertex vertex = new AbstractJobVertex("Test Vertex");
			vertex.setParallelism(3);
			vertex.setInvokableClass(NoOpInvokable.class);
			
			final JobGraph jobGraph = new JobGraph("Test Job", vertex);
			
			JobManager jm = startJobManager(3);
			try {
				
				// we need to register the job at the library cache manager (with no libraries)
				LibraryCacheManager.register(jobGraph.getJobID(), new String[0]);
				
				JobSubmissionResult result = jm.submitJob(jobGraph);
				
				assertEquals(AbstractJobResult.ReturnCode.SUCCESS, result.getReturnCode());
				
				// monitor the execution
				ExecutionGraph eg = jm.getCurrentJobs().get(jobGraph.getJobID());
				
				if (eg != null) {
					
					long deadline = System.currentTimeMillis() + 60*1000;
					boolean success = false;
					
					while (System.currentTimeMillis() < deadline) {
						JobStatus state = eg.getState();
						if (state == JobStatus.FINISHED) {
							success = true;
							break;
						}
						
						else if (state == JobStatus.FAILED || state == JobStatus.CANCELED) {
							break;
						}
						else {
							Thread.sleep(200);
						}
					}
					
					assertTrue("The job did not finish successfully.", success);
				}
				else {
					// already done, that was fast;
				}
			}
			finally {
				jm.shutdown();
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final JobManager startJobManager(int numSlots) throws Exception {
		Configuration cfg = new Configuration();
		cfg.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
		cfg.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, getAvailablePort());
		cfg.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 10);
		cfg.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlots);
		
		GlobalConfiguration.includeConfiguration(cfg);
		
		JobManager jm = new JobManager(ExecutionMode.LOCAL);
		return jm;
	}
	
	private static int getAvailablePort() throws IOException {
		for (int i = 0; i < 50; i++) {
			ServerSocket serverSocket = null;
			try {
				serverSocket = new ServerSocket(0);
				int port = serverSocket.getLocalPort();
				if (port != 0) {
					return port;
				}
			} finally {
				serverSocket.close();
			}
		}
		
		throw new IOException("could not find free port");
	}
}
