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

package org.apache.flink.runtime.jobgraph;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.ServerSocket;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.ExecutionMode;
import org.apache.flink.runtime.jobmanager.JobManager;

public class JobManagerTestUtils {

	public static final JobManager startJobManager(int numSlots) throws Exception {
		return startJobManager(1, numSlots);
	}
	
	public static final JobManager startJobManager(int numTaskManagers, int numSlotsPerTaskManager) throws Exception {
		Configuration cfg = new Configuration();
		cfg.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
		cfg.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, getAvailablePort());
		cfg.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 10);
		cfg.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, numSlotsPerTaskManager);
		cfg.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, numTaskManagers);
		
		GlobalConfiguration.includeConfiguration(cfg);
		
		JobManager jm = new JobManager(ExecutionMode.LOCAL);
		
		// we need to wait until the taskmanager is registered
		// max time is 5 seconds
		long deadline = System.currentTimeMillis() + 5000;
		
		while (jm.getNumberOfSlotsAvailableToScheduler() < numTaskManagers * numSlotsPerTaskManager &&
				System.currentTimeMillis() < deadline)
		{
			Thread.sleep(10);
		}
		
		assertEquals(numTaskManagers * numSlotsPerTaskManager, jm.getNumberOfSlotsAvailableToScheduler());
		
		return jm;
	}
	
	public static int getAvailablePort() throws IOException {
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
	
	public static void waitForTaskThreadsToBeTerminated() throws InterruptedException {
		Thread[] threads = new Thread[Thread.activeCount()];
		Thread.enumerate(threads);
		
		for (Thread t : threads) {
			if (t == null) {
				continue;
			}
			ThreadGroup tg = t.getThreadGroup();
			if (tg != null && tg.getName() != null && tg.getName().equals("Task Threads")) {
				t.join();
			}
		}
	}
}
