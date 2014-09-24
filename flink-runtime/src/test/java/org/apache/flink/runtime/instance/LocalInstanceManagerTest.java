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

package org.apache.flink.runtime.instance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.ServerSocket;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.ExecutionMode;
import org.apache.flink.runtime.ipc.RPC;
import org.apache.flink.runtime.ipc.RPC.Server;
import org.apache.flink.runtime.protocols.JobManagerProtocol;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManager;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LocalInstanceManagerTest {

	private Server jmServer;
	
	private int port;
	
	@Before
	public void startJobManagerServer() {
		try {
			this.port = getAvailablePort();
			this.jmServer = RPC.getServer(new MockRPC(), "localhost", this.port, 1);
			this.jmServer.start();
			
			Configuration cfg = new Configuration();
			
			cfg.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
			cfg.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, this.port);
			
			cfg.setLong(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 5);
			
			GlobalConfiguration.includeConfiguration(cfg);
		}
		catch (Throwable t) {
			t.printStackTrace();
			fail("Preparing the test (initializing the job manager server) failed.");
		}
	}
	
	@After
	public void stopJobManagerServer() {
		try {
			this.jmServer.stop();
			this.jmServer = null;
			this.port = 0;
		}
		catch (Throwable t) {}
	}
	
	@Test
	public void testCreateSingleTaskManager() {
		try {
			LocalInstanceManager li = new LocalInstanceManager(1);
			try {
	
				TaskManager[] tms = li.getTaskManagers();
	
				assertEquals(1, tms.length);
				assertEquals(ExecutionMode.LOCAL, tms[0].getExecutionMode());
				assertTrue(tms[0].getConnectionInfo().address().isLoopbackAddress());
			}
			finally {
				li.shutdown();
				TaskManager[] tms = li.getTaskManagers();
				assertEquals(0, tms.length);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testCreateMultipleTaskManagers() {
		try {
			LocalInstanceManager li = new LocalInstanceManager(2);
			
			try {
				TaskManager[] tms = li.getTaskManagers();
	
				assertEquals(2, tms.length);
				for (TaskManager tm : tms) {
					assertEquals(ExecutionMode.CLUSTER, tm.getExecutionMode());
					assertTrue(tm.getConnectionInfo().address().isLoopbackAddress());
				}
			}
			finally {
				li.shutdown();
				TaskManager[] tms = li.getTaskManagers();
				assertEquals(0, tms.length);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	private static final int getAvailablePort() throws IOException {
		ServerSocket serverSocket = null;
		for (int i = 0; i < 50; i++){
			try {
				serverSocket = new ServerSocket(0);
				int port = serverSocket.getLocalPort();
				if (port != 0) {
					return port;
				}
			}
			catch (IOException e) {}
			finally {
				if (serverSocket != null) {
					serverSocket.close();
				}
			}
		}
		
		throw new IOException("Could not find a free port.");
	}
	
	private static final class MockRPC implements JobManagerProtocol {

		@Override
		public boolean updateTaskExecutionState(TaskExecutionState taskExecutionState) {
			return false;
		}

		@Override
		public boolean sendHeartbeat(InstanceID taskManagerId) {
			return true;
		}

		@Override
		public InstanceID registerTaskManager(InstanceConnectionInfo instanceConnectionInfo, HardwareDescription hardwareDescription, int numberOfSlots) {
			return new InstanceID();
		}

		@Override
		public int getBlobServerPort() {
			return 0;
		}
	}
}
