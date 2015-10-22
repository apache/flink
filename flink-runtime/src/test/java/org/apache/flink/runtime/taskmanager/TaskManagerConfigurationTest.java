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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.junit.Test;

import scala.Tuple2;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;

import static org.junit.Assert.*;

/**
 * Validates that the TaskManager startup properly obeys the configuration
 * values.
 */
public class TaskManagerConfigurationTest {

	@Test
	public void testUsePreconfiguredNetworkInterface() {
		try {
			final String TEST_HOST_NAME = "testhostname";

			Configuration config = new Configuration();
			config.setString(ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, TEST_HOST_NAME);
			config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
			config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 7891);

			Tuple2<String, Object> address = TaskManager.selectNetworkInterfaceAndPort(config);

			// validate the configured test host name
			assertEquals(TEST_HOST_NAME, address._1());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testActorSystemPortConfig() {
		try {
			// config with pre-configured hostname to speed up tests (no interface selection)
			Configuration config = new Configuration();
			config.setString(ConfigConstants.TASK_MANAGER_HOSTNAME_KEY, "localhost");
			config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
			config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, 7891);

			// auto port
			assertEquals(0, TaskManager.selectNetworkInterfaceAndPort(config)._2());

			// pre-defined port
			final int testPort = 22551;
			config.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, testPort);
			assertEquals(testPort, TaskManager.selectNetworkInterfaceAndPort(config)._2());

			// invalid port
			try {
				config.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, -1);
				TaskManager.selectNetworkInterfaceAndPort(config);
				fail("should fail with an exception");
			}
			catch (IllegalConfigurationException e) {
				// bam!
			}

			// invalid port
			try {
				config.setInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, 100000);
				TaskManager.selectNetworkInterfaceAndPort(config);
				fail("should fail with an exception");
			}
			catch (IllegalConfigurationException e) {
				// bam!
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testNetworkInterfaceSelection() {
		ServerSocket server;
		String hostname = "localhost";

		try {
			InetAddress localhostAddress = InetAddress.getByName(hostname);
			server = new ServerSocket(0, 50, localhostAddress);
		}
		catch (UnknownHostException e) {
			// may happen if disconnected. skip test.
			System.err.println("Skipping 'testNetworkInterfaceSelection' test.");
			return;
		}
		catch (IOException e) {
			// may happen in certain test setups, skip test.
			System.err.println("Skipping 'testNetworkInterfaceSelection' test.");
			return;
		}

		try {
			// open a server port to allow the system to connect
			Configuration config = new Configuration();

			config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, hostname);
			config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, server.getLocalPort());

			assertNotNull(TaskManager.selectNetworkInterfaceAndPort(config)._1());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		finally {
			try {
				server.close();
			} catch (IOException e) {
				// ignore shutdown errors
			}
		}
	}

}
