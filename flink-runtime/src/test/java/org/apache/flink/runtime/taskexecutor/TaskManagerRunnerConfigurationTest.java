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

package org.apache.flink.runtime.taskexecutor;

import net.jcip.annotations.NotThreadSafe;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.IOUtils;
import org.junit.Rule;
import org.junit.Test;

import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.URI;

import static org.junit.Assert.*;

/**
 * Validates that the TaskManagerRunner startup properly obeys the configuration
 * values.
 *
 * NOTE: at least {@link #testDefaultFsParameterLoading()} should not be run in parallel to other
 * tests in the same JVM as it modifies a static (private) member of the {@link FileSystem} class
 * and verifies its content.
 */
@NotThreadSafe
public class TaskManagerRunnerConfigurationTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Test
	public void testUsePreconfiguredRpcService() throws Exception {
		final String TEST_HOST_NAME = "testhostname";

		Configuration config = new Configuration();
		config.setString(TaskManagerOptions.HOST, TEST_HOST_NAME);
		config.setString(JobManagerOptions.ADDRESS, "localhost");
		config.setInteger(JobManagerOptions.PORT, 7891);

		HighAvailabilityServices highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
			config,
			Executors.directExecutor(),
			HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);

		try {
			// auto port
			RpcService rpcService = TaskManagerRunner.createRpcService(config, highAvailabilityServices);
			assertTrue(rpcService.getPort() >= 0);
			// pre-defined host name
			assertEquals(TEST_HOST_NAME, rpcService.getAddress());

			// pre-defined port
			final int testPort = 22551;
			config.setString(TaskManagerOptions.RPC_PORT, String.valueOf(testPort));
			rpcService = TaskManagerRunner.createRpcService(config, highAvailabilityServices);
			assertEquals(testPort, rpcService.getPort());

			// port range
			config.setString(TaskManagerOptions.RPC_PORT, "8000-8001");
			rpcService = TaskManagerRunner.createRpcService(config, highAvailabilityServices);
			assertTrue(rpcService.getPort() >= 8000);
			assertTrue(rpcService.getPort() <= 8001);

			// invalid port
			try {
				config.setString(TaskManagerOptions.RPC_PORT, "-1");
				TaskManagerRunner.createRpcService(config, highAvailabilityServices);
				fail("should fail with an exception");
			}
			catch (IllegalArgumentException e) {
				// bam!
			}

			// invalid port
			try {
				config.setString(TaskManagerOptions.RPC_PORT, "100000");
				TaskManagerRunner.createRpcService(config, highAvailabilityServices);
				fail("should fail with an exception");
			}
			catch (IllegalArgumentException e) {
				// bam!
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		} finally {
			highAvailabilityServices.closeAndCleanupAllData();
		}
	}

	@Test
	public void testDefaultFsParameterLoading() throws Exception {
		try {
			final File tmpDir = temporaryFolder.newFolder();
			final File confFile = new File(tmpDir, GlobalConfiguration.FLINK_CONF_FILENAME);

			final URI defaultFS = new URI("otherFS", null, "localhost", 1234, null, null, null);

			final PrintWriter pw1 = new PrintWriter(confFile);
			pw1.println("fs.default-scheme: " + defaultFS);
			pw1.close();

			String[] args = new String[] {"--configDir", tmpDir.toString()};
			Configuration configuration = TaskManagerRunner.loadConfiguration(args);
			FileSystem.initialize(configuration);

			assertEquals(defaultFS, FileSystem.getDefaultFsUri());
		}
		finally {
			// reset FS settings
			FileSystem.initialize(new Configuration());
		}
	}

	@Test
	public void testCreateRpcService() throws Exception {
		ServerSocket server;
		String hostname = "localhost";

		try {
			InetAddress localhostAddress = InetAddress.getByName(hostname);
			server = new ServerSocket(0, 50, localhostAddress);
		} catch (IOException e) {
			// may happen in certain test setups, skip test.
			System.err.println("Skipping 'testCreateRpcService' test.");
			return;
		}

		// open a server port to allow the system to connect
		Configuration config = new Configuration();

		config.setString(JobManagerOptions.ADDRESS, hostname);
		config.setInteger(JobManagerOptions.PORT, server.getLocalPort());

		HighAvailabilityServices highAvailabilityServices = HighAvailabilityServicesUtils.createHighAvailabilityServices(
			config,
			Executors.directExecutor(),
			HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);

		try {
			assertNotNull(TaskManagerRunner.createRpcService(config, highAvailabilityServices).getAddress());
		}
		finally {
			highAvailabilityServices.closeAndCleanupAllData();
			IOUtils.closeQuietly(server);
		}
	}
}
