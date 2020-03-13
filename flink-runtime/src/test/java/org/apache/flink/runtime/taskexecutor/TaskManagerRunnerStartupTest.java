/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLogger;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that check how the {@link TaskManagerRunner} behaves when encountering startup problems.
 */
public class TaskManagerRunnerStartupTest extends TestLogger {

	private static final String LOCAL_HOST = "localhost";

	private static final int TOTAL_FLINK_MEMORY_MB = 1024;

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	private final RpcService rpcService = createRpcService();

	private TestingHighAvailabilityServices highAvailabilityServices;

	@Before
	public void setupTest() {
		highAvailabilityServices = new TestingHighAvailabilityServices();
	}

	@After
	public void tearDownTest() throws Exception {
		highAvailabilityServices.closeAndCleanupAllData();
		highAvailabilityServices = null;
	}


	/**
	 * Tests that the TaskManagerRunner startup fails synchronously when the I/O
	 * directories are not writable.
	 */
	@Test
	public void testIODirectoryNotWritable() throws Exception {
		File nonWritable = tempFolder.newFolder();
		Assume.assumeTrue("Cannot create non-writable temporary file directory. Skipping test.",
			nonWritable.setWritable(false, false));

		try {
			Configuration cfg = createFlinkConfiguration();
			cfg.setString(CoreOptions.TMP_DIRS, nonWritable.getAbsolutePath());

			try {

				startTaskManager(
					cfg,
					rpcService,
					highAvailabilityServices);

				fail("Should fail synchronously with an IOException");
			} catch (IOException e) {
				// splendid!
			}
		} finally {
			// noinspection ResultOfMethodCallIgnored
			nonWritable.setWritable(true, false);
			try {
				FileUtils.deleteDirectory(nonWritable);
			} catch (IOException e) {
				// best effort
			}
		}
	}

	/**
	 * Tests that the TaskManagerRunner startup fails synchronously when the memory configuration is wrong.
	 */
	@Test(expected = IllegalConfigurationException.class)
	public void testMemoryConfigWrong() throws Exception {
		Configuration cfg = createFlinkConfiguration();

		// something invalid
		cfg.set(TaskManagerOptions.NETWORK_MEMORY_MIN, MemorySize.parse("100m"));
		cfg.set(TaskManagerOptions.NETWORK_MEMORY_MAX, MemorySize.parse("10m"));
		startTaskManager(
			cfg,
			rpcService,
			highAvailabilityServices);
	}

	/**
	 * Tests that the TaskManagerRunner startup fails if the network stack cannot be initialized.
	 */
	@Test
	public void testStartupWhenNetworkStackFailsToInitialize() throws Exception {
		final ServerSocket blocker = new ServerSocket(0, 50, InetAddress.getByName(LOCAL_HOST));

		try {
			final Configuration cfg = createFlinkConfiguration();
			cfg.setInteger(NettyShuffleEnvironmentOptions.DATA_PORT, blocker.getLocalPort());

			startTaskManager(
				cfg,
				rpcService,
				highAvailabilityServices);

			fail("Should throw IOException when the network stack cannot be initialized.");
		} catch (IOException e) {
			// ignored
		} finally {
			IOUtils.closeQuietly(blocker);
		}
	}

	//-----------------------------------------------------------------------------------------------

	private static Configuration createFlinkConfiguration() {
		return TaskExecutorResourceUtils.adjustForLocalExecution(new Configuration());
	}

	private static RpcService createRpcService() {
		final RpcService rpcService = mock(RpcService.class);
		when(rpcService.getAddress()).thenReturn(LOCAL_HOST);
		return rpcService;
	}

	private static void startTaskManager(
		Configuration configuration,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices
	) throws Exception {

		TaskManagerRunner.startTaskManager(
			configuration,
			ResourceID.generate(),
			rpcService,
			highAvailabilityServices,
			mock(HeartbeatServices.class),
			NoOpMetricRegistry.INSTANCE,
			mock(BlobCacheService.class),
			false,
			error -> {});
	}
}
