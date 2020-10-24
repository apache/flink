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
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.scope.ScopeFormats;
import org.apache.flink.runtime.metrics.util.TestingMetricRegistry;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcServiceResource;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.collection.IsIn.isIn;
import static org.hamcrest.core.Every.everyItem;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests that check how the {@link TaskManagerRunner} behaves when encountering startup problems.
 */
public class TaskManagerRunnerStartupTest extends TestLogger {

	private static final String LOCAL_HOST = "localhost";

	@ClassRule
	public static final TestingRpcServiceResource RPC_SERVICE_RESOURCE = new TestingRpcServiceResource();

	@Rule
	public final TemporaryFolder tempFolder = new TemporaryFolder();

	private final RpcService rpcService = RPC_SERVICE_RESOURCE.getTestingRpcService();

	private TestingHighAvailabilityServices highAvailabilityServices;

	@Before
	public void setupTest() {
		highAvailabilityServices = new TestingHighAvailabilityServices();
		highAvailabilityServices.setResourceManagerLeaderRetriever(new SettableLeaderRetrievalService());
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
			cfg.setString(TaskManagerOptions.BIND_HOST, LOCAL_HOST);

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

	/**
	 * Checks that all expected metrics are initialized.
	 */
	@Test
	public void testMetricInitialization() throws Exception {
		Configuration cfg = createFlinkConfiguration();

		List<String> registeredMetrics = new ArrayList<>();
		startTaskManager(
			cfg,
			rpcService,
			highAvailabilityServices,
			TestingMetricRegistry.builder()
				.setRegisterConsumer((metric, metricName, group) -> registeredMetrics.add(group.getMetricIdentifier(metricName)))
				.setScopeFormats(ScopeFormats.fromConfig(cfg))
				.build());

		// GC-related metrics are not checked since their existence depends on the JVM used
		Set<String> expectedTaskManagerMetricsWithoutTaskManagerId = Sets.newHashSet(
			".taskmanager..Status.JVM.ClassLoader.ClassesLoaded",
			".taskmanager..Status.JVM.ClassLoader.ClassesUnloaded",
			".taskmanager..Status.JVM.Memory.Heap.Used",
			".taskmanager..Status.JVM.Memory.Heap.Committed",
			".taskmanager..Status.JVM.Memory.Heap.Max",
			".taskmanager..Status.JVM.Memory.NonHeap.Used",
			".taskmanager..Status.JVM.Memory.NonHeap.Committed",
			".taskmanager..Status.JVM.Memory.NonHeap.Max",
			".taskmanager..Status.JVM.Memory.Direct.Count",
			".taskmanager..Status.JVM.Memory.Direct.MemoryUsed",
			".taskmanager..Status.JVM.Memory.Direct.TotalCapacity",
			".taskmanager..Status.JVM.Memory.Mapped.Count",
			".taskmanager..Status.JVM.Memory.Mapped.MemoryUsed",
			".taskmanager..Status.JVM.Memory.Mapped.TotalCapacity",
			".taskmanager..Status.Flink.Memory.Managed.Used",
			".taskmanager..Status.Flink.Memory.Managed.Total",
			".taskmanager..Status.JVM.Threads.Count",
			".taskmanager..Status.JVM.CPU.Load",
			".taskmanager..Status.JVM.CPU.Time",
			".taskmanager..Status.Network.TotalMemorySegments",
			".taskmanager..Status.Network.AvailableMemorySegments",
			".taskmanager..Status.Shuffle.Netty.TotalMemorySegments",
			".taskmanager..Status.Shuffle.Netty.TotalMemory",
			".taskmanager..Status.Shuffle.Netty.AvailableMemorySegments",
			".taskmanager..Status.Shuffle.Netty.AvailableMemory",
			".taskmanager..Status.Shuffle.Netty.UsedMemorySegments",
			".taskmanager..Status.Shuffle.Netty.UsedMemory"
		);

		Pattern pattern = Pattern.compile("\\.taskmanager\\.([^.]+)\\..*");
		Set<String> registeredMetricsWithoutTaskManagerId = registeredMetrics.stream()
			.map(pattern::matcher)
			.flatMap(matcher -> matcher.find() ? Stream.of(matcher.group(0).replaceAll(matcher.group(1), "")) : Stream.empty())
			.collect(Collectors.toSet());

		assertThat(expectedTaskManagerMetricsWithoutTaskManagerId, everyItem(isIn(registeredMetricsWithoutTaskManagerId)));
	}

	//-----------------------------------------------------------------------------------------------

	private static Configuration createFlinkConfiguration() {
		return TaskExecutorResourceUtils.adjustForLocalExecution(new Configuration());
	}

	private static void startTaskManager(
		Configuration configuration,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices
	) throws Exception {
		startTaskManager(
			configuration,
			rpcService,
			highAvailabilityServices,
			NoOpMetricRegistry.INSTANCE
		);
	}

	private static void startTaskManager(
		Configuration configuration,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		MetricRegistry metricRegistry
	) throws Exception {

		TaskManagerRunner.startTaskManager(
			configuration,
			ResourceID.generate(),
			rpcService,
			highAvailabilityServices,
			new TestingHeartbeatServices(),
			metricRegistry,
			new BlobCacheService(
				configuration,
				new VoidBlobStore(),
				null),
			false,
			ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES,
			error -> {});
	}
}
