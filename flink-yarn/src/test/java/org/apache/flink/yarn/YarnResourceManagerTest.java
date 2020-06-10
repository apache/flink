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

package org.apache.flink.yarn;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.io.network.partition.NoOpResourceManagerPartitionTracker;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerBuilder;
import org.apache.flink.runtime.resourcemanager.slotmanager.TestingSlotManagerBuilder;
import org.apache.flink.runtime.resourcemanager.utils.MockResourceManagerRuntimeServices;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.yarn.entrypoint.YarnWorkerResourceSpecFactory;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_APP_ID;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_CLIENT_HOME_DIR;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_CLIENT_SHIP_FILES;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_FLINK_CLASSPATH;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_HADOOP_USER_NAME;
import static org.apache.flink.yarn.YarnConfigKeys.FLINK_DIST_JAR;
import static org.apache.flink.yarn.YarnConfigKeys.FLINK_YARN_FILES;
import static org.apache.flink.yarn.YarnResourceManager.ERROR_MASSAGE_ON_SHUTDOWN_REQUEST;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the YARN resource manager component.
 */
public class YarnResourceManagerTest extends TestLogger {

	private static final Time TIMEOUT = Time.seconds(10L);

	private Configuration flinkConfig;

	private Map<String, String> env;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Before
	public void setup() throws IOException {
		testingFatalErrorHandler = new TestingFatalErrorHandler();

		flinkConfig = new Configuration();
		flinkConfig.set(TaskManagerOptions.TOTAL_FLINK_MEMORY, MemorySize.parse("1g"));

		File root = folder.getRoot();
		File home = new File(root, "home");
		boolean created = home.mkdir();
		assertTrue(created);

		env = new HashMap<>();
		env.put(ENV_APP_ID, "foo");
		env.put(ENV_CLIENT_HOME_DIR, home.getAbsolutePath());
		env.put(ENV_CLIENT_SHIP_FILES, "");
		env.put(ENV_FLINK_CLASSPATH, "");
		env.put(ENV_HADOOP_USER_NAME, "foo");
		env.put(FLINK_DIST_JAR, new YarnLocalResourceDescriptor(
			"flink.jar",
			new Path("/tmp/flink.jar"),
			0,
			System.currentTimeMillis(),
			LocalResourceVisibility.APPLICATION).toString());
		env.put(ApplicationConstants.Environment.PWD.key(), home.getAbsolutePath());

		BootstrapTools.writeConfiguration(flinkConfig, new File(home.getAbsolutePath(), FLINK_CONF_FILENAME));
	}

	@After
	public void teardown() throws Exception {
		if (testingFatalErrorHandler != null) {
			testingFatalErrorHandler.rethrowError();
		}

		if (env != null) {
			env.clear();
		}
	}

	static class TestingYarnResourceManager extends YarnResourceManager {
		final TestingYarnAMRMClientAsync testingYarnAMRMClientAsync;
		final TestingYarnNMClientAsync testingYarnNMClientAsync;

		TestingYarnResourceManager(
				RpcService rpcService,
				ResourceID resourceId,
				Configuration flinkConfig,
				Map<String, String> env,
				HighAvailabilityServices highAvailabilityServices,
				HeartbeatServices heartbeatServices,
				SlotManager slotManager,
				JobLeaderIdService jobLeaderIdService,
				ClusterInformation clusterInformation,
				FatalErrorHandler fatalErrorHandler,
				@Nullable String webInterfaceUrl,
				ResourceManagerMetricGroup resourceManagerMetricGroup) {
			super(
				rpcService,
				resourceId,
				flinkConfig,
				env,
				highAvailabilityServices,
				heartbeatServices,
				slotManager,
				NoOpResourceManagerPartitionTracker::get,
				jobLeaderIdService,
				clusterInformation,
				fatalErrorHandler,
				webInterfaceUrl,
				resourceManagerMetricGroup);
			this.testingYarnNMClientAsync = new TestingYarnNMClientAsync(this);
			this.testingYarnAMRMClientAsync = new TestingYarnAMRMClientAsync(this);
		}

		<T> CompletableFuture<T> runInMainThread(Callable<T> callable) {
			return callAsync(callable, TIMEOUT);
		}

		MainThreadExecutor getMainThreadExecutorForTesting() {
			return super.getMainThreadExecutor();
		}

		@Override
		protected AMRMClientAsync<AMRMClient.ContainerRequest> createAndStartResourceManagerClient(
				YarnConfiguration yarnConfiguration,
				int yarnHeartbeatIntervalMillis,
				@Nullable String webInterfaceUrl) {
			return testingYarnAMRMClientAsync;
		}

		@Override
		protected NMClientAsync createAndStartNodeManagerClient(YarnConfiguration yarnConfiguration) {
			return testingYarnNMClientAsync;
		}
	}

	class Context {

		// services
		final TestingRpcService rpcService;
		final MockResourceManagerRuntimeServices rmServices;

		// RM
		final ResourceID rmResourceID;
		final TestingYarnResourceManager resourceManager;

		final int dataPort = 1234;
		final HardwareDescription hardwareDescription = new HardwareDescription(1, 2L, 3L, 4L);

		// domain objects for test purposes
		final ResourceProfile resourceProfile1 = ResourceProfile.UNKNOWN;
		final WorkerResourceSpec workerResourceSpec;

		final Resource containerResource;

		public String taskHost = "host1";

		final TestingYarnNMClientAsync testingYarnNMClientAsync;

		final TestingYarnAMRMClientAsync testingYarnAMRMClientAsync;

		int containerIdx = 0;

		/**
		 * Create mock RM dependencies.
		 */
		Context() throws Exception {
			this(flinkConfig, null);
		}

		Context(Configuration configuration, @Nullable SlotManager slotManager) throws  Exception {

			workerResourceSpec = YarnWorkerResourceSpecFactory.INSTANCE.createDefaultWorkerResourceSpec(configuration);
			if (slotManager == null) {
				slotManager = SlotManagerBuilder.newBuilder()
					.setDefaultWorkerResourceSpec(workerResourceSpec)
					.build();
			}
			rpcService = new TestingRpcService();
			rmServices = new MockResourceManagerRuntimeServices(rpcService, TIMEOUT, slotManager);

			// resource manager
			rmResourceID = ResourceID.generate();
			resourceManager =
					new TestingYarnResourceManager(
							rpcService,
							rmResourceID,
							configuration,
							env,
							rmServices.highAvailabilityServices,
							rmServices.heartbeatServices,
							rmServices.slotManager,
							rmServices.jobLeaderIdService,
							new ClusterInformation("localhost", 1234),
							testingFatalErrorHandler,
							null,
							UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup());

			testingYarnAMRMClientAsync = resourceManager.testingYarnAMRMClientAsync;
			testingYarnNMClientAsync = resourceManager.testingYarnNMClientAsync;

			containerResource = resourceManager.getContainerResource(workerResourceSpec).get();
		}

		/**
		 * Start the resource manager and grant leadership to it.
		 */
		void startResourceManager() throws Exception {
			resourceManager.start();
			rmServices.grantLeadership();
		}

		/**
		 * Stop the Akka actor system.
		 */
		void stopResourceManager() throws Exception {
			rpcService.stopService().get();
		}

		/**
		 * A wrapper function for running test. Deal with setup and teardown logic
		 * in Context.
		 * @param testMethod the real test body.
		 */
		void runTest(RunnableWithException testMethod) throws Exception {
			startResourceManager();
			try {
				testMethod.run();
			} finally {
				stopResourceManager();
			}
		}

		void verifyFutureCompleted(CompletableFuture future) throws Exception {
			future.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
		}

		Container createTestingContainer() {
			return createTestingContainerWithResource(resourceManager.getContainerResource(workerResourceSpec).get());
		}

		Container createTestingContainerWithResource(Resource resource) {
			final ContainerId containerId = ContainerId.newInstance(
				ApplicationAttemptId.newInstance(
					ApplicationId.newInstance(System.currentTimeMillis(), 1),
					1),
				containerIdx++);
			final NodeId nodeId = NodeId.newInstance("container", 1234);
			return new TestingContainer(containerId, nodeId, resource, Priority.UNDEFINED);
		}

		ContainerStatus createTestingContainerStatus(final ContainerId containerId) {
			return new TestingContainerStatus(containerId, ContainerState.COMPLETE, "Test exit", -1);
		}
	}

	@Test
	public void testShutdownRequestCausesFatalError() throws Exception {
		new Context() {{
			runTest(() -> {
				resourceManager.onShutdownRequest();

				Throwable t = testingFatalErrorHandler.getErrorFuture().get(2000L, TimeUnit.MILLISECONDS);
				assertThat(ExceptionUtils.findThrowable(t, ResourceManagerException.class).isPresent(), is(true));
				assertThat(ExceptionUtils.findThrowableWithMessage(t, ERROR_MASSAGE_ON_SHUTDOWN_REQUEST).isPresent(), is(true));

				testingFatalErrorHandler.clearError();
			});
		}};
	}

	@Test
	public void testStopWorkerAfterRegistration() throws Exception {
		new Context() {{
			final List<CompletableFuture<Void>> addContainerRequestFutures = new ArrayList<>();
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);
			final CompletableFuture<Void> removeContainerRequestFuture = new CompletableFuture<>();
			final CompletableFuture<Void> releaseAssignedContainerFuture = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncFuture = new CompletableFuture<>();
			final CompletableFuture<Void> stopContainerAsyncFuture = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(ignored ->
				Collections.singletonList(Collections.singletonList(resourceManager.getContainerRequest(containerResource))));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer((ignored1, ignored2) ->
				addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(null));
			testingYarnAMRMClientAsync.setRemoveContainerRequestConsumer((ignored1, ignored2) -> removeContainerRequestFuture.complete(null));
			testingYarnAMRMClientAsync.setReleaseAssignedContainerConsumer((ignored1, ignored2) -> releaseAssignedContainerFuture.complete(null));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer((ignored1, ignored2, ignored3) -> startContainerAsyncFuture.complete(null));
			testingYarnNMClientAsync.setStopContainerAsyncConsumer((ignored1, ignored2, ignored3) -> stopContainerAsyncFuture.complete(null));

			runTest(() -> {
				// Request slot from SlotManager.
				registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);

				// Callback from YARN when container is allocated.
				Container testingContainer = createTestingContainer();

				resourceManager.onContainersAllocated(ImmutableList.of(testingContainer));
				verifyFutureCompleted(addContainerRequestFutures.get(0));
				verifyFutureCompleted(removeContainerRequestFuture);
				verifyFutureCompleted(startContainerAsyncFuture);

				// Remote task executor registers with YarnResourceManager.
				rpcService.registerGateway(taskHost, new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway());

				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

				final ResourceID taskManagerResourceId = new ResourceID(testingContainer.getId().toString());
				final ResourceProfile resourceProfile = ResourceProfile.newBuilder()
					.setCpuCores(10.0)
					.setTaskHeapMemoryMB(1)
					.setTaskOffHeapMemoryMB(1)
					.setManagedMemoryMB(1)
					.setNetworkMemoryMB(0)
					.build();
				final SlotReport slotReport = new SlotReport(
					new SlotStatus(new SlotID(taskManagerResourceId, 1), resourceProfile));

				TaskExecutorRegistration taskExecutorRegistration = new TaskExecutorRegistration(
					taskHost,
					taskManagerResourceId,
					dataPort,
					hardwareDescription,
					ResourceProfile.ZERO,
					ResourceProfile.ZERO);
				CompletableFuture<Integer> numberRegisteredSlotsFuture = rmGateway
					.registerTaskExecutor(taskExecutorRegistration, Time.seconds(10L))
					.thenCompose(
						(RegistrationResponse response) -> {
							assertThat(response, instanceOf(TaskExecutorRegistrationSuccess.class));
							final TaskExecutorRegistrationSuccess success = (TaskExecutorRegistrationSuccess) response;
							return rmGateway.sendSlotReport(
								taskManagerResourceId,
								success.getRegistrationId(),
								slotReport,
								Time.seconds(10L));
						})
					.handleAsync(
						(Acknowledge ignored, Throwable throwable) -> rmServices.slotManager.getNumberRegisteredSlots(),
						resourceManager.getMainThreadExecutorForTesting());

				final int numberRegisteredSlots = numberRegisteredSlotsFuture.get();

				assertEquals(1, numberRegisteredSlots);

				// Unregister all task executors and release all containers.
				CompletableFuture<?> unregisterAndReleaseFuture = resourceManager.runInMainThread(() -> {
					rmServices.slotManager.unregisterTaskManagersAndReleaseResources();
					return null;
				});

				unregisterAndReleaseFuture.get();

				verifyFutureCompleted(stopContainerAsyncFuture);
				verifyFutureCompleted(releaseAssignedContainerFuture);
				assertFalse(addContainerRequestFutures.get(1).isDone());
			});

			// It's now safe to access the SlotManager state since the ResourceManager has been stopped.
			assertThat(rmServices.slotManager.getNumberRegisteredSlots(), Matchers.equalTo(0));
			assertThat(resourceManager.getNumberOfRegisteredTaskManagers().get(), Matchers.equalTo(0));
		}};
	}

	@Test
	public void testStopWorkerBeforeRegistration() throws Exception {
		new Context() {{
			final List<CompletableFuture<Void>> addContainerRequestFutures = new ArrayList<>();
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);
			final CompletableFuture<Void> removeContainerRequestFuture = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncFuture = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(ignored ->
				Collections.singletonList(Collections.singletonList(resourceManager.getContainerRequest(containerResource))));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer((ignored1, ignored2) ->
				addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(null));
			testingYarnAMRMClientAsync.setRemoveContainerRequestConsumer((ignored1, ignored2) -> removeContainerRequestFuture.complete(null));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer((ignored1, ignored2, ignored3) -> startContainerAsyncFuture.complete(null));

			runTest(() -> {
				// Request slot from SlotManager.
				registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);

				// Callback from YARN when container is allocated.
				Container testingContainer = createTestingContainer();
				resourceManager.onContainersAllocated(ImmutableList.of(testingContainer));

				verifyFutureCompleted(addContainerRequestFutures.get(0));
				verifyFutureCompleted(removeContainerRequestFuture);
				verifyFutureCompleted(startContainerAsyncFuture);

				ContainerStatus testingContainerStatus = createTestingContainerStatus(testingContainer.getId());
				resourceManager.onContainersCompleted(ImmutableList.of(testingContainerStatus));

				verifyFutureCompleted(addContainerRequestFutures.get(1));
			});
		}};
	}

	/**
	 * Tests that application files are deleted when the YARN application master is de-registered.
	 */
	@Test
	public void testDeleteApplicationFiles() throws Exception {
		new Context() {{
			final File applicationDir = folder.newFolder(".flink");
			env.put(FLINK_YARN_FILES, applicationDir.getCanonicalPath());

			runTest(() -> {
				resourceManager.deregisterApplication(ApplicationStatus.SUCCEEDED, null);
				assertFalse("YARN application directory was not removed", Files.exists(applicationDir.toPath()));
			});
		}};
	}

	/**
	 * Tests that YarnResourceManager will not request more containers than needs during
	 * callback from Yarn when container is Completed.
	 */
	@Test
	public void testOnContainerCompleted() throws Exception {
		new Context() {{
			final List<CompletableFuture<Void>> addContainerRequestFutures = new ArrayList<>();
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);
			final CompletableFuture<Void> removeContainerRequestFuture = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncFuture = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(ignored ->
				Collections.singletonList(Collections.singletonList(resourceManager.getContainerRequest(containerResource))));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer((ignored1, ignored2) ->
				addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(null));
			testingYarnAMRMClientAsync.setRemoveContainerRequestConsumer((ignored1, ignored2) -> removeContainerRequestFuture.complete(null));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer((ignored1, ignored2, ignored3) -> startContainerAsyncFuture.complete(null));

			runTest(() -> {
				registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);

				// Callback from YARN when container is allocated.
				Container testingContainer = createTestingContainer();

				resourceManager.onContainersAllocated(ImmutableList.of(testingContainer));
				verifyFutureCompleted(addContainerRequestFutures.get(0));
				verifyFutureCompleted(removeContainerRequestFuture);
				verifyFutureCompleted(startContainerAsyncFuture);

				// Callback from YARN when container is Completed, pending request can not be fulfilled by pending
				// containers, need to request new container.
				ContainerStatus testingContainerStatus = createTestingContainerStatus(testingContainer.getId());

				resourceManager.onContainersCompleted(ImmutableList.of(testingContainerStatus));
				verifyFutureCompleted(addContainerRequestFutures.get(1));

				// Callback from YARN when container is Completed happened before global fail, pending request
				// slot is already fulfilled by pending containers, no need to request new container.
				resourceManager.onContainersCompleted(ImmutableList.of(testingContainerStatus));
				assertFalse(addContainerRequestFutures.get(2).isDone());
			});
		}};
	}

	@Test
	public void testOnStartContainerError() throws Exception {
		new Context() {{
			final List<CompletableFuture<Void>> addContainerRequestFutures = new ArrayList<>();
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);
			final CompletableFuture<Void> removeContainerRequestFuture = new CompletableFuture<>();
			final CompletableFuture<Void> releaseAssignedContainerFuture = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncFuture = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(ignored ->
				Collections.singletonList(Collections.singletonList(resourceManager.getContainerRequest(containerResource))));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer((ignored1, ignored2) ->
				addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(null));
			testingYarnAMRMClientAsync.setRemoveContainerRequestConsumer((ignored1, ignored2) -> removeContainerRequestFuture.complete(null));
			testingYarnAMRMClientAsync.setReleaseAssignedContainerConsumer((ignored1, ignored2) -> releaseAssignedContainerFuture.complete(null));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer((ignored1, ignored2, ignored3) -> startContainerAsyncFuture.complete(null));

			runTest(() -> {
				registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);
				Container testingContainer = createTestingContainer();

				resourceManager.onContainersAllocated(ImmutableList.of(testingContainer));
				verifyFutureCompleted(addContainerRequestFutures.get(0));
				verifyFutureCompleted(removeContainerRequestFuture);
				verifyFutureCompleted(startContainerAsyncFuture);

				resourceManager.onStartContainerError(testingContainer.getId(), new Exception("start error"));
				verifyFutureCompleted(releaseAssignedContainerFuture);
				verifyFutureCompleted(addContainerRequestFutures.get(1));
			});
		}};
	}

	@Test
	public void testStartWorkerVariousSpec_SameContainerResource() throws Exception{
		final WorkerResourceSpec workerResourceSpec1 = new WorkerResourceSpec.Builder()
			.setCpuCores(1)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(0)
			.setNetworkMemoryMB(100)
			.setManagedMemoryMB(100)
			.build();
		final WorkerResourceSpec workerResourceSpec2 = new WorkerResourceSpec.Builder()
			.setCpuCores(1)
			.setTaskHeapMemoryMB(99)
			.setTaskOffHeapMemoryMB(0)
			.setNetworkMemoryMB(100)
			.setManagedMemoryMB(100)
			.build();

		final SlotManager slotManager = new TestingSlotManagerBuilder()
			.setGetRequiredResourcesSupplier(() -> Collections.singletonMap(workerResourceSpec1, 1))
			.createSlotManager();

		new Context(flinkConfig, slotManager) {{
			final Resource containerResource = resourceManager.getContainerResource(workerResourceSpec1).get();

			final List<CompletableFuture<Void>> addContainerRequestFutures = new ArrayList<>();
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final String startCommand1 = TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (100L << 20);
			final String startCommand2 = TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (99L << 20);
			final CompletableFuture<Void> startContainerAsyncCommandFuture1 = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncCommandFuture2 = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(ignored ->
				Collections.singletonList(ImmutableList.of(
					resourceManager.getContainerRequest(resourceManager.getContainerResource(workerResourceSpec1).get()),
					resourceManager.getContainerRequest(resourceManager.getContainerResource(workerResourceSpec2).get()))));
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer((ignored1, ignored2) ->
				addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(null));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer((ignored1, context, ignored2) -> {
				if (containsStartCommand(context, startCommand1)) {
					startContainerAsyncCommandFuture1.complete(null);
				} else if (containsStartCommand(context, startCommand2)) {
					startContainerAsyncCommandFuture2.complete(null);
				}
			});

			runTest(() -> {
				// Make sure two worker resource spec will be normalized to the same container resource
				assertEquals(containerResource, resourceManager.getContainerResource(workerResourceSpec2).get());

				resourceManager.startNewWorker(workerResourceSpec1);
				resourceManager.startNewWorker(workerResourceSpec2);

				// Verify both containers requested
				verifyFutureCompleted(addContainerRequestFutures.get(0));
				verifyFutureCompleted(addContainerRequestFutures.get(1));

				// Mock that both containers are allocated
				Container container1 = createTestingContainerWithResource(containerResource);
				Container container2 = createTestingContainerWithResource(containerResource);
				resourceManager.onContainersAllocated(ImmutableList.of(container1, container2));

				// Verify workers with both spec are started.
				verifyFutureCompleted(startContainerAsyncCommandFuture1);
				verifyFutureCompleted(startContainerAsyncCommandFuture2);

				// Mock that one container is completed, while the worker is still pending
				ContainerStatus testingContainerStatus = createTestingContainerStatus(container1.getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));

				// Verify that only one more container is requested.
				verifyFutureCompleted(addContainerRequestFutures.get(2));
				assertFalse(addContainerRequestFutures.get(3).isDone());
			});
		}};
	}

	@Test
	public void testStartWorkerVariousSpec_DifferentContainerResource() throws Exception{
		final WorkerResourceSpec workerResourceSpec1 = new WorkerResourceSpec.Builder()
			.setCpuCores(1)
			.setTaskHeapMemoryMB(50)
			.setTaskOffHeapMemoryMB(50)
			.setNetworkMemoryMB(50)
			.setManagedMemoryMB(50)
			.build();
		final WorkerResourceSpec workerResourceSpec2 = new WorkerResourceSpec.Builder()
			.setCpuCores(2)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(100)
			.setNetworkMemoryMB(100)
			.setManagedMemoryMB(100)
			.build();

		final SlotManager slotManager = new TestingSlotManagerBuilder()
			.setGetRequiredResourcesSupplier(() -> Collections.singletonMap(workerResourceSpec1, 1))
			.createSlotManager();

		new Context(flinkConfig, slotManager) {{
			final Resource containerResource1 = resourceManager.getContainerResource(workerResourceSpec1).get();
			final Resource containerResource2 = resourceManager.getContainerResource(workerResourceSpec2).get();

			final List<CompletableFuture<Resource>> addContainerRequestFutures = new ArrayList<>();
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			addContainerRequestFutures.add(new CompletableFuture<>());
			final AtomicInteger addContainerRequestFuturesNumCompleted = new AtomicInteger(0);

			final String startCommand1 = TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (50L << 20);
			final String startCommand2 = TaskManagerOptions.TASK_HEAP_MEMORY.key() + "=" + (100L << 20);
			final CompletableFuture<Void> startContainerAsyncCommandFuture1 = new CompletableFuture<>();
			final CompletableFuture<Void> startContainerAsyncCommandFuture2 = new CompletableFuture<>();

			testingYarnAMRMClientAsync.setGetMatchingRequestsFunction(tuple -> {
				if (tuple.f2.equals(containerResource1)) {
					return Collections.singletonList(
						Collections.singletonList(resourceManager.getContainerRequest(resourceManager.getContainerResource(workerResourceSpec1).get())));
				} else if (tuple.f2.equals(containerResource2)) {
					return Collections.singletonList(
						Collections.singletonList(resourceManager.getContainerRequest(resourceManager.getContainerResource(workerResourceSpec2).get())));
				}
				return null;
			});
			testingYarnAMRMClientAsync.setAddContainerRequestConsumer((request, ignored) ->
				addContainerRequestFutures.get(addContainerRequestFuturesNumCompleted.getAndIncrement()).complete(request.getCapability()));
			testingYarnNMClientAsync.setStartContainerAsyncConsumer((ignored1, context, ignored3) -> {
				if (containsStartCommand(context, startCommand1)) {
					startContainerAsyncCommandFuture1.complete(null);
				} else if (containsStartCommand(context, startCommand2)) {
					startContainerAsyncCommandFuture2.complete(null);
				}
			});

			runTest(() -> {
				// Make sure two worker resource spec will be normalized to different container resources
				assertNotEquals(containerResource1, containerResource2);

				resourceManager.startNewWorker(workerResourceSpec1);
				resourceManager.startNewWorker(workerResourceSpec2);

				// Verify both containers requested
				verifyFutureCompleted(addContainerRequestFutures.get(0));
				verifyFutureCompleted(addContainerRequestFutures.get(1));

				// Mock that container 1 is allocated
				Container container1 = createTestingContainerWithResource(containerResource1);
				resourceManager.onContainersAllocated(Collections.singletonList(container1));

				// Verify that only worker with spec1 is started.
				verifyFutureCompleted(startContainerAsyncCommandFuture1);
				assertFalse(startContainerAsyncCommandFuture2.isDone());

				// Mock that container 1 is completed, while the worker is still pending
				ContainerStatus testingContainerStatus = createTestingContainerStatus(container1.getId());
				resourceManager.onContainersCompleted(Collections.singletonList(testingContainerStatus));

				// Verify that only container 1 is requested again
				verifyFutureCompleted(addContainerRequestFutures.get(2));
				assertThat(addContainerRequestFutures.get(2).get(), is(containerResource1));
				assertFalse(addContainerRequestFutures.get(3).isDone());
			});
		}};
	}

	private boolean containsStartCommand(ContainerLaunchContext containerLaunchContext, String command) {
		return containerLaunchContext.getCommands().stream().anyMatch(str -> str.contains(command));
	}

	private void registerSlotRequest(
			TestingYarnResourceManager resourceManager,
			MockResourceManagerRuntimeServices rmServices,
			ResourceProfile resourceProfile,
			String taskHost) throws ExecutionException, InterruptedException {

		CompletableFuture<?> registerSlotRequestFuture = resourceManager.runInMainThread(() -> {
			rmServices.slotManager.registerSlotRequest(
				new SlotRequest(new JobID(), new AllocationID(), resourceProfile, taskHost));
			return null;
		});

		// wait for the registerSlotRequest completion
		registerSlotRequestFuture.get();
	}
}
