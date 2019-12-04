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
import org.apache.flink.configuration.ResourceManagerOptions;
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
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.utils.MockResourceManagerRuntimeServices;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.RunnableWithException;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
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
import org.mockito.verification.VerificationWithTimeout;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_APP_ID;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_CLIENT_HOME_DIR;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_CLIENT_SHIP_FILES;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_FLINK_CLASSPATH;
import static org.apache.flink.yarn.YarnConfigKeys.ENV_HADOOP_USER_NAME;
import static org.apache.flink.yarn.YarnConfigKeys.FLINK_JAR_PATH;
import static org.apache.flink.yarn.YarnConfigKeys.FLINK_YARN_FILES;
import static org.apache.flink.yarn.YarnResourceManager.ERROR_MASSAGE_ON_SHUTDOWN_REQUEST;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * General tests for the YARN resource manager component.
 */
public class YarnResourceManagerTest extends TestLogger {

	private static final Time TIMEOUT = Time.seconds(10L);

	private static final VerificationWithTimeout VERIFICATION_TIMEOUT = timeout(TIMEOUT.toMilliseconds());

	private Configuration flinkConfig;

	private Map<String, String> env;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();

	@Before
	public void setup() throws IOException {
		testingFatalErrorHandler = new TestingFatalErrorHandler();

		flinkConfig = new Configuration();
		flinkConfig.setInteger(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN, 100);

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
		env.put(FLINK_JAR_PATH, root.toURI().toString());
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
		AMRMClientAsync<AMRMClient.ContainerRequest> mockResourceManagerClient;
		NMClientAsync mockNMClient;

		TestingYarnResourceManager(
				RpcService rpcService,
				String resourceManagerEndpointId,
				ResourceID resourceId,
				Configuration flinkConfig,
				Map<String, String> env,
				HighAvailabilityServices highAvailabilityServices,
				HeartbeatServices heartbeatServices,
				SlotManager slotManager,
				MetricRegistry metricRegistry,
				JobLeaderIdService jobLeaderIdService,
				ClusterInformation clusterInformation,
				FatalErrorHandler fatalErrorHandler,
				@Nullable String webInterfaceUrl,
				AMRMClientAsync<AMRMClient.ContainerRequest> mockResourceManagerClient,
				NMClientAsync mockNMClient,
				JobManagerMetricGroup jobManagerMetricGroup) {
			super(
				rpcService,
				resourceManagerEndpointId,
				resourceId,
				flinkConfig,
				env,
				highAvailabilityServices,
				heartbeatServices,
				slotManager,
				metricRegistry,
				jobLeaderIdService,
				clusterInformation,
				fatalErrorHandler,
				webInterfaceUrl,
				jobManagerMetricGroup);
			this.mockNMClient = mockNMClient;
			this.mockResourceManagerClient = mockResourceManagerClient;
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
			return mockResourceManagerClient;
		}

		@Override
		protected NMClientAsync createAndStartNodeManagerClient(YarnConfiguration yarnConfiguration) {
			return mockNMClient;
		}
	}

	class Context {

		// services
		final TestingRpcService rpcService;
		final MockResourceManagerRuntimeServices rmServices;

		// RM
		final ResourceID rmResourceID;
		static final String RM_ADDRESS = "resourceManager";
		final TestingYarnResourceManager resourceManager;

		final int dataPort = 1234;
		final HardwareDescription hardwareDescription = new HardwareDescription(1, 2L, 3L, 4L);

		// domain objects for test purposes
		final ResourceProfile resourceProfile1 = ResourceProfile.UNKNOWN;

		public String taskHost = "host1";

		public NMClientAsync mockNMClient = mock(NMClientAsync.class);

		@SuppressWarnings("unchecked")
		public AMRMClientAsync<AMRMClient.ContainerRequest> mockResourceManagerClient = mock(AMRMClientAsync.class);

		public JobManagerMetricGroup mockJMMetricGroup =
				UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup();

		/**
		 * Create mock RM dependencies.
		 */
		Context() throws Exception {
			this(flinkConfig);
		}

		Context(Configuration configuration) throws  Exception {
			rpcService = new TestingRpcService();
			rmServices = new MockResourceManagerRuntimeServices(rpcService, TIMEOUT);

			// resource manager
			rmResourceID = ResourceID.generate();
			resourceManager =
					new TestingYarnResourceManager(
							rpcService,
							RM_ADDRESS,
							rmResourceID,
							configuration,
							env,
							rmServices.highAvailabilityServices,
							rmServices.heartbeatServices,
							rmServices.slotManager,
							rmServices.metricRegistry,
							rmServices.jobLeaderIdService,
							new ClusterInformation("localhost", 1234),
							testingFatalErrorHandler,
							null,
							mockResourceManagerClient,
							mockNMClient,
							mockJMMetricGroup);
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

		void verifyContainerHasBeenStarted(Container testingContainer) {
			verify(mockResourceManagerClient, VERIFICATION_TIMEOUT).removeContainerRequest(any(AMRMClient.ContainerRequest.class));
			verify(mockNMClient, VERIFICATION_TIMEOUT).startContainerAsync(eq(testingContainer), any(ContainerLaunchContext.class));
		}

		void verifyContainerHasBeenRequested() {
			verify(mockResourceManagerClient, VERIFICATION_TIMEOUT).addContainerRequest(any(AMRMClient.ContainerRequest.class));
		}
	}

	private static Container mockContainer(String host, int port, int containerId, Resource resource) {
		Container mockContainer = mock(Container.class);

		NodeId mockNodeId = NodeId.newInstance(host, port);
		ContainerId mockContainerId = ContainerId.newInstance(
			ApplicationAttemptId.newInstance(
				ApplicationId.newInstance(System.currentTimeMillis(), 1),
				1
			),
			containerId
		);

		when(mockContainer.getId()).thenReturn(mockContainerId);
		when(mockContainer.getNodeId()).thenReturn(mockNodeId);
		when(mockContainer.getResource()).thenReturn(resource);
		when(mockContainer.getPriority()).thenReturn(Priority.UNDEFINED);

		return mockContainer;
	}

	private static ContainerStatus mockContainerStatus(ContainerId containerId) {
		ContainerStatus mockContainerStatus = mock(ContainerStatus.class);

		when(mockContainerStatus.getContainerId()).thenReturn(containerId);
		when(mockContainerStatus.getState()).thenReturn(ContainerState.COMPLETE);
		when(mockContainerStatus.getDiagnostics()).thenReturn("Test exit");
		when(mockContainerStatus.getExitStatus()).thenReturn(-1);

		return mockContainerStatus;
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
	public void testStopWorker() throws Exception {
		new Context() {{
			runTest(() -> {
				// Request slot from SlotManager.
				registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);

				// Callback from YARN when container is allocated.
				Container testingContainer = mockContainer("container", 1234, 1, resourceManager.getContainerResource());

				doReturn(Collections.singletonList(Collections.singletonList(resourceManager.getContainerRequest())))
					.when(mockResourceManagerClient).getMatchingRequests(any(Priority.class), anyString(), any(Resource.class));

				resourceManager.onContainersAllocated(ImmutableList.of(testingContainer));
				verifyContainerHasBeenRequested();
				verifyContainerHasBeenStarted(testingContainer);

				// Remote task executor registers with YarnResourceManager.
				TaskExecutorGateway mockTaskExecutorGateway = mock(TaskExecutorGateway.class);
				rpcService.registerGateway(taskHost, mockTaskExecutorGateway);

				final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

				final ResourceID taskManagerResourceId = new ResourceID(testingContainer.getId().toString());
				final SlotReport slotReport = new SlotReport(
					new SlotStatus(
						new SlotID(taskManagerResourceId, 1),
						new ResourceProfile(10, 1, 1, 1, 0, 0, Collections.emptyMap())));

				CompletableFuture<Integer> numberRegisteredSlotsFuture = rmGateway
					.registerTaskExecutor(
						taskHost,
						taskManagerResourceId,
						dataPort,
						hardwareDescription,
						Time.seconds(10L))
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

				verify(mockNMClient).stopContainerAsync(any(ContainerId.class), any(NodeId.class));
				verify(mockResourceManagerClient).releaseAssignedContainer(any(ContainerId.class));
			});

			// It's now safe to access the SlotManager state since the ResourceManager has been stopped.
			assertThat(rmServices.slotManager.getNumberRegisteredSlots(), Matchers.equalTo(0));
			assertThat(resourceManager.getNumberOfRegisteredTaskManagers().get(), Matchers.equalTo(0));
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
			runTest(() -> {
				registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);

				// Callback from YARN when container is allocated.
				Container testingContainer = mockContainer("container", 1234, 1, resourceManager.getContainerResource());

				doReturn(Collections.singletonList(Collections.singletonList(resourceManager.getContainerRequest())))
					.when(mockResourceManagerClient).getMatchingRequests(any(Priority.class), anyString(), any(Resource.class));

				resourceManager.onContainersAllocated(ImmutableList.of(testingContainer));
				verifyContainerHasBeenRequested();
				verifyContainerHasBeenStarted(testingContainer);

				// Callback from YARN when container is Completed, pending request can not be fulfilled by pending
				// containers, need to request new container.
				ContainerStatus testingContainerStatus = mockContainerStatus(testingContainer.getId());

				resourceManager.onContainersCompleted(ImmutableList.of(testingContainerStatus));
				verify(mockResourceManagerClient, VERIFICATION_TIMEOUT.times(2)).addContainerRequest(any(AMRMClient.ContainerRequest.class));

				// Callback from YARN when container is Completed happened before global fail, pending request
				// slot is already fulfilled by pending containers, no need to request new container.
				resourceManager.onContainersCompleted(ImmutableList.of(testingContainerStatus));
				verify(mockResourceManagerClient, times(2)).addContainerRequest(any(AMRMClient.ContainerRequest.class));
			});
		}};
	}

	@Test
	public void testOnStartContainerError() throws Exception {
		new Context() {{
			runTest(() -> {
				registerSlotRequest(resourceManager, rmServices, resourceProfile1, taskHost);
				Container testingContainer = mockContainer("container", 1234, 1, resourceManager.getContainerResource());

				doReturn(Collections.singletonList(Collections.singletonList(resourceManager.getContainerRequest())))
					.when(mockResourceManagerClient).getMatchingRequests(any(Priority.class), anyString(), any(Resource.class));

				resourceManager.onContainersAllocated(ImmutableList.of(testingContainer));
				verifyContainerHasBeenRequested();
				verifyContainerHasBeenStarted(testingContainer);

				resourceManager.onStartContainerError(testingContainer.getId(), new Exception("start error"));
				verify(mockResourceManagerClient, VERIFICATION_TIMEOUT).releaseAssignedContainer(testingContainer.getId());
				verify(mockResourceManagerClient, VERIFICATION_TIMEOUT.times(2)).addContainerRequest(any(AMRMClient.ContainerRequest.class));
			});
		}};
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
