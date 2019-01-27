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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatListener;
import org.apache.flink.runtime.heartbeat.HeartbeatManager;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * * General tests for the Blink YARN session resource manager component.
 */
public class YarnSessionResourceManagerTest extends TestLogger {
	private YarnSessionResourceManager yarnSessionResourceManager;
	private static final int TASK_MANAGER_COUNT = 5;
	private static final int TASK_MANAGER_MEMORY = 4096;
	ApplicationAttemptId applicationAttemptId;

	@Before
	public void setup() {
		yarnSessionResourceManager = createYarnSessionResourceManager();
		applicationAttemptId = ApplicationAttemptId.newInstance(
				ApplicationId.newInstance(System.currentTimeMillis(), 1), 1);
	}

	@SuppressWarnings("unchecked")
	private YarnSessionResourceManager createYarnSessionResourceManager() {
		Configuration conf = new Configuration();
		conf.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 128);
		conf.setLong(TaskManagerOptions.FLOATING_MANAGED_MEMORY_SIZE, 256);
		conf.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, 64 << 20);
		ResourceManagerConfiguration rmConfig =
			new ResourceManagerConfiguration(Time.seconds(1), Time.seconds(10));
		final HighAvailabilityServices highAvailabilityServices = mock(HighAvailabilityServices.class);
		when(highAvailabilityServices.getResourceManagerLeaderElectionService()).thenReturn(mock(LeaderElectionService.class));
		final HeartbeatServices heartbeatServices = mock(HeartbeatServices.class);
		when(heartbeatServices.createHeartbeatManagerSender(
			any(ResourceID.class),
			any(HeartbeatListener.class),
			any(ScheduledExecutor.class),
			any(Logger.class))).thenReturn(mock(HeartbeatManager.class));
		Map<String, String> env = new HashMap<>();
		env.put(YarnConfigKeys.ENV_TM_COUNT, String.valueOf(TASK_MANAGER_COUNT));
		env.put(YarnConfigKeys.ENV_TM_MEMORY, String.valueOf(TASK_MANAGER_MEMORY));
		return new YarnSessionResourceManager(
			new TestingRpcService(), "sessionResourceManager", ResourceID.generate(), conf,
			env, rmConfig,
			highAvailabilityServices, heartbeatServices,
			new SlotManager(mock(ScheduledExecutor.class), Time.seconds(1), Time.seconds(1), Time.seconds(1)),
			mock(MetricRegistry.class),
			mock(JobLeaderIdService.class),
			new ClusterInformation("localhost", 1234),
			mock(FatalErrorHandler.class), null);
	}

	@Test
	public void testContainersCompleted() throws Exception {
		yarnSessionResourceManager.setAMRMClient(mock(AMRMClientAsync.class));
		yarnSessionResourceManager.setNMClient(mock(NMClient.class));
		yarnSessionResourceManager.setExecutor(new ManuallyTriggeredScheduledExecutor());
		try {
			yarnSessionResourceManager.start();
		} catch (ResourceManagerException e) {}
		yarnSessionResourceManager.startClusterWorkers();

		assertEquals(5, yarnSessionResourceManager.getPendingContainerRequest().get());

		Container container1 = mock(Container.class);
		when(container1.getId()).thenReturn(ContainerId.newInstance(applicationAttemptId, 1));
		when(container1.getResource()).thenReturn(Resource.newInstance(1024, 1));
		Container container2 = mock(Container.class);
		when(container2.getPriority()).thenReturn(Priority.newInstance(1));
		when(container2.getId()).thenReturn(ContainerId.newInstance(applicationAttemptId, 2));
		when(container2.getResource()).thenReturn(Resource.newInstance(1024, 2));
		List<Container> containers = new LinkedList<>();
		containers.add(container1);
		containers.add(container2);

		YarnSessionResourceManager.YarnAMRMClientCallback callback = yarnSessionResourceManager.getYarnAMRMClientCallback();

		callback.onContainersAllocated(containers);
		assertEquals(3, yarnSessionResourceManager.getPendingContainerRequest().get());

		ContainerStatus containerStatus = ContainerStatus.newInstance(
			container1.getId(), ContainerState.COMPLETE, "Testing", -1
		);
		callback.onContainersCompleted(Collections.singletonList(containerStatus));
		assertEquals(4, yarnSessionResourceManager.getPendingContainerRequest().get());

		// When pending is enough, will not allocate a new container
		ResourceProfile resourceProfile = new ResourceProfile(1, 1024);
		yarnSessionResourceManager.startNewWorker(resourceProfile);
		assertEquals(4, yarnSessionResourceManager.getPendingContainerRequest().get());
	}

	@Test
	public void testAllocateContainerWithFloatingManagedMemory() {
		AMRMClientAsync yarnClient = mock(AMRMClientAsync.class);
		yarnSessionResourceManager.setAMRMClient(yarnClient);
		ArgumentCaptor<AMRMClient.ContainerRequest> containerRequestCaptor =
				ArgumentCaptor.forClass(AMRMClient.ContainerRequest.class);

		yarnSessionResourceManager.startClusterWorkers();

		verify(yarnClient, times(5)).addContainerRequest(containerRequestCaptor.capture());
		AMRMClient.ContainerRequest request = containerRequestCaptor.getAllValues().get(0);
		// Check memory allocated for YARN should contain managed and floating managed memory
		assertEquals(TASK_MANAGER_MEMORY, request.getCapability().getMemory());
	}

	@Test
	public void testGetPreviousContainers() throws Exception {
		ContainerId containerId1 = ContainerId.newInstance(applicationAttemptId, 1);
		Container containerFromPrevious = Container.newInstance(
				containerId1,
				null,
				"",
				Resource.newInstance(1024, 1),
				Priority.newInstance(0),
				null);
		RegisterApplicationMasterResponse mockResponse = mock(RegisterApplicationMasterResponse.class);
		when(mockResponse.getContainersFromPreviousAttempts()).thenReturn(Collections.singletonList(containerFromPrevious));
		AMRMClientAsync mockAMRMClient = mock(AMRMClientAsync.class);
		when(mockAMRMClient.registerApplicationMaster(eq("localhost"), eq(123), any(String.class)))
				.thenReturn(mockResponse);
		yarnSessionResourceManager.setExecutor(new ManuallyTriggeredScheduledExecutor());
		yarnSessionResourceManager.setAMRMClient(mockAMRMClient);
		yarnSessionResourceManager.setNMClient(mock(NMClient.class));
		try {
			yarnSessionResourceManager.start();
		} catch (ResourceManagerException e) {
		}
		yarnSessionResourceManager.getContainersFromPreviousAttempts(mockResponse);
		yarnSessionResourceManager.startClusterWorkers();

		assertEquals(4, yarnSessionResourceManager.getPendingContainerRequest().get());
	}

	@Test
	public void testCheckRegisterTimeoutContainers() throws Exception {
		yarnSessionResourceManager.setAMRMClient(mock(AMRMClientAsync.class));
		yarnSessionResourceManager.setNMClient(mock(NMClient.class));
		yarnSessionResourceManager.setExecutor(new ManuallyTriggeredScheduledExecutor());
		try {
			yarnSessionResourceManager.start();
		} catch (ResourceManagerException e) {}
		yarnSessionResourceManager.startClusterWorkers();

		assertEquals(5, yarnSessionResourceManager.getPendingContainerRequest().get());

		Container container = mock(Container.class);
		ContainerId containerId2 = ContainerId.newInstance(applicationAttemptId, 2);
		when(container.getId()).thenReturn(containerId2);
		when(container.getResource()).thenReturn(Resource.newInstance(1024, 1));
		List<Container> containers = new LinkedList<>();
		containers.add(container);
		YarnSessionResourceManager.YarnAMRMClientCallback callback = yarnSessionResourceManager.getYarnAMRMClientCallback();

		callback.onContainersAllocated(containers);
		assertEquals(4, yarnSessionResourceManager.getPendingContainerRequest().get());

		assertFalse(yarnSessionResourceManager.checkAndRegisterContainer(new ResourceID(containerId2.toString())));
		assertEquals(5, yarnSessionResourceManager.getPendingContainerRequest().get());
	}

	@Test
	public void testReleaseOverAllocatedContainers() throws Exception {
		AMRMClientAsync yarnClient = mock(AMRMClientAsync.class);
		yarnSessionResourceManager.setAMRMClient(yarnClient);
		yarnSessionResourceManager.setNMClient(mock(NMClient.class));
		yarnSessionResourceManager.setExecutor(new ManuallyTriggeredScheduledExecutor());
		try {
			yarnSessionResourceManager.start();
		} catch (ResourceManagerException e) {}
		yarnSessionResourceManager.startClusterWorkers();

		assertEquals(5, yarnSessionResourceManager.getPendingContainerRequest().get());

		List<Container> containers = new LinkedList<>();
		for (int i = 1; i < 11; i++) {
			Container container = mock(Container.class);
			ContainerId containerId = ContainerId.newInstance(applicationAttemptId, i);
			when(container.getId()).thenReturn(containerId);
			when(container.getResource()).thenReturn(Resource.newInstance(1024, 1));
			containers.add(container);
		}

		YarnSessionResourceManager.YarnAMRMClientCallback callback = yarnSessionResourceManager.getYarnAMRMClientCallback();
		// YARN resource manager return more container than allocated
		callback.onContainersAllocated(containers);
		assertEquals(0, yarnSessionResourceManager.getPendingContainerRequest().get());
		assertEquals(5, yarnSessionResourceManager.getNumberAllocatedWorkers());

		// Container[6-10] should be released
		ArgumentCaptor<ContainerId> containerReleaseCaptor = ArgumentCaptor.forClass(ContainerId.class);
		verify(yarnClient, times(5)).releaseAssignedContainer(containerReleaseCaptor.capture());
		ArgumentCaptor<AMRMClient.ContainerRequest> containerRemoveCaptor =
				ArgumentCaptor.forClass(AMRMClient.ContainerRequest.class);
		verify(yarnClient, times(5)).removeContainerRequest(containerRemoveCaptor.capture());
		assertEquals(5, containerReleaseCaptor.getAllValues().size());
		int start = 6;
		for (ContainerId containerId : containerReleaseCaptor.getAllValues()) {
			assertEquals(ContainerId.newInstance(applicationAttemptId, start++), containerId);
		}
		assertEquals(5, yarnSessionResourceManager.getNumberAllocatedWorkers());
	}
}
