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

package org.apache.flink.kubernetes.runtime.clusterframework;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.Constants;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.KubernetesConnectionManager;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
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

import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Watcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * General tests for the Flink Kubernetes resource manager component.
 */
public class KubernetesResourceManagerTest extends KubernetesRMTestBase {

	protected SlotManager spySlotManager;

	@Before
	public void setup() {
		super.setup();
		spySlotManager = Mockito.spy(new SlotManager(Mockito.mock(ScheduledExecutor.class), Time.seconds(1), Time.seconds(1), Time.seconds(1)));
	}

	@SuppressWarnings("unchecked")
	protected KubernetesResourceManager createKubernetesResourceManager(Configuration conf) {
		ResourceManagerConfiguration rmConfig =
			new ResourceManagerConfiguration(Time.seconds(1), Time.seconds(10));
		final HighAvailabilityServices highAvailabilityServices = Mockito.mock(HighAvailabilityServices.class);
		Mockito.when(highAvailabilityServices.getResourceManagerLeaderElectionService()).thenReturn(Mockito.mock(LeaderElectionService.class));
		final HeartbeatServices heartbeatServices = Mockito.mock(HeartbeatServices.class);
		Mockito.when(heartbeatServices.createHeartbeatManagerSender(
			Matchers.any(ResourceID.class),
			Matchers.any(HeartbeatListener.class),
			Matchers.any(ScheduledExecutor.class),
			Matchers.any(Logger.class))).thenReturn(Mockito.mock(HeartbeatManager.class));
		return new KubernetesResourceManager(
			new TestingRpcService(), "ResourceManager", ResourceID.generate(), conf,
			rmConfig,
			highAvailabilityServices, heartbeatServices,
			spySlotManager,
			Mockito.mock(MetricRegistry.class),
			Mockito.mock(JobLeaderIdService.class),
			new ClusterInformation("localhost", 1234),
			Mockito.mock(FatalErrorHandler.class));
	}

	protected String createPodName(int podId) {
		return APP_ID + Constants.TASK_MANAGER_LABEL_SUFFIX + Constants.NAME_SEPARATOR + podId;
	}

	protected Pod createPod(int priority, int podId) {
		Map<String, String> labels = new HashMap<>();
		labels.put(Constants.LABEL_APP_KEY, APP_ID);
		labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
		labels.put(Constants.LABEL_PRIORITY_KEY, String.valueOf(priority));
		ObjectMeta meta = new ObjectMeta();
		meta.setName(createPodName(podId));
		meta.setLabels(labels);
		Pod pod = new Pod();
		pod.setMetadata(meta);
		return pod;
	}

	protected long[] getSortedWorkerNodeIds(KubernetesResourceManager spyKubernetesRM){
		long[] ids = spyKubernetesRM.getWorkerNodes().values().stream().mapToLong(e -> e.getPodId()).toArray();
		Arrays.sort(ids);
		return ids;
	}

	public KubernetesResourceManager mockResourceManager(
		Configuration flinkConf, KubernetesConnectionManager kubernetesConnectionManager) {
		KubernetesResourceManager kubernetesSessionRM =
			createKubernetesResourceManager(flinkConf);
		KubernetesResourceManager spyKubernetesRM = Mockito.spy(kubernetesSessionRM);
		spyKubernetesRM.setOwnerReference(new OwnerReferenceBuilder().build());
		Mockito.doReturn(kubernetesConnectionManager).when(spyKubernetesRM).createKubernetesConnectionManager();
		try {
			Mockito.doNothing().when(spyKubernetesRM).setupOwnerReference();
		} catch (ResourceManagerException e) {
			throw new RuntimeException(e);
		}
		return spyKubernetesRM;
	}

	@Test
	public void testNormalProcess() throws Exception {
		KubernetesConnectionManager kubernetesConnectionManager =
			new KubernetesRMTestBase.TestingKubernetesConnectionManager(flinkConf);
		KubernetesResourceManager spyKubernetesRM =
			mockResourceManager(flinkConf, kubernetesConnectionManager);
		// TM register check always return true
		Mockito.doNothing().when(spyKubernetesRM).checkTMRegistered(Matchers.any());

		// start RM
		spyKubernetesRM.start();

		// request worker nodes
		ResourceProfile resourceProfile1 = new ResourceProfile(1.0, 200);
		ResourceProfile resourceProfile2 = new ResourceProfile(1.0, 500);
		spyKubernetesRM.startNewWorker(resourceProfile1);
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		spyKubernetesRM.startNewWorker(resourceProfile1);
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		spyKubernetesRM.startNewWorker(resourceProfile2);
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(1).size());
		spyKubernetesRM.startNewWorker(resourceProfile2);
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().get(1).size());
		spyKubernetesRM.startNewWorker(resourceProfile2);
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(3, spyKubernetesRM.getPendingWorkerNodes().get(1).size());

		// cancel 1 worker node(id=1) with priority 0
		spyKubernetesRM.cancelNewWorker(resourceProfile1);
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		// cancel 1 worker node(id=3) with priority 1
		spyKubernetesRM.cancelNewWorker(resourceProfile2);
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().get(1).size());
		Assert.assertEquals(new HashSet<>(), spyKubernetesRM.getWorkerNodes().keySet());

		// add removed worker node(id=1) with priority 0, should be skipped
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(0, 1));
		Assert.assertEquals(0, spyKubernetesRM.getWorkerNodes().size());
		// add removed worker node(id=3) with priority 1, should be skipped
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(1, 3));
		Assert.assertEquals(0, spyKubernetesRM.getWorkerNodes().size());

		// add worker node(id=2) with priority 0
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(0, 2));
		Assert.assertEquals(1, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{2L}, getSortedWorkerNodeIds(spyKubernetesRM));
		// add worker node(id=4) with priority 1
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(1, 4));
		Assert.assertEquals(2, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{2L, 4L}, getSortedWorkerNodeIds(spyKubernetesRM));

		// stop worker node(id=4) with priority 1
		// mock that number of pending slot requests is larger than number of pending worker nodes
		Mockito.doReturn(2).when(spySlotManager).getNumberPendingSlotRequests();
		spyKubernetesRM.stopWorker(spyKubernetesRM.getWorkerNodes().get(new ResourceID(createPodName(4))));
		// should left 1 worker node(id=2)
		Assert.assertEquals(1, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{2L}, getSortedWorkerNodeIds(spyKubernetesRM));
		// should request new worker node inside stopWorker, now pending worker nodes: 2
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().get(1).size());

		// deregister app
		spyKubernetesRM.deregisterApplication(ApplicationStatus.SUCCEEDED, "");
		// check stopped
		Assert.assertTrue(spyKubernetesRM.isStopped());
	}

	@Test
	public void testNormalProcessForMultiSlots() throws Exception {
		// set max core=3 for multiple slots
		Configuration newFlinkConf = new Configuration(flinkConf);
		newFlinkConf.setDouble(TaskManagerOptions.TASK_MANAGER_MULTI_SLOTS_MAX_CORE, 4.0);
		KubernetesConnectionManager kubernetesConnectionManager =
			new KubernetesRMTestBase.TestingKubernetesConnectionManager(newFlinkConf);
		KubernetesResourceManager spyKubernetesRM =
			mockResourceManager(newFlinkConf, kubernetesConnectionManager);
		// TM register check always return true
		Mockito.doNothing().when(spyKubernetesRM).checkTMRegistered(Matchers.any());

		// start RM
		spyKubernetesRM.start();

		// request 50 worker nodes with priority 0 and 1
		ResourceProfile resourceProfile1 = new ResourceProfile(1.0, 200);
		ResourceProfile resourceProfile2 = new ResourceProfile(2.0, 200);
		int requestNum = 0;
		for (int i = 0; i < 50; i++) {
			requestNum++;
			spyKubernetesRM.startNewWorker(resourceProfile1);
			spyKubernetesRM.startNewWorker(resourceProfile2);
			Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
			Assert.assertEquals(requestNum % 4 == 0 ? requestNum / 4 : requestNum / 4 + 1,
				spyKubernetesRM.getPendingWorkerNodes().get(0).size());
			Assert.assertEquals(requestNum % 2 == 0 ? requestNum / 2 : requestNum / 2 + 1,
				spyKubernetesRM.getPendingWorkerNodes().get(1).size());
		}

		// cancel 47 worker nodes with priority 0 and 1
		for (int j = 0; j < 47; j++) {
			requestNum--;
			spyKubernetesRM.cancelNewWorker(resourceProfile1);
			spyKubernetesRM.cancelNewWorker(resourceProfile2);
			Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
			Assert.assertEquals(requestNum % 4 == 0 ? requestNum / 4 : requestNum / 4 + 1,
				spyKubernetesRM.getPendingWorkerNodes().get(0).size());
			Assert.assertEquals(requestNum % 2 == 0 ? requestNum / 2 : requestNum / 2 + 1,
				spyKubernetesRM.getPendingWorkerNodes().get(1).size());
		}
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().get(1).size());
		List<String> ids = spyKubernetesRM.getPendingWorkerNodes().values().stream()
			.flatMap(e -> e.stream()).map(e -> e.getResourceIdString()).collect(Collectors.toList());
		Collections.sort(ids);
		Assert.assertEquals(3, ids.size());
		Assert.assertArrayEquals(new String[]{createPodName(36), createPodName(37),
			createPodName(38)}, ids.toArray());

		// add removed worker node(id=1) with priority 0, should be skipped
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(0, 1));
		Assert.assertEquals(0, spyKubernetesRM.getWorkerNodes().size());
		// add removed worker node(id=2) with priority 1, should be skipped
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(1, 2));
		Assert.assertEquals(0, spyKubernetesRM.getWorkerNodes().size());

		// add worker node(id=37) with priority 0
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(0, 37));
		Assert.assertEquals(1, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{37L}, getSortedWorkerNodeIds(spyKubernetesRM));
		// add worker node(id=38) with priority 1
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(1, 38));
		Assert.assertEquals(2, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{37L, 38L}, getSortedWorkerNodeIds(spyKubernetesRM));
		// add worker node(id=36) with priority 1
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(1, 36));
		Assert.assertEquals(3, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{36L, 37L, 38L}, getSortedWorkerNodeIds(spyKubernetesRM));

		// no pending now
		Assert.assertEquals(0, spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		Assert.assertEquals(0, spyKubernetesRM.getPendingWorkerNodes().get(1).size());

		// stop worker node(id=36) with priority 1
		// mock that number of pending slot requests is larger than number of pending worker nodes
		Mockito.doReturn(2).when(spySlotManager).getNumberPendingSlotRequests();
		spyKubernetesRM.stopWorker(spyKubernetesRM.getWorkerNodes().get(new ResourceID(createPodName(36))));
		// should left 2 worker node(id=37,38)
		Assert.assertEquals(2, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{37L, 38L}, getSortedWorkerNodeIds(spyKubernetesRM));
		// should request new worker node inside stopWorker, now pending worker nodes: 1
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(1).size());

		// deregister app
		spyKubernetesRM.deregisterApplication(ApplicationStatus.SUCCEEDED, "");
		// check stopped
		Assert.assertTrue(spyKubernetesRM.isStopped());
	}

	@Test
	public void testGetPreviousWorkerNodes() throws Exception {
		KubernetesConnectionManager kubernetesConnectionManager =
			new KubernetesRMTestBase.TestingKubernetesConnectionManager(flinkConf);
		// add 5 pods as previous worker nodes
		kubernetesConnectionManager.createPod(createPod(0, 1));
		kubernetesConnectionManager.createPod(createPod(0, 3));
		kubernetesConnectionManager.createPod(createPod(0, 5));
		kubernetesConnectionManager.createPod(createPod(2, 4));
		kubernetesConnectionManager.createPod(createPod(2, 6));
		KubernetesResourceManager spyKubernetesRM =
			mockResourceManager(flinkConf, kubernetesConnectionManager);

		// TM register check always return true
		Mockito.doNothing().when(spyKubernetesRM).checkTMRegistered(Matchers.any());

		// start RM, get previous 5 worker nodes
		spyKubernetesRM.start();
		Assert.assertEquals(5, spyKubernetesRM.getNumberAllocatedWorkers());
		Assert.assertArrayEquals(new long[]{1L, 3L, 4L, 5L, 6L}, getSortedWorkerNodeIds(spyKubernetesRM));
		Assert.assertEquals(0, spyKubernetesRM.getPendingWorkerNodes().size());

		// request worker nodes with two different profiles, priority should be 3 and 4
		ResourceProfile resourceProfile1 = new ResourceProfile(1.0, 200);
		ResourceProfile resourceProfile2 = new ResourceProfile(1.0, 500);
		spyKubernetesRM.startNewWorker(resourceProfile1);
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(3).size());
		spyKubernetesRM.startNewWorker(resourceProfile1);
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().get(3).size());
		spyKubernetesRM.startNewWorker(resourceProfile2);
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(4).size());
		spyKubernetesRM.startNewWorker(resourceProfile2);
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().get(4).size());
		spyKubernetesRM.startNewWorker(resourceProfile2);
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(3, spyKubernetesRM.getPendingWorkerNodes().get(4).size());

		// add invalid worker node(id=2) with priority 0, should be skipped
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(0, 2));
		Assert.assertEquals(5, spyKubernetesRM.getWorkerNodes().size());
		// add already exist worker node(id=4) with priority 2, should be skipped
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(2, 4));
		Assert.assertEquals(5, spyKubernetesRM.getWorkerNodes().size());

		// add worker node(id=7) with priority 3
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(3, 7));
		Assert.assertEquals(6, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{1L, 3L, 4L, 5L, 6L, 7L}, getSortedWorkerNodeIds(spyKubernetesRM));
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(3).size());

		// add worker node(id=9) with priority 4
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(4, 9));
		Assert.assertEquals(7, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{1L, 3L, 4L, 5L, 6L, 7L, 9L}, getSortedWorkerNodeIds(spyKubernetesRM));
		Assert.assertEquals(2, spyKubernetesRM.getPendingWorkerNodes().get(4).size());

		// add worker node(id=8) with priority 3
		spyKubernetesRM.handlePodMessage(Watcher.Action.ADDED, createPod(3, 8));
		Assert.assertEquals(8, spyKubernetesRM.getWorkerNodes().size());
		Assert.assertArrayEquals(new long[]{1L, 3L, 4L, 5L, 6L, 7L, 8L, 9L}, getSortedWorkerNodeIds(spyKubernetesRM));
		Assert.assertEquals(0, spyKubernetesRM.getPendingWorkerNodes().get(3).size());

		// request worker nodes with priority 3
		spyKubernetesRM.startNewWorker(resourceProfile1);
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().get(3).size());

		// deregister app
		spyKubernetesRM.deregisterApplication(ApplicationStatus.SUCCEEDED, "");
		// check stopped
		Assert.assertTrue(spyKubernetesRM.isStopped());
	}

	@Test
	public void testTaskManagerRegisterTimeout() throws Exception {
		KubernetesConnectionManager kubernetesConnectionManager =
			new KubernetesRMTestBase.TestingKubernetesConnectionManager(flinkConf);
		KubernetesResourceManager spyKubernetesRM =
			mockResourceManager(flinkConf, kubernetesConnectionManager);

		// start RM
		spyKubernetesRM.start();
		Assert.assertEquals(0, spyKubernetesRM.getNumberAllocatedWorkers());

		// request worker nodes with priority 0
		int requestNum = 5;
		ResourceProfile resourceProfile1 = new ResourceProfile(1.0, 200);
		for (int i = 0; i < requestNum; i++) {
			spyKubernetesRM.startNewWorker(resourceProfile1);
			Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().size());
			Assert.assertEquals(i + 1, spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		}

		// add 2 worker nodes(id=1,2) with priority 0
		int addNum = 2;
		for (int i = 0; i < addNum; i++) {
			spyKubernetesRM
				.handlePodMessage(Watcher.Action.ADDED, createPod(0, i + 1));
			Assert.assertEquals(i + 1, spyKubernetesRM.getWorkerNodes().size());
			Assert.assertEquals(requestNum - 1 - i,
				spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		}
		Assert.assertArrayEquals(LongStream.rangeClosed(1, addNum).toArray(), getSortedWorkerNodeIds(spyKubernetesRM));

		Mockito.verify(spyKubernetesRM, Mockito.times(requestNum))
			.requestNewWorkerNode(Mockito.any(), Mockito.anyInt());

		// trigger task manager register timeout, should request new worker nodes
		Mockito.doReturn(requestNum).when(spySlotManager).getNumberPendingSlotRequests();
		spyKubernetesRM.getWorkerNodes().keySet().stream().forEach(spyKubernetesRM::checkTMRegistered);
		Mockito.verify(spyKubernetesRM, Mockito.times(requestNum + addNum))
			.requestNewWorkerNode(Mockito.any(), Mockito.anyInt());
		Assert.assertEquals(0, spyKubernetesRM.getNumberAllocatedWorkers());
		Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().size());
		Assert.assertEquals(requestNum, spyKubernetesRM.getPendingWorkerNodes().get(0).size());

		// deregister app
		spyKubernetesRM.deregisterApplication(ApplicationStatus.SUCCEEDED, "");
		// check stopped
		Assert.assertTrue(spyKubernetesRM.isStopped());
	}

	@Test
	public void testExitWhenReachMaxFailedAttempts() throws Exception {
		// set max failed attempts = 2
		int maxFailedAttempts = 2;
		Configuration newFlinkConf = new Configuration(flinkConf);
		newFlinkConf.setInteger(KubernetesConfigOptions.WORKER_NODE_MAX_FAILED_ATTEMPTS, maxFailedAttempts);
		KubernetesConnectionManager kubernetesConnectionManager =
			new KubernetesRMTestBase.TestingKubernetesConnectionManager(newFlinkConf);
		KubernetesResourceManager spyKubernetesRM =
			mockResourceManager(newFlinkConf, kubernetesConnectionManager);

		// start RM
		spyKubernetesRM.start();
		Assert.assertEquals(0, spyKubernetesRM.getNumberAllocatedWorkers());
		Assert.assertEquals(0, spyKubernetesRM.getPendingWorkerNodes().size());

		// request worker nodes with priority 0
		int requestNum = 5;
		ResourceProfile resourceProfile1 = new ResourceProfile(1.0, 200);
		for (int i = 0; i < requestNum; i++) {
			spyKubernetesRM.startNewWorker(resourceProfile1);
			Assert.assertEquals(1, spyKubernetesRM.getPendingWorkerNodes().size());
			Assert.assertEquals(i + 1, spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		}

		// add 2 worker nodes(id=1,2) with priority 0
		int addNum = 2;
		for (int i = 0; i < addNum; i++) {
			spyKubernetesRM
				.handlePodMessage(Watcher.Action.ADDED, createPod(0, i + 1));
			Assert.assertEquals(i + 1, spyKubernetesRM.getWorkerNodes().size());
			Assert.assertEquals(requestNum - 1 - i,
				spyKubernetesRM.getPendingWorkerNodes().get(0).size());
		}
		Assert.assertArrayEquals(LongStream.rangeClosed(1, addNum).toArray(), getSortedWorkerNodeIds(spyKubernetesRM));

		Mockito.verify(spyKubernetesRM, Mockito.times(requestNum))
			.requestNewWorkerNode(Mockito.any(), Mockito.anyInt());

		// trigger task manager register timeout, should request new worker nodes
		Mockito.doReturn(requestNum).when(spySlotManager).getNumberPendingSlotRequests();
		spyKubernetesRM.getWorkerNodes().keySet().stream().forEach(spyKubernetesRM::checkTMRegistered);

		// won't add new request for the last attempt
		Assert.assertEquals(requestNum - 1, spyKubernetesRM.getPendingWorkerNodes().get(0).size());

		// will stop internally
		Assert.assertTrue(spyKubernetesRM.isStopped());
	}

	@Test
	public void testConnectionLostButNotReachMaxRetryTimes() throws ResourceManagerException {
		super.testConnectionLostButNotReachMaxRetryTimes();
	}

	@Test
	public void testConnectionLostAndReachMaxRetryTimes() throws ResourceManagerException {
		super.testConnectionLostAndReachMaxRetryTimes();
	}
}
