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
import org.apache.flink.kubernetes.configuration.Constants;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.KubernetesConnectionManager;
import org.apache.flink.kubernetes.utils.KubernetesRMUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
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

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.Watcher;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * General tests for the Flink Kubernetes session resource manager component.
 */
public class KubernetesSessionResourceManagerTest extends KubernetesRMTestBase {

	protected static final int TASK_MANAGER_COUNT = 3;

	protected Map<String, String> taskManagerPodLabels;

	protected String taskManagerPodNamePrefix;

	protected String taskManagerConfigMapName;

	@Before
	public void setup() {
		super.setup();
		flinkConf.setInteger(KubernetesConfigOptions.TASK_MANAGER_COUNT, TASK_MANAGER_COUNT);
		taskManagerPodLabels = new HashMap<>();
		taskManagerPodLabels.put(Constants.LABEL_APP_KEY, APP_ID);
		taskManagerPodLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
		taskManagerPodNamePrefix =
			APP_ID + Constants.TASK_MANAGER_LABEL_SUFFIX + Constants.NAME_SEPARATOR;
		taskManagerConfigMapName =
			APP_ID + Constants.TASK_MANAGER_CONFIG_MAP_SUFFIX;
	}

	@SuppressWarnings("unchecked")
	protected KubernetesSessionResourceManager createKubernetesSessionResourceManager(Configuration conf) {
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
		return new KubernetesSessionResourceManager(
			new TestingRpcService(), "sessionResourceManager", ResourceID.generate(), conf,
			rmConfig,
			highAvailabilityServices, heartbeatServices,
			new SlotManager(Mockito.mock(ScheduledExecutor.class), Time.seconds(1), Time.seconds(1), Time.seconds(1)),
			Mockito.mock(MetricRegistry.class),
			Mockito.mock(JobLeaderIdService.class),
			new ClusterInformation("localhost", 1234),
			Mockito.mock(FatalErrorHandler.class));
	}

	protected Pod createPod(int podId) {
		ObjectMeta meta = new ObjectMeta();
		meta.setName(APP_ID + Constants.TASK_MANAGER_LABEL_SUFFIX + Constants.NAME_SEPARATOR + podId);
		Pod pod = new Pod();
		pod.setMetadata(meta);
		return pod;
	}

	public KubernetesSessionResourceManager mockResourceManager(
		Configuration flinkConf, KubernetesConnectionManager kubernetesConnectionManager) {
		KubernetesSessionResourceManager kubernetesSessionRM =
			createKubernetesSessionResourceManager(flinkConf);
		KubernetesSessionResourceManager spyKubernetesSessionRM = Mockito.spy(kubernetesSessionRM);
		spyKubernetesSessionRM.setOwnerReference(new OwnerReferenceBuilder().build());
		Mockito.doReturn(kubernetesConnectionManager).when(spyKubernetesSessionRM).createKubernetesConnectionManager();
		try {
			Mockito.doNothing().when(spyKubernetesSessionRM).setupOwnerReference();
		} catch (ResourceManagerException e) {
			throw new RuntimeException(e);
		}
		return spyKubernetesSessionRM;
	}

	@Test
	public void testNormalProcess() throws Exception {
		KubernetesConnectionManager kubernetesConnectionManager =
			new KubernetesRMTestBase.TestingKubernetesConnectionManager(flinkConf);
		KubernetesSessionResourceManager spyKubernetesSessionRM =
			mockResourceManager(flinkConf, kubernetesConnectionManager);
		// TM register check always return true
		Mockito.doNothing().when(spyKubernetesSessionRM).checkTMRegistered(Matchers.any());

		// start session RM
		spyKubernetesSessionRM.start();
		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getPendingWorkerNodes().size());
		Assert.assertEquals(0, spyKubernetesSessionRM.getNumberAllocatedWorkers());

		// add worker nodes
		int podNum = 0;
		while (podNum < TASK_MANAGER_COUNT) {
			spyKubernetesSessionRM.handlePodMessage(Watcher.Action.ADDED, createPod(++podNum));
			Assert.assertEquals(TASK_MANAGER_COUNT - podNum, spyKubernetesSessionRM.getPendingWorkerNodes().size());
			Assert.assertEquals(podNum, spyKubernetesSessionRM.getNumberAllocatedWorkers());
		}
		Assert.assertEquals(0, spyKubernetesSessionRM.getPendingWorkerNodes().size());
		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getNumberAllocatedWorkers());

		// task manager register timeout will never happen
		Mockito.verify(spyKubernetesSessionRM,
			Mockito.times(TASK_MANAGER_COUNT)).requestNewWorkerNode();

		spyKubernetesSessionRM.postStop();
	}

	@Test
	public void testGetPreviousWorkerNodes() throws Exception {
		KubernetesConnectionManager kubernetesConnectionManager =
			new KubernetesRMTestBase.TestingKubernetesConnectionManager(flinkConf);
		// add 2 pods as previous worker nodes
		OwnerReference ownerReference = new OwnerReferenceBuilder().build();
		Container container = new ContainerBuilder().build();
		Pod pod1 = KubernetesRMUtils
			.createTaskManagerPod(taskManagerPodLabels, taskManagerPodNamePrefix + "1",
				taskManagerConfigMapName, ownerReference, container, new ConfigMapBuilder().build());
		Pod pod2 = KubernetesRMUtils
			.createTaskManagerPod(taskManagerPodLabels, taskManagerPodNamePrefix + "2",
				taskManagerConfigMapName, ownerReference, container, new ConfigMapBuilder().build());
		kubernetesConnectionManager.createPod(pod1);
		kubernetesConnectionManager.createPod(pod2);
		KubernetesSessionResourceManager spyKubernetesSessionRM =
			mockResourceManager(flinkConf, kubernetesConnectionManager);
		// TM register check always return true
		Mockito.doNothing().when(spyKubernetesSessionRM).checkTMRegistered(Matchers.any());

		// start session RM, get previous 2 worker nodes
		spyKubernetesSessionRM.start();
		Assert.assertEquals(2, spyKubernetesSessionRM.getNumberAllocatedWorkers());
		Assert.assertEquals(TASK_MANAGER_COUNT - 2, spyKubernetesSessionRM.getPendingWorkerNodes().size());

		// add worker nodes
		int podNum = spyKubernetesSessionRM.getNumberAllocatedWorkers();
		while (podNum < TASK_MANAGER_COUNT) {
			spyKubernetesSessionRM.handlePodMessage(Watcher.Action.ADDED, createPod(++podNum));
			Assert.assertEquals(podNum, spyKubernetesSessionRM.getNumberAllocatedWorkers());
			Assert.assertEquals(TASK_MANAGER_COUNT - podNum, spyKubernetesSessionRM.getPendingWorkerNodes().size());
		}
		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getNumberAllocatedWorkers());
		Assert.assertEquals(0, spyKubernetesSessionRM.getPendingWorkerNodes().size());

		// task manager register timeout will never happen
		Mockito.verify(spyKubernetesSessionRM,
			Mockito.times(TASK_MANAGER_COUNT - 2)).requestNewWorkerNode();

		spyKubernetesSessionRM.postStop();
	}

	@Test
	public void testTaskManagerRegisterTimeout() throws Exception {
		KubernetesConnectionManager kubernetesConnectionManager =
			new KubernetesRMTestBase.TestingKubernetesConnectionManager(flinkConf);
		KubernetesSessionResourceManager spyKubernetesSessionRM =
			mockResourceManager(flinkConf, kubernetesConnectionManager);

		// start session RM
		spyKubernetesSessionRM.start();
		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getPendingWorkerNodes().size());
		Assert.assertEquals(0, spyKubernetesSessionRM.getNumberAllocatedWorkers());

		// add worker nodes
		int podNum = spyKubernetesSessionRM.getNumberAllocatedWorkers();
		while (podNum < TASK_MANAGER_COUNT) {
			spyKubernetesSessionRM.handlePodMessage(Watcher.Action.ADDED, createPod(++podNum));
			Assert.assertEquals(podNum, spyKubernetesSessionRM.getNumberAllocatedWorkers());
			Assert.assertEquals(TASK_MANAGER_COUNT - podNum,
				spyKubernetesSessionRM.getPendingWorkerNodes().size());
		}
		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getNumberAllocatedWorkers());
		Assert.assertEquals(0, spyKubernetesSessionRM.getPendingWorkerNodes().size());

		// trigger task manager register timeout
		spyKubernetesSessionRM.getWorkerNodes().keySet().stream().forEach(spyKubernetesSessionRM::checkTMRegistered);
		Mockito.verify(spyKubernetesSessionRM, Mockito.times(TASK_MANAGER_COUNT * 2))
			.requestNewWorkerNode();

		Assert.assertEquals(0, spyKubernetesSessionRM.getNumberAllocatedWorkers());
		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getPendingWorkerNodes().size());

		while (podNum < TASK_MANAGER_COUNT * 2) {
			spyKubernetesSessionRM.handlePodMessage(Watcher.Action.ADDED, createPod(++podNum));
			Assert.assertEquals(podNum - TASK_MANAGER_COUNT, spyKubernetesSessionRM.getNumberAllocatedWorkers());
			Assert.assertEquals(TASK_MANAGER_COUNT * 2 - podNum,
				spyKubernetesSessionRM.getPendingWorkerNodes().size());
		}

		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getNumberAllocatedWorkers());
		Assert.assertEquals(0, spyKubernetesSessionRM.getPendingWorkerNodes().size());

		spyKubernetesSessionRM.postStop();
	}

	@Test
	public void testExitWhenReachMaxFailedAttempts() throws Exception {
		// set max failed attempts = tm-count - 1
		int maxFailedAttempts = TASK_MANAGER_COUNT - 1;
		Configuration newFlinkConf = new Configuration(flinkConf);
		newFlinkConf.setInteger(KubernetesConfigOptions.WORKER_NODE_MAX_FAILED_ATTEMPTS, maxFailedAttempts);
		KubernetesConnectionManager kubernetesConnectionManager =
			new KubernetesRMTestBase.TestingKubernetesConnectionManager(newFlinkConf);
		KubernetesSessionResourceManager spyKubernetesSessionRM =
			mockResourceManager(newFlinkConf, kubernetesConnectionManager);

		// start session RM
		spyKubernetesSessionRM.start();
		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getPendingWorkerNodes().size());
		Assert.assertEquals(0, spyKubernetesSessionRM.getNumberAllocatedWorkers());

		// add worker nodes
		int podNum = spyKubernetesSessionRM.getNumberAllocatedWorkers();
		while (podNum < TASK_MANAGER_COUNT) {
			spyKubernetesSessionRM.handlePodMessage(Watcher.Action.ADDED, createPod(++podNum));
			Assert.assertEquals(podNum, spyKubernetesSessionRM.getNumberAllocatedWorkers());
			Assert.assertEquals(TASK_MANAGER_COUNT - podNum, spyKubernetesSessionRM.getPendingWorkerNodes().size());
		}
		Assert.assertEquals(TASK_MANAGER_COUNT, spyKubernetesSessionRM.getNumberAllocatedWorkers());
		Assert.assertEquals(0, spyKubernetesSessionRM.getPendingWorkerNodes().size());

		// trigger task manager register timeout
		spyKubernetesSessionRM.getWorkerNodes().keySet().stream().forEach(spyKubernetesSessionRM::checkTMRegistered);
		// won't add new request for the last attempt
		Assert.assertEquals(maxFailedAttempts - 1, spyKubernetesSessionRM.getPendingWorkerNodes().size());

		Assert.assertTrue(spyKubernetesSessionRM.isStopped());
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
