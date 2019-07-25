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

package org.apache.flink.kubernetes;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.kubeclient.KubeClientFactory;
import org.apache.flink.kubernetes.resourcemanager.KubernetesResourceManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;

import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;

/**
 * Test cases for {@link KubernetesResourceManager}.
 * */
public class KubernetesResourceManagerTest extends KubernetesTestBase {
	private static final Time TIMEOUT = Time.seconds(10L);
	private FlinkKubernetesOptions options = new FlinkKubernetesOptions(new Configuration());

	@Before
	public void setup() {

		options.setClusterId("test-rm");
		options.setNameSpace("test");
		options.setImageName("test-image");
	}

	public JobManagerMetricGroup mockJMMetricGroup =
		UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup();

	class MockResourceManagerRuntimeServices {

		private final ScheduledExecutor scheduledExecutor;

		private final TestingHighAvailabilityServices highAvailabilityServices;
		private final HeartbeatServices heartbeatServices;
		private final MetricRegistry metricRegistry;
		private final TestingLeaderElectionService rmLeaderElectionService;
		private final JobLeaderIdService jobLeaderIdService;
		private final SlotManager slotManager;

		private UUID rmLeaderSessionId;

		MockResourceManagerRuntimeServices(TestingRpcService rpcService) throws Exception {
			scheduledExecutor = mock(ScheduledExecutor.class);
			highAvailabilityServices = new TestingHighAvailabilityServices();
			rmLeaderElectionService = new TestingLeaderElectionService();
			highAvailabilityServices.setResourceManagerLeaderElectionService(rmLeaderElectionService);
			heartbeatServices = new HeartbeatServices(5L, 5L);
			metricRegistry = NoOpMetricRegistry.INSTANCE;
			slotManager = new SlotManager(
				new ScheduledExecutorServiceAdapter(new DirectScheduledExecutorService()),
				Time.seconds(10), Time.seconds(10), Time.minutes(1), true);
			jobLeaderIdService = new JobLeaderIdService(
				highAvailabilityServices,
				rpcService.getScheduledExecutor(),
				Time.minutes(5L));
		}

		void grantLeadership() throws Exception {
			rmLeaderSessionId = UUID.randomUUID();
			rmLeaderElectionService.isLeader(rmLeaderSessionId).get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
		}
	}

	private KubernetesResourceManager createResourceManager(FlinkKubernetesOptions options) throws Exception {

		KubeClientFactory.setkubernetesClient(server.getClient());

		TestingRpcService rpcService = new TestingRpcService(options.getConfiguration());
		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		MockResourceManagerRuntimeServices rmServices = new MockResourceManagerRuntimeServices(rpcService);
		return new KubernetesResourceManager(
			options,
			rpcService,
			"k8sRm",
			new ResourceID(""),
			rmServices.highAvailabilityServices,
			rmServices.heartbeatServices,
			rmServices.slotManager,
			rmServices.metricRegistry,
			rmServices.jobLeaderIdService,
			new ClusterInformation("localhost", 1234),
			testingFatalErrorHandler,
			mockJMMetricGroup
		);
	}

	@Test
	public void testCreateWorker() throws Exception {

		KubernetesResourceManager resourceManager = this.createResourceManager(options);
		ResourceProfile profile = new ResourceProfile(1.1, 2);
		resourceManager.onStart();
		resourceManager.startNewWorker(profile);

		KubernetesClient client = server.getClient();
		PodList list = client.pods().list();
		Assert.assertEquals(1, list.getItems().size());

		Pod pod = list.getItems().get(0);

		Assert.assertTrue(pod.getMetadata().getName().startsWith(KubernetesResourceManager.TASKMANAGER_ID_PREFIX));
		Assert.assertEquals(1, pod.getSpec().getContainers().size());

		//checkArguments
		List<String> args = Arrays.asList(
			"taskmanager",
			" -D",
			"jobmanager.rpc.address=" + resourceManager.getRpcService().getAddress(),
			" -D",
			"jobmanager.rpc.port=" + resourceManager.getRpcService().getPort()
		);

		Assert.assertArrayEquals(args.toArray(), pod.getSpec().getContainers().get(0).getArgs().toArray());

		EnvVar[] expectedEnv = new EnvVar[]{new EnvVar(KubernetesResourceManager.ENV_RESOURCE_ID, pod.getMetadata().getName(), null)};

		Assert.assertArrayEquals(expectedEnv, pod
			.getSpec()
			.getContainers()
			.get(0)
			.getEnv()
			.stream()
			.toArray(EnvVar[]::new));
	}

	@Test
	public void testStopWorker() throws Exception {
		KubernetesResourceManager resourceManager = this.createResourceManager(options);
		ResourceProfile profile = new ResourceProfile(1.1, 2);
		resourceManager.onStart();
		resourceManager.startNewWorker(profile);

		KubernetesClient client = server.getClient();
		PodList list = client.pods().list();
		Assert.assertEquals(1, list.getItems().size());

		ConcurrentMap<ResourceID, KubernetesResourceManager.KubernetesWorkerNode> map = resourceManager.getWorkerNodeMap();
		Assert.assertEquals(1, map.size());

		Assert.assertTrue(resourceManager.stopWorker(map.values().stream().findFirst().get()));

		list = client.pods().list();
		Assert.assertEquals(0, list.getItems().size());
	}
}
