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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.TaskManagerPodParameter;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.TaskExecutorRegistration;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.utils.MockResourceManagerRuntimeServices;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerStateBuilder;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Test cases for {@link KubernetesResourceManager}.
 */
public class KubernetesResourceManagerTest extends KubernetesTestBase {

	private static final Time TIMEOUT = Time.seconds(10L);

	private TestingFatalErrorHandler testingFatalErrorHandler;

	private final String jobManagerHost = "jm-host1";

	private Configuration flinkConfig;

	private TestingKubernetesResourceManager resourceManager;

	private FlinkKubeClient flinkKubeClient;

	@Before
	public void setup() throws Exception {
		testingFatalErrorHandler = new TestingFatalErrorHandler();
		flinkConfig = new Configuration(FLINK_CONFIG);
		flinkConfig.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1024m"));

		flinkKubeClient = getFabric8FlinkKubeClient();
		resourceManager = createAndStartResourceManager(flinkConfig);
	}

	@After
	public void teardown() throws Exception {
		resourceManager.close();
		if (testingFatalErrorHandler != null) {
			testingFatalErrorHandler.rethrowError();
		}
	}

	class TestingKubernetesResourceManager extends KubernetesResourceManager {

		private final SlotManager slotManager;

		TestingKubernetesResourceManager(
				RpcService rpcService,
				String resourceManagerEndpointId,
				ResourceID resourceId,
				Configuration flinkConfig,
				HighAvailabilityServices highAvailabilityServices,
				HeartbeatServices heartbeatServices,
				SlotManager slotManager,
				JobLeaderIdService jobLeaderIdService,
				ClusterInformation clusterInformation,
				FatalErrorHandler fatalErrorHandler,
				ResourceManagerMetricGroup resourceManagerMetricGroup) {
			super(
				rpcService,
				resourceManagerEndpointId,
				resourceId,
				flinkConfig,
				highAvailabilityServices,
				heartbeatServices,
				slotManager,
				jobLeaderIdService,
				clusterInformation,
				fatalErrorHandler,
				resourceManagerMetricGroup
			);
			this.slotManager = slotManager;
		}

		<T> CompletableFuture<T> runInMainThread(Callable<T> callable) {
			return callAsync(callable, TIMEOUT);
		}

		@Override
		protected void runAsync(Runnable runnable) {
			runnable.run();
		}

		@Override
		protected FlinkKubeClient createFlinkKubeClient() {
			return flinkKubeClient;
		}

		MainThreadExecutor getMainThreadExecutorForTesting() {
			return super.getMainThreadExecutor();
		}

		SlotManager getSlotManager() {
			return this.slotManager;
		}
	}

	@Test
	public void testStartAndStopWorker() throws Exception {
		registerSlotRequest();

		final KubernetesClient client = getKubeClient();
		final PodList list = client.pods().list();
		assertEquals(1, list.getItems().size());
		final Pod pod = list.getItems().get(0);

		final Map<String, String> labels = getCommonLabels();
		labels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
		assertEquals(labels, pod.getMetadata().getLabels());

		assertEquals(1, pod.getSpec().getContainers().size());
		final Container tmContainer = pod.getSpec().getContainers().get(0);
		assertEquals(CONTAINER_IMAGE, tmContainer.getImage());

		final String podName = CLUSTER_ID + "-taskmanager-1-1";
		assertEquals(podName, pod.getMetadata().getName());

		// Check environments
		assertThat(tmContainer.getEnv(), Matchers.contains(
			new EnvVarBuilder().withName(Constants.ENV_FLINK_POD_NAME).withValue(podName).build()));

		// Check task manager main class args.
		assertEquals(3, tmContainer.getArgs().size());
		final String confDirOption = "--configDir " + flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR);
		assertTrue(tmContainer.getArgs().get(2).contains(confDirOption));

		resourceManager.onAdded(Collections.singletonList(new KubernetesPod(flinkConfig, pod)));
		final ResourceID resourceID = new ResourceID(podName);
		assertThat(resourceManager.getWorkerNodes().keySet(), Matchers.contains(resourceID));

		registerTaskExecutor(resourceID);

		// Unregister all task executors and release all task managers.
		CompletableFuture<?> unregisterAndReleaseFuture = resourceManager.runInMainThread(() -> {
			resourceManager.getSlotManager().unregisterTaskManagersAndReleaseResources();
			return null;
		});
		unregisterAndReleaseFuture.get();
		assertEquals(0, client.pods().list().getItems().size());
		assertEquals(0, resourceManager.getWorkerNodes().size());
	}

	@Test
	public void testTaskManagerPodTerminated() throws Exception {
		registerSlotRequest();

		final KubernetesClient client = getKubeClient();
		final Pod pod1 = client.pods().list().getItems().get(0);
		final String taskManagerPrefix = CLUSTER_ID + "-taskmanager-1-";

		resourceManager.onAdded(Collections.singletonList(new KubernetesPod(flinkConfig, pod1)));

		// General modification event
		resourceManager.onModified(Collections.singletonList(new KubernetesPod(flinkConfig, pod1)));
		assertEquals(1, client.pods().list().getItems().size());
		assertEquals(taskManagerPrefix + 1, client.pods().list().getItems().get(0).getMetadata().getName());

		// Terminate the pod.
		terminatePod(pod1);
		resourceManager.onModified(Collections.singletonList(new KubernetesPod(flinkConfig, pod1)));

		// Old pod should be deleted and a new task manager should be created
		assertEquals(1, client.pods().list().getItems().size());
		final Pod pod2 = client.pods().list().getItems().get(0);
		assertEquals(taskManagerPrefix + 2, pod2.getMetadata().getName());

		// Error happens in the pod.
		resourceManager.onAdded(Collections.singletonList(new KubernetesPod(flinkConfig, pod2)));
		terminatePod(pod2);
		resourceManager.onError(Collections.singletonList(new KubernetesPod(flinkConfig, pod2)));
		final Pod pod3 = client.pods().list().getItems().get(0);
		assertEquals(taskManagerPrefix + 3, pod3.getMetadata().getName());

		// Delete the pod.
		resourceManager.onAdded(Collections.singletonList(new KubernetesPod(flinkConfig, pod3)));
		terminatePod(pod3);
		resourceManager.onDeleted(Collections.singletonList(new KubernetesPod(flinkConfig, pod3)));
		assertEquals(taskManagerPrefix + 4, client.pods().list().getItems().get(0).getMetadata().getName());
	}

	@Test
	public void testGetWorkerNodesFromPreviousAttempts() throws Exception {
		// Prepare pod of previous attempt
		final String previewPodName = CLUSTER_ID + "-taskmanager-1-1";
		flinkKubeClient.createTaskManagerPod(new TaskManagerPodParameter(
			previewPodName,
			new ArrayList<>(),
			1024,
			1,
			new HashMap<>()));
		final KubernetesClient client = getKubeClient();
		assertEquals(1, client.pods().list().getItems().size());

		// Call initialize method to recover worker nodes from previous attempt.
		resourceManager.initialize();

		final String taskManagerPrefix = CLUSTER_ID + "-taskmanager-";
		// Register the previous taskmanager, no new pod should be created
		registerTaskExecutor(new ResourceID(previewPodName));
		registerSlotRequest();
		assertEquals(1, client.pods().list().getItems().size());

		// Register a new slot request, a new taskmanger pod will be created with attempt2
		registerSlotRequest();
		assertEquals(2, client.pods().list().getItems().size());
		assertThat(client.pods().list().getItems().stream()
				.map(e -> e.getMetadata().getName())
				.collect(Collectors.toList()),
			Matchers.containsInAnyOrder(taskManagerPrefix + "1-1", taskManagerPrefix + "2-1"));
	}

	private TestingKubernetesResourceManager createAndStartResourceManager(Configuration configuration) throws Exception {

		final TestingRpcService rpcService = new TestingRpcService(configuration);
		final MockResourceManagerRuntimeServices rmServices = new MockResourceManagerRuntimeServices(rpcService, TIMEOUT);

		final TestingKubernetesResourceManager kubernetesResourceManager = new TestingKubernetesResourceManager(
			rpcService,
			"kubernetesResourceManager",
			ResourceID.generate(),
			configuration,
			rmServices.highAvailabilityServices,
			rmServices.heartbeatServices,
			rmServices.slotManager,
			rmServices.jobLeaderIdService,
			new ClusterInformation("localhost", 1234),
			testingFatalErrorHandler,
			UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup()
		);
		kubernetesResourceManager.start();
		rmServices.grantLeadership();
		return kubernetesResourceManager;
	}

	private void registerSlotRequest() throws Exception {
		CompletableFuture<?> registerSlotRequestFuture = resourceManager.runInMainThread(() -> {
			resourceManager.getSlotManager().registerSlotRequest(
				new SlotRequest(new JobID(), new AllocationID(), ResourceProfile.UNKNOWN, jobManagerHost));
			return null;
		});
		registerSlotRequestFuture.get();
	}

	private void registerTaskExecutor(ResourceID resourceID) throws Exception {
		final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.createTestingTaskExecutorGateway();
		((TestingRpcService) resourceManager.getRpcService()).registerGateway(resourceID.toString(), taskExecutorGateway);

		final ResourceManagerGateway rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);

		final SlotReport slotReport = new SlotReport(new SlotStatus(new SlotID(resourceID, 1), ResourceProfile.ZERO));

		TaskExecutorRegistration taskExecutorRegistration = new TaskExecutorRegistration(
			resourceID.toString(),
			resourceID,
			1234,
			new HardwareDescription(1, 2L, 3L, 4L),
			ResourceProfile.ZERO,
			ResourceProfile.ZERO);
		CompletableFuture<Integer> numberRegisteredSlotsFuture = rmGateway
			.registerTaskExecutor(
				taskExecutorRegistration,
				TIMEOUT)
			.thenCompose(
				(RegistrationResponse response) -> {
					assertThat(response, instanceOf(TaskExecutorRegistrationSuccess.class));
					final TaskExecutorRegistrationSuccess success = (TaskExecutorRegistrationSuccess) response;
					return rmGateway.sendSlotReport(
						resourceID,
						success.getRegistrationId(),
						slotReport,
						TIMEOUT);
				})
			.handleAsync(
				(Acknowledge ignored, Throwable throwable) -> resourceManager.getSlotManager().getNumberRegisteredSlots(),
				resourceManager.getMainThreadExecutorForTesting());
		Assert.assertEquals(1, numberRegisteredSlotsFuture.get().intValue());
	}

	private void terminatePod(Pod pod) {
		pod.setStatus(new PodStatusBuilder()
			.withContainerStatuses(new ContainerStatusBuilder().withState(
				new ContainerStateBuilder().withNewTerminated().endTerminated().build())
				.build())
			.build());
	}
}
