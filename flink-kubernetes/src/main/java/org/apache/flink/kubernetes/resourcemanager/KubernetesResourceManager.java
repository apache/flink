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

package org.apache.flink.kubernetes.resourcemanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.FlinkKubernetesOptions;
import org.apache.flink.kubernetes.kubeclient.KubeClient;
import org.apache.flink.kubernetes.kubeclient.KubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.TaskManagerPodParameter;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Kubernetes specific implementation of the {@link ResourceManager}.
 */
public class KubernetesResourceManager extends ResourceManager<KubernetesResourceManager.KubernetesWorkerNode> {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesResourceManager.class);

	public static final String TASKMANAGER_ID_PREFIX = "flink-taskmanager-";

	public static final String ENV_RESOURCE_ID = "RESOURCE_ID";

	private final ConcurrentMap<ResourceID, KubernetesWorkerNode> workerNodeMap;

	private final Collection<ResourceProfile> slotsPerWorker;

	private final FlinkKubernetesOptions flinkKubernetesOptions;

	private KubeClient kubeClient;

	public KubernetesResourceManager(
		FlinkKubernetesOptions flinkKubernetesOptions,
		RpcService rpcService,
		String resourceManagerEndpointId,
		ResourceID resourceId,
		HighAvailabilityServices highAvailabilityServices,
		HeartbeatServices heartbeatServices,
		SlotManager slotManager,
		MetricRegistry metricRegistry,
		JobLeaderIdService jobLeaderIdService,
		ClusterInformation clusterInformation,
		FatalErrorHandler fatalErrorHandler,
		JobManagerMetricGroup jobManagerMetricGroup) {
		super(rpcService, resourceManagerEndpointId, resourceId, highAvailabilityServices, heartbeatServices, slotManager, metricRegistry, jobLeaderIdService, clusterInformation, fatalErrorHandler, jobManagerMetricGroup);

		this.flinkKubernetesOptions = flinkKubernetesOptions;
		this.workerNodeMap = new ConcurrentHashMap<>();

		int numberOfTaskSlots = flinkKubernetesOptions.getConfiguration().getInteger(TaskManagerOptions.NUM_TASK_SLOTS);
		this.slotsPerWorker = createSlotsPerWorker(numberOfTaskSlots);

	}

	@Override
	protected void initialize() {
		LOG.info("Initializing Kubernetes client.");
		LOG.info("KubernetesResourceManager.initialize clusterId:" + this.flinkKubernetesOptions.getClusterId());
		LOG.info("KubernetesResourceManager.initialize image:" + this.flinkKubernetesOptions.getImageName());

		this.kubeClient = KubeClientFactory.fromConfiguration(this.flinkKubernetesOptions);
		this.kubeClient.initialize();
	}

	@Override
	protected void internalDeregisterApplication(ApplicationStatus finalStatus, @Nullable String optionalDiagnostics) {
		LOG.info("Stopping kubernetes cluster, id: {0}", this.flinkKubernetesOptions.getClusterId());
		this.kubeClient.stopAndCleanupCluster(this.flinkKubernetesOptions.getClusterId());
	}

	@Override
	public Collection<ResourceProfile> startNewWorker(ResourceProfile resourceProfile) {

		Preconditions.checkNotNull(this.kubeClient);
		String podName = TASKMANAGER_ID_PREFIX + UUID.randomUUID();
		String imageName = flinkKubernetesOptions.getImageName();
		Preconditions.checkNotNull(imageName);
		LOG.info("creating new worker, worker podName: {}", podName);

		try {
			List<String> args = Arrays.asList(
				"taskmanager",
				" -D",
				"jobmanager.rpc.address=" + getRpcService().getAddress(),
				" -D",
				"jobmanager.rpc.port=" + getRpcService().getPort()
			);

			HashMap<String, String> env = new HashMap<>();
			env.put(ENV_RESOURCE_ID, podName);

			TaskManagerPodParameter parameter = new TaskManagerPodParameter(
				podName,
				imageName,
				args,
				resourceProfile,
				env);

			this.kubeClient.createTaskManagerPod(parameter);
			KubernetesWorkerNode worker = new KubernetesWorkerNode(new ResourceID(podName));
			workerNodeMap.put(worker.getResourceID(), worker);
			return slotsPerWorker;
		} catch (Exception e) {
			this.kubeClient.logException(e);
			throw new FlinkRuntimeException("Could not start new worker");
		}
	}

	@Override
	protected KubernetesWorkerNode workerStarted(ResourceID resourceID) {
		LOG.info("Worker {} started.", resourceID.toString());
		return workerNodeMap.get(resourceID);
	}

	@Override
	public boolean stopWorker(KubernetesWorkerNode worker) {
		Preconditions.checkNotNull(this.kubeClient);
		LOG.info("Stopping Worker {}.", worker.getResourceID().toString());
		try {
			this.kubeClient.stopPod(worker.getResourceID().toString());
		} catch (Exception e) {
			this.kubeClient.logException(e);
			return false;
		}
		return true;
	}

	@VisibleForTesting
	public ConcurrentMap<ResourceID, KubernetesWorkerNode> getWorkerNodeMap() {
		return workerNodeMap;
	}

	/**
	 * Kubernetes specific implementation of the {@link ResourceIDRetrievable}.
	 */
	public static class KubernetesWorkerNode implements ResourceIDRetrievable {
		private final ResourceID resourceID;

		KubernetesWorkerNode(ResourceID resourceID) {
			this.resourceID = resourceID;
		}

		@Override
		public ResourceID getResourceID() {
			return resourceID;
		}
	}
}
