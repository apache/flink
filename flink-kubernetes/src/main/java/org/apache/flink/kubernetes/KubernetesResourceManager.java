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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesResourceManagerConfiguration;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.factory.KubernetesTaskManagerFactory;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesTaskManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ActiveResourceManager;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Kubernetes specific implementation of the {@link ResourceManager}.
 */
public class KubernetesResourceManager extends ActiveResourceManager<KubernetesWorkerNode>
	implements FlinkKubeClient.PodCallbackHandler {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesResourceManager.class);

	/** The taskmanager pod name pattern is {clusterId}-{taskmanager}-{attemptId}-{podIndex}. */
	private static final String TASK_MANAGER_POD_FORMAT = "%s-taskmanager-%d-%d";

	private final Map<ResourceID, KubernetesWorkerNode> workerNodes = new HashMap<>();

	/** When ResourceManager failover, the max attempt should recover. */
	private long currentMaxAttemptId = 0;

	/** Current max pod index. When creating a new pod, it should increase one. */
	private long currentMaxPodId = 0;

	private final String clusterId;

	private final FlinkKubeClient kubeClient;

	private final KubernetesResourceManagerConfiguration configuration;

	/** Map from pod name to worker resource. */
	private final Map<String, WorkerResourceSpec> podWorkerResources;

	private KubernetesWatch podsWatch;

	public KubernetesResourceManager(
			RpcService rpcService,
			ResourceID resourceId,
			Configuration flinkConfig,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			ResourceManagerMetricGroup resourceManagerMetricGroup,
			FlinkKubeClient kubeClient,
			KubernetesResourceManagerConfiguration configuration) {
		super(
			flinkConfig,
			System.getenv(),
			rpcService,
			resourceId,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			clusterPartitionTrackerFactory,
			jobLeaderIdService,
			clusterInformation,
			fatalErrorHandler,
			resourceManagerMetricGroup);
		this.clusterId = configuration.getClusterId();
		this.kubeClient = kubeClient;
		this.configuration = configuration;
		this.podWorkerResources = new HashMap<>();
	}

	@Override
	protected Configuration loadClientConfiguration() {
		return GlobalConfiguration.loadConfiguration();
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		recoverWorkerNodesFromPreviousAttempts();

		podsWatch = kubeClient.watchPodsAndDoCallback(
			KubernetesUtils.getTaskManagerLabels(clusterId),
			this);
	}

	@Override
	public CompletableFuture<Void> onStop() {
		// shut down all components
		Throwable throwable = null;

		try {
			podsWatch.close();
		} catch (Throwable t) {
			throwable = t;
		}

		try {
			kubeClient.close();
		} catch (Throwable t) {
			throwable = ExceptionUtils.firstOrSuppressed(t, throwable);
		}

		return getStopTerminationFutureOrCompletedExceptionally(throwable);
	}

	@Override
	protected void internalDeregisterApplication(ApplicationStatus finalStatus, @Nullable String diagnostics) {
		LOG.info(
			"Stopping kubernetes cluster, clusterId: {}, diagnostics: {}",
			clusterId,
			diagnostics == null ? "" : diagnostics);
		kubeClient.stopAndCleanupCluster(clusterId);
	}

	@Override
	public boolean startNewWorker(WorkerResourceSpec workerResourceSpec) {
		LOG.info("Starting new worker with worker resource spec, {}", workerResourceSpec);
		requestKubernetesPod(workerResourceSpec);
		return true;
	}

	@Override
	protected KubernetesWorkerNode workerStarted(ResourceID resourceID) {
		return workerNodes.get(resourceID);
	}

	@Override
	public boolean stopWorker(final KubernetesWorkerNode worker) {
		final ResourceID resourceId = worker.getResourceID();
		LOG.info("Stopping Worker {}.", resourceId);
		internalStopPod(resourceId.toString());
		return true;
	}

	@Override
	public void onAdded(List<KubernetesPod> pods) {
		runAsync(() -> {
			int duplicatePodNum = 0;
			for (KubernetesPod pod : pods) {
				final String podName = pod.getName();
				final ResourceID resourceID = new ResourceID(podName);

				if (workerNodes.containsKey(resourceID)) {
					log.debug("Ignore TaskManager pod that is already added: {}", podName);
					++duplicatePodNum;
					continue;
				}

				final WorkerResourceSpec workerResourceSpec = Preconditions.checkNotNull(
					podWorkerResources.get(podName),
					"Unrecognized pod {}. Pods from previous attempt should have already been added.", podName);

				final int pendingNum = getNumRequestedNotAllocatedWorkersFor(workerResourceSpec);
				Preconditions.checkState(pendingNum > 0, "Should not receive more workers than requested.");

				notifyNewWorkerAllocated(workerResourceSpec, resourceID);
				final KubernetesWorkerNode worker = new KubernetesWorkerNode(resourceID);
				workerNodes.put(resourceID, worker);

				log.info("Received new TaskManager pod: {}", podName);
			}
			log.info("Received {} new TaskManager pods. Remaining pending pod requests: {}",
				pods.size() - duplicatePodNum, getNumRequestedNotAllocatedWorkers());
		});
	}

	@Override
	public void onModified(List<KubernetesPod> pods) {
		runAsync(() -> pods.forEach(this::removePodAndTryRestartIfRequired));
	}

	@Override
	public void onDeleted(List<KubernetesPod> pods) {
		runAsync(() -> pods.forEach(this::removePodAndTryRestartIfRequired));
	}

	@Override
	public void onError(List<KubernetesPod> pods) {
		runAsync(() -> pods.forEach(this::removePodAndTryRestartIfRequired));
	}

	@Override
	public void handleFatalError(Throwable throwable) {
		onFatalError(throwable);
	}

	@VisibleForTesting
	Map<ResourceID, KubernetesWorkerNode> getWorkerNodes() {
		return workerNodes;
	}

	private void recoverWorkerNodesFromPreviousAttempts() throws ResourceManagerException {
		final List<KubernetesPod> podList = kubeClient.getPodsWithLabels(KubernetesUtils.getTaskManagerLabels(clusterId));
		for (KubernetesPod pod : podList) {
			final KubernetesWorkerNode worker = new KubernetesWorkerNode(new ResourceID(pod.getName()));
			workerNodes.put(worker.getResourceID(), worker);
			final long attempt = worker.getAttempt();
			if (attempt > currentMaxAttemptId) {
				currentMaxAttemptId = attempt;
			}
		}

		log.info("Recovered {} pods from previous attempts, current attempt id is {}.",
			workerNodes.size(),
			++currentMaxAttemptId);
	}

	private void requestKubernetesPod(WorkerResourceSpec workerResourceSpec) {
		final KubernetesTaskManagerParameters parameters =
			createKubernetesTaskManagerParameters(workerResourceSpec);

		podWorkerResources.put(parameters.getPodName(), workerResourceSpec);
		final int pendingWorkerNum = notifyNewWorkerRequested(workerResourceSpec).getNumNotAllocated();

		log.info("Requesting new TaskManager pod with <{},{}>. Number pending requests {}.",
			parameters.getTaskManagerMemoryMB(),
			parameters.getTaskManagerCPU(),
			pendingWorkerNum);

		final KubernetesPod taskManagerPod =
			KubernetesTaskManagerFactory.buildTaskManagerKubernetesPod(parameters);
		kubeClient.createTaskManagerPod(taskManagerPod)
			.whenCompleteAsync(
				(ignore, throwable) -> {
					if (throwable != null) {
						final Time retryInterval = configuration.getPodCreationRetryInterval();
						log.warn("Could not start TaskManager in pod {}, retry in {}. ",
							taskManagerPod.getName(), retryInterval, throwable);
						podWorkerResources.remove(parameters.getPodName());
						notifyNewWorkerAllocationFailed(workerResourceSpec);
						scheduleRunAsync(
							this::requestKubernetesPodIfRequired,
							retryInterval);
					} else {
						log.info("TaskManager {} will be started with {}.", parameters.getPodName(), workerResourceSpec);
					}
				},
				getMainThreadExecutor());
	}

	private KubernetesTaskManagerParameters createKubernetesTaskManagerParameters(WorkerResourceSpec workerResourceSpec) {
		final TaskExecutorProcessSpec taskExecutorProcessSpec =
			TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, workerResourceSpec);

		final String podName = String.format(
			TASK_MANAGER_POD_FORMAT,
			clusterId,
			currentMaxAttemptId,
			++currentMaxPodId);

		final ContaineredTaskManagerParameters taskManagerParameters =
			ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec);

		final String dynamicProperties =
			BootstrapTools.getDynamicPropertiesAsString(flinkClientConfig, flinkConfig);

		return new KubernetesTaskManagerParameters(
			flinkConfig,
			podName,
			dynamicProperties,
			taskManagerParameters,
			ExternalResourceUtils.getExternalResources(flinkConfig, KubernetesConfigOptions.EXTERNAL_RESOURCE_KUBERNETES_CONFIG_KEY_SUFFIX));
	}

	/**
	 * Request new pod if pending pods cannot satisfy pending slot requests.
	 */
	private void requestKubernetesPodIfRequired() {
		for (Map.Entry<WorkerResourceSpec, Integer> entry : getRequiredResources().entrySet()) {
			final WorkerResourceSpec workerResourceSpec = entry.getKey();
			final int requiredTaskManagers = entry.getValue();

			while (requiredTaskManagers > getNumRequestedNotRegisteredWorkersFor(workerResourceSpec)) {
				requestKubernetesPod(workerResourceSpec);
			}
		}
	}

	private void removePodAndTryRestartIfRequired(KubernetesPod pod) {
		if (pod.isTerminated()) {
			internalStopPod(pod.getName());
			requestKubernetesPodIfRequired();
		}
	}

	private void internalStopPod(String podName) {
		final ResourceID resourceId = new ResourceID(podName);
		final boolean isPendingWorkerOfCurrentAttempt = isPendingWorkerOfCurrentAttempt(podName);

		kubeClient.stopPod(podName)
			.whenComplete(
				(ignore, throwable) -> {
					if (throwable != null) {
						log.warn("Could not stop TaskManager in pod {}.", podName, throwable);
					}
				}
			);

		final WorkerResourceSpec workerResourceSpec = podWorkerResources.remove(podName);
		workerNodes.remove(resourceId);

		if (isPendingWorkerOfCurrentAttempt) {
			notifyNewWorkerAllocationFailed(
				Preconditions.checkNotNull(workerResourceSpec,
					"Worker resource spec of current attempt pending worker should be known."));
		} else {
			notifyAllocatedWorkerStopped(resourceId);
		}
	}

	private boolean isPendingWorkerOfCurrentAttempt(String podName) {
		return podWorkerResources.containsKey(podName) &&
			!workerNodes.containsKey(new ResourceID(podName));
	}
}
