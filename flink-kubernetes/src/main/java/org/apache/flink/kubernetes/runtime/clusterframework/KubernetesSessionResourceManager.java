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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.Constants;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.KubernetesConnectionManager;
import org.apache.flink.kubernetes.utils.KubernetesRMUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.TaskManagerResource;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.DynamicAssigningSlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * The Kubernetes implementation of the resource manager. Used when the system is started
 * via the resource framework Kubernetes.
 */
public class KubernetesSessionResourceManager extends
	ResourceManager<KubernetesWorkerNode> {

	/**
	 * Kubernetes pod map. Package private for unit test purposes.
	 */
	final ConcurrentMap<ResourceID, KubernetesWorkerNode> workerNodeMap;

	private final Configuration flinkConfig;

	private ConfigMap tmConfigMap;

	/**
	 * Connection manager to communicate with Kubernetes.
	 */
	private KubernetesConnectionManager kubernetesConnectionManager;

	/** The number of containers requested. **/
	private final int workerNum;

	/** The pending pod requests, but not yet granted. */
	private final Set<ResourceID> pendingWorkerNodes;

	private final Time taskManagerRegisterTimeout;

	private final TaskManagerResource taskManagerResource;

	private Watch watcher;

	private final String clusterId;

	private final Map<String, String> taskManagerPodLabels;

	private final String taskManagerPodNamePrefix;

	private final String taskManagerConfigMapName;

	private final AtomicLong maxPodId = new AtomicLong(0);

	private final String confDir;

	private volatile boolean isStopped;

	private final int workerNodeMaxFailedAttempts;

	private final AtomicInteger workerNodeFailedAttempts = new AtomicInteger(0);

	private OwnerReference ownerReference;

	private BiConsumer<Watcher.Action, Pod> podEventHandler;

	private Consumer<Exception> watcherCloseHandler;

	public KubernetesSessionResourceManager(
		RpcService rpcService,
		String resourceManagerEndpointId,
		ResourceID resourceId,
		Configuration flinkConfig,
		ResourceManagerConfiguration resourceManagerConfiguration,
		HighAvailabilityServices highAvailabilityServices,
		HeartbeatServices heartbeatServices,
		SlotManager slotManager,
		MetricRegistry metricRegistry,
		JobLeaderIdService jobLeaderIdService,
		ClusterInformation clusterInformation,
		FatalErrorHandler fatalErrorHandler) {
		super(
			rpcService,
			resourceManagerEndpointId,
			resourceId,
			resourceManagerConfiguration,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			metricRegistry,
			jobLeaderIdService,
			clusterInformation,
			fatalErrorHandler);
		this.flinkConfig = flinkConfig;
		this.workerNodeMap = new ConcurrentHashMap<>();
		this.pendingWorkerNodes = new HashSet<>();
		this.confDir = flinkConfig.getString(KubernetesConfigOptions.CONF_DIR);

		taskManagerRegisterTimeout = Time.seconds(flinkConfig
			.getLong(KubernetesConfigOptions.TASK_MANAGER_REGISTER_TIMEOUT));

		workerNodeMaxFailedAttempts = flinkConfig.getInteger(
			KubernetesConfigOptions.WORKER_NODE_MAX_FAILED_ATTEMPTS);

		// build the task manager's total resource according to user's resource
		taskManagerResource = TaskManagerResource.fromConfiguration(flinkConfig,
			KubernetesRMUtils.createTaskManagerResourceProfile(flinkConfig), 1);
		log.info("Task manager resource: " + taskManagerResource);

		if (slotManager instanceof DynamicAssigningSlotManager) {
			((DynamicAssigningSlotManager) slotManager).setTotalResourceOfTaskExecutor(
				taskManagerResource.getTaskResourceProfile());
			log.info("The resource for user in a task executor is {}.", taskManagerResource);
		}

		clusterId = flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID);
		taskManagerPodLabels = new HashMap<>();
		taskManagerPodLabels.put(Constants.LABEL_APP_KEY, clusterId);
		taskManagerPodLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
		taskManagerPodNamePrefix =
			clusterId + Constants.TASK_MANAGER_LABEL_SUFFIX + Constants.NAME_SEPARATOR;
		taskManagerConfigMapName =
			clusterId + Constants.TASK_MANAGER_CONFIG_MAP_SUFFIX;
		workerNum =
			flinkConfig.getInteger(KubernetesConfigOptions.TASK_MANAGER_COUNT);
		log.info("Initialize KubernetesSessionResourceManager: clusterId: {}, "
			+ "workerNum: {}", clusterId, workerNum);
	}

	@VisibleForTesting
	protected void getWorkerNodesFromPreviousAttempts() throws ResourceManagerException {
		PodList podList = kubernetesConnectionManager.getPods(taskManagerPodLabels);
		if (podList != null && podList.getItems().size() > 0) {
			// add worker nodes
			for (Pod pod : podList.getItems()) {
				addWorkerNode(pod, false);
			}
			if (!workerNodeMap.isEmpty()) {
				long maxId = workerNodeMap.values().stream()
					.mapToLong(KubernetesWorkerNode::getPodId).max().getAsLong();
				maxPodId.set(maxId);
			}
			log.info(
				"Recovered {} pods from previous attempts, max pod id is {}.",
				workerNodeMap.size(), maxPodId.get());
		}
	}

	private synchronized KubernetesWorkerNode addWorkerNode(Pod pod, boolean checkPending)
		throws ResourceManagerException {
		String podName = pod.getMetadata().getName();
		ResourceID resourceId = new ResourceID(podName);
		boolean pendingRemoved = pendingWorkerNodes.remove(resourceId);
		if (!pendingRemoved && checkPending) {
			log.warn("Skip adding worker node {} since it's no longer pending!", resourceId);
			kubernetesConnectionManager.removePod(pod);
			return null;
		}
		if (workerNodeMap.containsKey(resourceId)) {
			log.warn("Skip adding worker node {} since it's already exist!", resourceId);
			return workerNodeMap.get(resourceId);
		}
		if (workerNodeMap.size() >= workerNum) {
			log.error("Skip adding worker node {} since the number of worker nodes ({}) is equal with "
				+ "or beyond required ({})", workerNodeMap.size(), workerNum);
			kubernetesConnectionManager.removePod(pod);
			return null;
		}
		if (podName.startsWith(taskManagerPodNamePrefix)) {
			String podId = podName
				.substring(podName.lastIndexOf(Constants.NAME_SEPARATOR) + 1);
			if (StringUtils.isNumeric(podId)) {
				KubernetesWorkerNode workerNode =
					new KubernetesWorkerNode(pod, podName,
						Long.parseLong(podId));
				workerNodeMap.put(workerNode.getResourceID(), workerNode);
				scheduleRunAsync(() -> checkTMRegistered(resourceId), taskManagerRegisterTimeout);
				log.info("Add worker node : {}, worker nodes: {}, pending worker nodes: {} - {}",
					workerNode.getResourceID(), workerNodeMap.size(), pendingWorkerNodes.size(), pendingWorkerNodes);
				return workerNode;
			} else {
				log.warn("Skip adding invalid pod whose name is {} "
					+ "and the last part is not a number.", podName);
				kubernetesConnectionManager.removePod(pod);
			}
		} else {
			log.warn("Skip adding invalid pod whose name is {} and prefix is not {}.",
				podName, taskManagerPodNamePrefix);
			kubernetesConnectionManager.removePod(pod);
		}
		return null;
	}

	private synchronized boolean removeWorkerNode(ResourceID resourceID, String diagnostics, boolean increaseFailedAttempt) {
		if (!workerNodeMap.containsKey(resourceID)) {
			log.info("Skip removing non-exist worker node {}.", resourceID);
			return false;
		}
		log.info("Try to remove worker node: {}, diagnostics: {}", resourceID, diagnostics);
		// If a worker terminated exceptionally, start a new one;
		KubernetesWorkerNode node = workerNodeMap.remove(resourceID);
		if (node != null) {
			try {
				if (increaseFailedAttempt) {
					increaseWorkerNodeFailedAttempts();
				}
				closeTaskManagerConnection(resourceID, new Exception(diagnostics));
				kubernetesConnectionManager.removePod(node.getPod());
			} catch (Exception e) {
				String fatalMsg = "Failed to remove worker node. Exiting, bye...";
				onFatalError(new ResourceManagerException(fatalMsg, e));
			}
			checkWorkerNodeFailedAttempts();
			requestWorkerNodes();
			log.info("Removed worker node: {}, left worker nodes: {}", resourceID, workerNodeMap.size());
			return true;
		}
		return false;
	}

	protected KubernetesConnectionManager createKubernetesConnectionManager() {
		return new KubernetesConnectionManager(flinkConfig);
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		isStopped = false;
		try {
			kubernetesConnectionManager = createKubernetesConnectionManager();
		} catch (Exception e) {
			throw new ResourceManagerException("Could not start resource manager client.", e);
		}
		try {
			getWorkerNodesFromPreviousAttempts();
		} catch (Exception e) {
			throw new ResourceManagerException("Could not get pods from previous attempts.", e);
		}
		try {
			setupOwnerReference();
		} catch (Exception e) {
			throw new ResourceManagerException("Could not setup owner reference.", e);
		}
		try {
			setupTaskManagerConfigMap();
		} catch (Exception e) {
			throw new ResourceManagerException("Could not upload TaskManager config map.", e);
		}
		try {
			podEventHandler = (action, pod) -> runAsync(() -> handlePodMessage(action, pod));
			watcherCloseHandler = (exception) -> {
				while (true) {
					try {
						watcher = createAndStartWatcher();
						break;
					} catch (Exception e) {
						log.error("Can't create and start watcher, should try it again.", e);
					}
				}
			};
			watcher = createAndStartWatcher();
		} catch (Exception e) {
			throw new ResourceManagerException(
				"Could not create and start watcher.", e);
		}
		try {
			requestWorkerNodes();
		} catch (Exception e) {
			throw new ResourceManagerException(
				"Could not create and start worker node.", e);
		}
	}

	protected void setupOwnerReference() throws ResourceManagerException {
		Service service = kubernetesConnectionManager.getService(clusterId + Constants.SERVICE_NAME_SUFFIX);
		if (service != null) {
			ownerReference = KubernetesRMUtils.createOwnerReference(service);
		} else {
			throw new RuntimeException("Failed to get service " + clusterId + Constants.SERVICE_NAME_SUFFIX);
		}
	}

	protected void setupTaskManagerConfigMap() throws ResourceManagerException {
		tmConfigMap = KubernetesRMUtils.createTaskManagerConfigMap(flinkConfig, confDir,
			ownerReference, taskManagerConfigMapName);
		kubernetesConnectionManager.createOrReplaceConfigMap(tmConfigMap);
	}

	protected Watch createAndStartWatcher() throws ResourceManagerException {
		return kubernetesConnectionManager.createAndStartPodsWatcher(
			taskManagerPodLabels, podEventHandler, watcherCloseHandler);
	}

	protected void handlePodMessage(Watcher.Action action, Pod pod) {
		ResourceID resourceId = new ResourceID(pod.getMetadata().getName());
		log.info("Received {} event for worker node {}, details: {}", action, resourceId, pod.getStatus());
		switch (action) {
		case ADDED:
			if (removePodIfTerminated(pod)) {
				break;
			}
			if (workerNodeMap.containsKey(resourceId)) {
				log.info("Skip adding worker node {} since it's already exist!", resourceId);
			} else {
				try {
					addWorkerNode(pod, true);
				} catch (Exception e) {
					String fatalMsg = "Failed to add work node. Exiting, bye...";
					onFatalError(new ResourceManagerException(fatalMsg));
				}
			}
			break;
		case MODIFIED:
			removePodIfTerminated(pod);
			break;
		case ERROR:
			removePodIfTerminated(pod);
			break;
		case DELETED:
			removeWorkerNode(
				new ResourceID(pod.getMetadata().getName()),
				"Pod is deleted.", false);
			break;
		default:
			log.debug("Skip handling {} event for pod {}", action,
				pod.getMetadata().getName());
			break;
		}
	}

	private boolean removePodIfTerminated(Pod pod) {
		if (pod.getStatus() != null && !pod.getStatus().getContainerStatuses().isEmpty()) {
			List<ContainerStateTerminated> podTerminatedStates =
				pod.getStatus().getContainerStatuses().stream()
					.filter(e -> e.getState() != null && e.getState().getTerminated() != null)
					.map(e -> e.getState().getTerminated()).collect(
					Collectors.toList());
			if (!podTerminatedStates.isEmpty()) {
				//increase failed attempts if terminated exceptionally
				removeWorkerNode(new ResourceID(pod.getMetadata().getName()),
					"Pod terminated : " + podTerminatedStates, true);
				return true;
			}
		}
		return false;
	}

	private void increaseWorkerNodeFailedAttempts() {
		workerNodeFailedAttempts.incrementAndGet();
		log.info("Worker node failed attempts: {}, max failed attempts: {}",
			workerNodeFailedAttempts.get(),
			workerNodeMaxFailedAttempts);
	}

	private void checkWorkerNodeFailedAttempts() {
		if (workerNodeFailedAttempts.get()
			>= workerNodeMaxFailedAttempts) {
			isStopped = true;
			String fatalMsg = "Worker node failures have reached the maximum failed attempts ("
				+ workerNodeMaxFailedAttempts + "). Exiting, bye...";
			onFatalError(new ResourceManagerException(fatalMsg));
		}
	}

	protected long generateNewPodId() {
		return maxPodId.addAndGet(1);
	}

	protected ResourceID requestNewWorkerNode() throws ResourceManagerException {
		String taskManagerPodName = taskManagerPodNamePrefix + generateNewPodId();
		Container container = KubernetesRMUtils.createTaskManagerContainer(
			flinkConfig, taskManagerResource, confDir, taskManagerPodName, null, null, null);
		log.info("Task manager start command: " + container.getArgs());
		Pod taskManagerPod = KubernetesRMUtils
			.createTaskManagerPod(taskManagerPodLabels, taskManagerPodName,
				taskManagerConfigMapName, ownerReference, container, tmConfigMap);
		kubernetesConnectionManager.createPod(taskManagerPod);
		return new ResourceID(taskManagerPodName);
	}

	@Override
	public CompletableFuture<Void> postStop() {
		// shut down all components
		Throwable firstException = null;

		if (kubernetesConnectionManager != null) {
			try {
				kubernetesConnectionManager.close();
			} catch (Throwable t) {
				firstException = ExceptionUtils
					.firstOrSuppressed(t, firstException);
			}
		}

		if (watcher != null) {
			try {
				watcher.close();
			} catch (Throwable t) {
				firstException = ExceptionUtils.firstOrSuppressed(t, firstException);
			}
		}

		final CompletableFuture<Void> terminationFuture = super.postStop();

		if (firstException != null) {
			return FutureUtils.completedExceptionally(new FlinkException("Error while shutting down YARN resource manager", firstException));
		} else {
			return terminationFuture;
		}
	}

	@Override
	protected synchronized void internalDeregisterApplication(
		ApplicationStatus finalStatus,
		@Nullable String diagnostics) throws ResourceManagerException {
		log.info("Unregister application from the Kubernetes Resource Manager, "
			+ "finalStatus: {}, diagnostics: {}", finalStatus, diagnostics);
		isStopped = true;
		// remove all TM pods
		kubernetesConnectionManager.removePods(taskManagerPodLabels);
	}

	@Override
	public void startNewWorker(ResourceProfile resourceProfile) {
		requestWorkerNodes();
	}

	@Override
	public boolean stopWorker(KubernetesWorkerNode workerNode) {
		if (workerNode != null) {
			return removeWorkerNode(workerNode.getResourceID(), "Stop worker", false);
		}
		return false;
	}

	@Override
	protected KubernetesWorkerNode workerStarted(ResourceID resourceID) {
		return workerNodeMap.get(resourceID);
	}

	@Override
	public void cancelNewWorker(ResourceProfile resourceProfile) {
	}

	@Override
	protected int getNumberAllocatedWorkers() {
		return workerNodeMap.size();
	}

	protected ConcurrentMap<ResourceID, KubernetesWorkerNode> getWorkerNodes() {
		return workerNodeMap;
	}

	protected Set<ResourceID> getPendingWorkerNodes() {
		return pendingWorkerNodes;
	}

	protected synchronized void requestWorkerNodes() {
		if (isStopped) {
			return;
		}
		int requiredWorkerNum =
			workerNum - workerNodeMap.size() - pendingWorkerNodes.size();
		if (requiredWorkerNum < 1) {
			log.info(
				"Allocated and pending containers have reached the limit {}, will not allocate more.",
				workerNum);
			return;
		}

		try {
			for (int i = 0; i < requiredWorkerNum; ++i) {
				ResourceID newResourceId = requestNewWorkerNode();
				pendingWorkerNodes.add(newResourceId);
				log.info("Add pending worker node: {}", newResourceId);
			}
		} catch (Exception e) {
			String fatalMsg = "Failed to request new worker node. Exiting, bye...";
			onFatalError(new ResourceManagerException(fatalMsg, e));
		}

		log.info("Number pending requests {}. Requesting new container with resources {}. ",
			pendingWorkerNodes.size(), taskManagerResource);
	}

	protected synchronized void checkTMRegistered(ResourceID resourceId) {
		KubernetesWorkerNode node = workerNodeMap.get(resourceId);
		if (node != null && !taskExecutorRegistered(resourceId)) {
			//increase failed attempts if terminated exceptionally
			increaseWorkerNodeFailedAttempts();
			log.info("Task manager {} did not register in {}, will stop it and request a new one.", resourceId, taskManagerRegisterTimeout);
			stopWorker(node);
		}
	}

	@VisibleForTesting
	protected void setOwnerReference(OwnerReference ownerReference) {
		this.ownerReference = ownerReference;
	}

	@VisibleForTesting
	protected boolean isStopped() {
		return isStopped;
	}
}
