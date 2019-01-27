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
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
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
import java.util.LinkedHashSet;
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
public class KubernetesResourceManager extends ResourceManager<KubernetesWorkerNode> {

	/**
	 * Kubernetes pod map. Package private for unit test purposes.
	 */
	final ConcurrentMap<ResourceID, KubernetesWorkerNode> workerNodeMap;

	private final Configuration flinkConfig;

	protected ConfigMap tmConfigMap;

	/**
	 * Connection manager to communicate with Kubernetes.
	 */
	private KubernetesConnectionManager kubernetesConnectionManager;

	private final Time taskManagerRegisterTimeout;

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

	private final FatalErrorHandler fatalErrorHandler;

	private OwnerReference ownerReference;

	private BiConsumer<Watcher.Action, Pod> podEventHandler;

	private Consumer<Exception> watcherCloseHandler;

	/** The min cpu core of a task executor. */
	private final double minCorePerContainer;

	/** The min memory of task executor to allocate (in MB). */
	private final int minMemoryPerContainer;

	/** The max cpu core of a task executor, used to decide how many slots can be placed on a task executor. */
	private final double maxCorePerContainer;

	/** The max memory of a task executor, used to decide how many slots can be placed on a task executor. */
	private final int maxMemoryPerContainer;

	/** The max extended resource of a task executor, used to decide how many slots can be placed on a task executor. */
	private final Map<String, Double> maxExtendedResourcePerContainer;

	/** The pending pod requests for each priority, but not yet granted.
	 *  Currently we use priority to identity a typical type of resource.
	 **/
	private final ConcurrentHashMap<Integer, Set<ResourceID>> pendingWorkerNodes;

	private final Map<TaskManagerResource, Integer> resourceToPriorityMap;

	private final Map<Integer, TaskManagerResource> priorityToResourceMap;

	/**
	 * The number of slots not used by any request.
	 */
	private final Map<Integer, Integer> priorityToSpareSlots;

	private volatile int latestPriority = 0;

	public KubernetesResourceManager(
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
		this.fatalErrorHandler = fatalErrorHandler;
		this.workerNodeMap = new ConcurrentHashMap<>();
		this.confDir = flinkConfig.getString(KubernetesConfigOptions.CONF_DIR);

		taskManagerRegisterTimeout = Time.seconds(flinkConfig
			.getLong(KubernetesConfigOptions.TASK_MANAGER_REGISTER_TIMEOUT));

		workerNodeMaxFailedAttempts = flinkConfig.getInteger(
			KubernetesConfigOptions.WORKER_NODE_MAX_FAILED_ATTEMPTS);

		clusterId = flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID);
		taskManagerPodLabels = new HashMap<>();
		taskManagerPodLabels.put(Constants.LABEL_APP_KEY, clusterId);
		taskManagerPodLabels.put(Constants.LABEL_COMPONENT_KEY, Constants.LABEL_COMPONENT_TASK_MANAGER);
		taskManagerPodNamePrefix =
			clusterId + Constants.TASK_MANAGER_LABEL_SUFFIX + Constants.NAME_SEPARATOR;
		taskManagerConfigMapName =
			clusterId + Constants.TASK_MANAGER_CONFIG_MAP_SUFFIX;

		minCorePerContainer = flinkConfig.getDouble(TaskManagerOptions.TASK_MANAGER_MULTI_SLOTS_MIN_CORE);
		minMemoryPerContainer = flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_MULTI_SLOTS_MIN_MEMORY);
		maxCorePerContainer = flinkConfig.getDouble(TaskManagerOptions.TASK_MANAGER_MULTI_SLOTS_MAX_CORE);
		maxMemoryPerContainer = flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_MULTI_SLOTS_MAX_MEMORY);
		maxExtendedResourcePerContainer = KubernetesRMUtils.loadExtendedResourceConstrains(flinkConfig);

		pendingWorkerNodes = new ConcurrentHashMap<>();
		priorityToSpareSlots = new HashMap<>();
		priorityToResourceMap = new HashMap<>();
		resourceToPriorityMap = new HashMap<>();
		log.info("Initialize KubernetesResourceManager: clusterId: {}", clusterId);
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
				latestPriority = workerNodeMap.values().stream()
					.mapToInt(KubernetesWorkerNode::getPriority).max().getAsInt() + 1;
			}
			log.info(
				"Recovered {} pods from previous attempts, max pod id is {}.",
				workerNodeMap.size(), maxPodId.get());
		}
	}

	protected synchronized KubernetesWorkerNode addWorkerNode(Pod pod, boolean checkPending)
		throws ResourceManagerException {
		String podName = pod.getMetadata().getName();
		ResourceID resourceId = new ResourceID(podName);
		if (workerNodeMap.containsKey(resourceId)) {
			log.warn("Skip adding worker node {} since it's already exist!", resourceId);
			return workerNodeMap.get(resourceId);
		}
		if (podName.startsWith(taskManagerPodNamePrefix)) {
			String podId = podName.substring(podName.lastIndexOf(Constants.NAME_SEPARATOR) + 1);
			Map<String, String> labels = pod.getMetadata().getLabels();
			if (StringUtils.isNumeric(podId) && labels != null
				&& labels.containsKey(Constants.LABEL_PRIORITY_KEY)
				&& StringUtils.isNumeric(labels.get(Constants.LABEL_PRIORITY_KEY))) {
				int priority = Integer.parseInt(labels.get(Constants.LABEL_PRIORITY_KEY));
				Set<ResourceID> curPendingWorkerNodes = pendingWorkerNodes.get(priority);
				if (checkPending) {
					if (curPendingWorkerNodes == null) {
						log.error("Skip invalid pod {} whose priority {} is not pending.", podName, priority);
						kubernetesConnectionManager.removePod(pod);
						return null;
					}
					boolean pendingRemoved = curPendingWorkerNodes.remove(resourceId);
					if (!pendingRemoved) {
						log.warn("Skip adding worker node {} since it's no longer pending!", resourceId);
						kubernetesConnectionManager.removePod(pod);
						return null;
					}
				} else if (curPendingWorkerNodes != null) {
					// remove from pending worker nodes if exist
					curPendingWorkerNodes.remove(resourceId);
				}
				KubernetesWorkerNode workerNode =
					new KubernetesWorkerNode(pod, podName,
						Long.parseLong(podId), priority);
				workerNodeMap.put(workerNode.getResourceID(), workerNode);
				scheduleRunAsync(() -> checkTMRegistered(resourceId), taskManagerRegisterTimeout);
				log.info("Add worker node : {}, worker nodes: {}, pending worker nodes: {} - {}",
					workerNode.getResourceID(), workerNodeMap.size(),
					curPendingWorkerNodes == null ? 0 : curPendingWorkerNodes.size(), curPendingWorkerNodes);
				return workerNode;
			} else {
				log.error("Skip invalid pod whose podId ({}) or priority in labels({}) is not a number.", podId, labels);
				kubernetesConnectionManager.removePod(pod);
			}
		} else {
			log.error("Skip invalid pod whose name is {} and prefix is not {}.",
				podName, taskManagerPodNamePrefix);
			kubernetesConnectionManager.removePod(pod);
		}
		return null;
	}

	protected synchronized boolean removeWorkerNode(ResourceID resourceID, String diagnostics, boolean increaseFailedAttempt) {
		if (!workerNodeMap.containsKey(resourceID)) {
			return false;
		}
		if (increaseFailedAttempt) {
			increaseWorkerNodeFailedAttempts();
		}
		log.info("Try to remove worker node: {}, diagnostics: {}", resourceID, diagnostics);
		KubernetesWorkerNode node = workerNodeMap.remove(resourceID);
		// Remove pod and check worker node failed attempts
		if (node != null) {
			try {
				kubernetesConnectionManager.removePod(node.getPod());
				checkWorkerNodeFailedAttempts();
				// We only request new container for it when the container has not register to the RM as otherwise
				// the job master will ask for it when failover.
				boolean registered = closeTaskManagerConnection(resourceID, new Exception(diagnostics));
				if (!registered) {
					if (priorityToResourceMap.containsKey(node.getPriority())) {
						// Container completed unexpectedly ~> start a new one
						internalRequestContainer(node.getPriority());
					} else {
						log.info("Not found resource for priority {}, this is usually due to job master failover.",
							node.getPriority());
					}
				}
				log.info("Removed worker node: {}, left worker nodes: {}", resourceID, workerNodeMap.size());
				return true;
			} catch (Exception e) {
				String fatalMsg = "Failed to remove work node. Exiting, bye...";
				onFatalError(new ResourceManagerException(fatalMsg, e));
			}
		}
		return false;
	}

	/**
	 * Request new container if pending containers cannot satisfies pending slot requests.
	 */
	private void internalRequestContainer(int priority) throws ResourceManagerException {
		Set<ResourceID> curPendingWorkerNodes = pendingWorkerNodes.get(priority);
		TaskManagerResource tmResource = priorityToResourceMap.get(priority);
		if (curPendingWorkerNodes == null || tmResource == null) {
			log.error("There is no previous allocation with id {} for {}.", priority, tmResource);
		} else {
			// TODO: Just a weak check because we don't know how many pending slot requests belongs to
			// this priority. So currently we use overall pending slot requests number to restrain
			// the container requests of this priority.
			int pendingSlotRequests = getNumberPendingSlotRequests();
			int pendingSlotAllocation = curPendingWorkerNodes.size() * tmResource.getSlotNum();
			if (pendingSlotRequests > pendingSlotAllocation) {
				requestNewWorkerNode(tmResource, priority);
			} else {
				log.info("Skip request yarn container, there are enough pending slot allocation for slot requests." +
						" Priority {}. Resource {}. Pending slot allocation {}. Pending slot requests {}.",
					priority,
					tmResource,
					pendingSlotAllocation,
					pendingSlotRequests);
			}
		}
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
	}

	protected void setupOwnerReference() throws ResourceManagerException {
		Service service = kubernetesConnectionManager.getService(clusterId + Constants.SERVICE_NAME_SUFFIX);
		if (service != null) {
			ownerReference = KubernetesRMUtils.createOwnerReference(service);
		} else {
			throw new ResourceManagerException("Failed to get service " + clusterId + Constants.SERVICE_NAME_SUFFIX);
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
					String fatalMsg = "Failed to add worker node. Exiting, bye...";
					onFatalError(new ResourceManagerException(fatalMsg, e));
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
				return removeWorkerNode(new ResourceID(pod.getMetadata().getName()),
					"Pod terminated : " + podTerminatedStates, true);
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
			String fatalMsg = "Worker node failed attempts (" + workerNodeFailedAttempts.get()
				+ ") beyond the max failed attempts ("
				+ workerNodeMaxFailedAttempts + "). Exiting, bye...";
			onFatalError(new ResourceManagerException(fatalMsg));
		}
	}

	protected long generateNewPodId() {
		return maxPodId.addAndGet(1);
	}

	protected synchronized void requestNewWorkerNode(TaskManagerResource taskManagerResource, int priority)
		throws ResourceManagerException {
		if (isStopped) {
			return;
		}
		String taskManagerPodName = taskManagerPodNamePrefix + generateNewPodId();
		try {
			// init additional environments
			Map<String, String> additionalEnvs = new HashMap<>();
			additionalEnvs.put(Constants.ENV_TM_NUM_TASK_SLOT, String.valueOf(taskManagerResource.getSlotNum()));
			additionalEnvs.put(Constants.ENV_TM_RESOURCE_PROFILE_KEY,
				KubernetesRMUtils.getEncodedResourceProfile(taskManagerResource.getTaskResourceProfile()));

			final long managedMemory = taskManagerResource.getManagedMemorySize() > 1 ? taskManagerResource.getManagedMemorySize() :
				flinkConfig.getLong(TaskManagerOptions.MANAGED_MEMORY_SIZE);
			additionalEnvs.put(Constants.ENV_TM_MANAGED_MEMORY_SIZE, String.valueOf(managedMemory));

			final int floatingManagedMemory = taskManagerResource.getFloatingManagedMemorySize();
			additionalEnvs.put(Constants.ENV_TM_FLOATING_MANAGED_MEMORY_SIZE, String.valueOf(floatingManagedMemory));

			additionalEnvs.put(Constants.ENV_TM_PROCESS_NETTY_MEMORY,
				String.valueOf(taskManagerResource.getTaskManagerNettyMemorySizeMB()));

			long networkBufBytes = ((long) taskManagerResource.getNetworkMemorySize()) << 20;
			additionalEnvs.put(Constants.ENV_TM_NETWORK_BUFFERS_MEMORY_MIN, String.valueOf(networkBufBytes));
			additionalEnvs.put(Constants.ENV_TM_NETWORK_BUFFERS_MEMORY_MAX, String.valueOf(networkBufBytes));
			log.debug("Task manager environments: " + additionalEnvs);
			// create TM container
			Container container = KubernetesRMUtils.createTaskManagerContainer(
				flinkConfig, taskManagerResource, confDir, taskManagerPodName,
				minCorePerContainer, minMemoryPerContainer, additionalEnvs);
			log.debug("Task manager start command: " + container.getArgs());
			// create TM pod
			Map<String, String> currentLabels = new HashMap<>(taskManagerPodLabels);
			currentLabels.put(Constants.LABEL_PRIORITY_KEY, String.valueOf(priority));
			Pod taskManagerPod = KubernetesRMUtils
				.createTaskManagerPod(currentLabels, taskManagerPodName,
					taskManagerConfigMapName, ownerReference, container, tmConfigMap);
			kubernetesConnectionManager.createPod(taskManagerPod);
			// update pending worker nodes
			Set<ResourceID> curPendingWorkerNodes = pendingWorkerNodes.get(priority);
			if (curPendingWorkerNodes == null) {
				curPendingWorkerNodes = new LinkedHashSet<>();
				pendingWorkerNodes.put(priority, curPendingWorkerNodes);
			}
			curPendingWorkerNodes.add(new ResourceID(taskManagerPodName));
			log.info("Requesting new TaskExecutor container with priority {}. Number pending requests {}. TM Pod name {}. TM Resources {}.",
				priority, curPendingWorkerNodes.size(), taskManagerPodName, taskManagerResource);
		} catch (Exception e) {
			log.error("Failed to request new worker node with priority {}. TM Pod name {}. TM Resources {}.",
				priority, taskManagerPodName, taskManagerResource, e);
			throw new ResourceManagerException(e);
		}
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
	public synchronized void startNewWorker(ResourceProfile resourceProfile) {
		// Priority for worker containers - priorities are intra-application
		int slotNumber = calculateSlotNumber(resourceProfile);
		TaskManagerResource tmResource = TaskManagerResource.fromConfiguration(flinkConfig, resourceProfile, slotNumber);
		int priority = generatePriority(tmResource);

		int spareSlots = priorityToSpareSlots.getOrDefault(priority, 0);
		if (spareSlots > 0) {
			priorityToSpareSlots.put(priority, spareSlots - 1);
		} else {
			if (slotNumber > 1) {
				priorityToSpareSlots.put(priority, slotNumber - 1);
			}
			try {
				requestNewWorkerNode(tmResource, priority);
			} catch (Exception e) {
				String fatalMsg = "Failed to request new worker node.";
				onFatalError(new ResourceManagerException(fatalMsg, e));
			}
		}
	}

	@Override
	public synchronized boolean stopWorker(KubernetesWorkerNode workerNode) {
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
	public synchronized void cancelNewWorker(ResourceProfile resourceProfile) {
		int slotNumber = calculateSlotNumber(resourceProfile);
		TaskManagerResource tmResource = TaskManagerResource.fromConfiguration(flinkConfig, resourceProfile, slotNumber);
		int priority = generatePriority(tmResource);
		Set<ResourceID> curPendingWorkerNodes = pendingWorkerNodes.get(priority);
		log.info("Canceling new worker with priority {}, pending worker nodes: {}.", priority, curPendingWorkerNodes.size());
		if (curPendingWorkerNodes == null) {
			log.error("There is no previous allocation with id {} for {}.", priority, resourceProfile);
		} else if (curPendingWorkerNodes.size() > 0) {
			try {
				// update the pending request number
				if (slotNumber == 1) {
					// if one container has one slot, just decrease the pending number
					ResourceID resourceID = curPendingWorkerNodes.iterator().next();
					curPendingWorkerNodes.remove(resourceID);
					kubernetesConnectionManager.removePod(resourceID.toString());
				} else {
					Integer spareSlots = priorityToSpareSlots.get(priority);
					// if spare slots not fulfill a container, add one to the spare number, else decrease the pending number
					if (spareSlots == null) {
						priorityToSpareSlots.put(priority, 1);
					} else if (spareSlots < slotNumber - 1) {
						priorityToSpareSlots.put(priority, spareSlots + 1);
					} else {
						priorityToSpareSlots.remove(priority);
						ResourceID resourceID = curPendingWorkerNodes.iterator().next();
						curPendingWorkerNodes.remove(resourceID);
						kubernetesConnectionManager.removePod(resourceID.toString());
					}
				}
			} catch (Exception e) {
				String fatalMsg = "Failed to cancel new work node. Exiting, bye...";
				onFatalError(new ResourceManagerException(fatalMsg, e));
			}
		}
		log.info("Canceled new worker with priority {}, pending worker nodes: {}, priority to spare slots: {}.",
			priority, curPendingWorkerNodes.size(), priorityToSpareSlots);
	}

	@Override
	protected int getNumberAllocatedWorkers() {
		return workerNodeMap.size();
	}

	protected ConcurrentMap<ResourceID, KubernetesWorkerNode> getWorkerNodes() {
		return workerNodeMap;
	}

	protected Map<Integer, Set<ResourceID>> getPendingWorkerNodes() {
		return pendingWorkerNodes;
	}

	protected synchronized void checkTMRegistered(ResourceID resourceId) {
		KubernetesWorkerNode node = workerNodeMap.get(resourceId);
		if (node != null && !taskExecutorRegistered(resourceId)) {
			increaseWorkerNodeFailedAttempts();
			log.info("Task manager {} did not register in {}, will stop it and request a new one.", resourceId, taskManagerRegisterTimeout);
			stopWorker(node);
		}
	}

	@VisibleForTesting
	protected void setOwnerReference(OwnerReference ownerReference) {
		this.ownerReference = ownerReference;
	}

	/**
	 * Calculate the slot number in a task executor according to the resource.
	 *
	 * @param resourceProfile The resource profile of a request
	 * @return The slot number in a task executor.
	 */
	@VisibleForTesting
	int calculateSlotNumber(ResourceProfile resourceProfile) {
		if (resourceProfile.getCpuCores() <= 0 || resourceProfile.getMemoryInMB() <= 0) {
			return 1;
		}
		else {
			if (resourceProfile.getCpuCores() > maxCorePerContainer) {
				return 1;
			}
			if (resourceProfile.getMemoryInMB() > maxMemoryPerContainer) {
				return 1;
			}
			int slot = Math.min((int) (maxCorePerContainer / resourceProfile.getCpuCores()),
				(maxMemoryPerContainer / resourceProfile.getMemoryInMB()));

			for (org.apache.flink.api.common.resources.Resource extendedResource : resourceProfile.getExtendedResources().values()) {
				// Skip floating memory, it has been added to memory
				if (extendedResource.getName().equals(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME)) {
					continue;
				}
				Double maxPerContainer = maxExtendedResourcePerContainer.get(extendedResource.getName().toLowerCase());
				if (maxPerContainer != null) {
					if (extendedResource.getValue() > maxPerContainer) {
						return 1;
					}
					slot = Math.min(slot, (int) (maxPerContainer / extendedResource.getValue()));
				}
			}
			return slot;
		}
	}

	/**
	 * Generate priority by given resource profile.
	 * Priority is only used for distinguishing request of different resource.
	 * @param tmResource The resource profile of a request
	 * @return The priority of this resource profile.
	 */
	private int generatePriority(TaskManagerResource tmResource) {
		Integer priority = resourceToPriorityMap.get(tmResource);
		if (priority != null) {
			return priority;
		} else {
			priority = latestPriority++;
			resourceToPriorityMap.put(tmResource, priority);
			priorityToResourceMap.put(priority, tmResource);
			return priority;
		}
	}

	@VisibleForTesting
	public boolean isStopped() {
		return isStopped;
	}
}
