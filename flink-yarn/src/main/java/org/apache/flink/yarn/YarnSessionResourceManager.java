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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.resources.CommonExtendedResource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.standalone.TaskManagerResourceCalculator;
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
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The yarn implementation of the resource manager. Used when the system is started
 * via the resource framework YARN.
 */
public class YarnSessionResourceManager extends ResourceManager<YarnWorkerNode> {

	/**
	 * The process environment variables.
	 */
	private final Map<String, String> env;

	/**
	 * YARN container map. Package private for unit test purposes.
	 */
	final ConcurrentMap<ResourceID, YarnWorkerNode> workerNodeMap;

	/**
	 * The heartbeat interval while the resource master is waiting for containers.
	 */
	private static final int FAST_YARN_HEARTBEAT_INTERVAL_MS = 500;

	/**
	 * Yarn vcore ratio, how many virtual cores will use a physical core.
	 * */
	private final double yarnVcoreRatio;

	/**
	 * Environment variable name of the final container id used by the YarnResourceManager.
	 * Container ID generation may vary across Hadoop versions.
	 */
	static final String ENV_FLINK_CONTAINER_ID = "_FLINK_CONTAINER_ID";

	/**
	 * Environment variable name of the hostname given by the YARN.
	 * In task executor we use the hostnames given by YARN consistently throughout akka
	 */
	static final String ENV_FLINK_NODE_ID = "_FLINK_NODE_ID";

	/**
	 * Default heartbeat interval between this resource manager and the YARN ResourceManager.
	 */
	private final int yarnHeartbeatIntervalMillis;

	private final Configuration flinkConfig;

	private final TaskManagerConfiguration taskManagerConfiguration;

	private final YarnConfiguration yarnConfig;

	@Nullable
	private final String webInterfaceUrl;

	private YarnAMRMClientCallback yarnAMRMClientCallback;

	/**
	 * Client to communicate with the Resource Manager (YARN's master).
	 */
	private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient;

	/**
	 * Client to communicate with the Node manager and launch TaskExecutor processes.
	 */
	private NMClient nodeManagerClient;

	/** The number of containers requested. **/
	private final int workerNum;

	/** The resource for each Yarn container. **/
	private final Resource workerResource;

	private final int slotNumber;

	/** The number of containers requested, but not yet granted. */
	private final AtomicInteger numPendingContainerRequests;

	/** The priority of all containers. **/
	private final Priority resourcePriority;

	private final Time containerRegisterTimeout;

	private final TaskManagerResource taskManagerResource;

	/**
	 * executor for start yarn container.
	 */
	private Executor executor;

	public YarnSessionResourceManager(
			RpcService rpcService,
			String resourceManagerEndpointId,
			ResourceID resourceId,
			Configuration flinkConfig,
			Map<String, String> env,
			ResourceManagerConfiguration resourceManagerConfiguration,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			MetricRegistry metricRegistry,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			@Nullable String webInterfaceUrl) {
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
		this.yarnConfig = new YarnConfiguration();
		this.taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(flinkConfig);
		this.env = env;
		this.workerNodeMap = new ConcurrentHashMap<>();
		this.slotNumber = Integer.parseInt(env.getOrDefault(YarnConfigKeys.ENV_SLOTS, "10"));

		final int yarnHeartbeatIntervalMS = flinkConfig.getInteger(
				YarnConfigOptions.HEARTBEAT_DELAY_SECONDS) * 1000;

		final long yarnExpiryIntervalMS = yarnConfig.getLong(
				YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
				YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS);

		if (yarnHeartbeatIntervalMS >= yarnExpiryIntervalMS) {
			log.warn("The heartbeat interval of the Flink Application master ({}) is greater " +
							"than YARN's expiry interval ({}). The application is likely to be killed by YARN.",
					yarnHeartbeatIntervalMS, yarnExpiryIntervalMS);
		}
		yarnHeartbeatIntervalMillis = yarnHeartbeatIntervalMS;

		numPendingContainerRequests = new AtomicInteger(0);

		resourcePriority = Priority.newInstance(0);

		containerRegisterTimeout = Time.seconds(flinkConfig.getLong(YarnConfigOptions.CONTAINER_REGISTER_TIMEOUT));

		this.executor = Executors.newScheduledThreadPool(
				flinkConfig.getInteger(YarnConfigOptions.CONTAINER_LAUNCHER_NUMBER));

		this.yarnVcoreRatio = flinkConfig.getInteger(YarnConfigOptions.YARN_VCORE_RATIO);

		this.webInterfaceUrl = webInterfaceUrl;

		// build the task manager's total resource according to user's resource
		Preconditions.checkNotNull(env.get(YarnConfigKeys.ENV_TM_MEMORY));
		int taskManagerTotalMemorySizeMB = Integer.parseInt(env.get(YarnConfigKeys.ENV_TM_MEMORY));
		// slotNum is 1, the resource profile is total resource shared by all slots in session mode.
		taskManagerResource = TaskManagerResource.fromConfiguration(
			flinkConfig, initContainerResourceConfig(), 1, taskManagerTotalMemorySizeMB);
		log.info(taskManagerResource.toString());

		if (slotManager instanceof DynamicAssigningSlotManager) {
			((DynamicAssigningSlotManager) slotManager).setTotalResourceOfTaskExecutor(
				taskManagerResource.getTaskResourceProfile());
			log.info("The resource for user in a task executor is {}.", taskManagerResource);
		}

		workerNum = Integer.parseInt(env.getOrDefault(YarnConfigKeys.ENV_TM_COUNT, "1"));
		int containerMemory = taskManagerResource.getTotalContainerMemory();
		int containerVcore = (int) (taskManagerResource.getContainerCpuCores() *
				flinkConfig.getInteger(YarnConfigOptions.YARN_VCORE_RATIO));
		// TODO: Set extended resources if the version of yarn api >= 2.8 .
		workerResource = Resource.newInstance(containerMemory, containerVcore);
		log.info("workerNum: {}, workerResource: {}", workerNum, workerResource);
	}

	protected AMRMClientAsync<AMRMClient.ContainerRequest> createAndStartResourceManagerClient(
			YarnConfiguration yarnConfiguration,
			int yarnHeartbeatIntervalMillis,
			@Nullable String webInterfaceUrl) throws Exception {
		yarnAMRMClientCallback = new YarnAMRMClientCallback();
		AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient =
				AMRMClientAsync.createAMRMClientAsync(yarnHeartbeatIntervalMillis, yarnAMRMClientCallback);
		resourceManagerClient.init(yarnConfiguration);
		resourceManagerClient.start();

		//TODO: change akka address to tcp host and port, the getAddress() interface should return a standard tcp address
		Tuple2<String, Integer> hostPort = parseHostPort(getAddress());

		final int restPort;

		if (webInterfaceUrl != null) {
			final int lastColon = webInterfaceUrl.lastIndexOf(':');

			if (lastColon == -1) {
				restPort = -1;
			} else {
				restPort = Integer.valueOf(webInterfaceUrl.substring(lastColon + 1));
			}
		} else {
			restPort = -1;
		}

		RegisterApplicationMasterResponse response =
				resourceManagerClient.registerApplicationMaster(hostPort.f0, restPort, webInterfaceUrl);

		getContainersFromPreviousAttempts(response);

		return resourceManagerClient;
	}

	@VisibleForTesting
	public void getContainersFromPreviousAttempts(final RegisterApplicationMasterResponse registerApplicationMasterResponse) {
		final List<Container> containersFromPreviousAttempts =
				new RegisterApplicationMasterResponseReflector(log).getContainersFromPreviousAttempts(registerApplicationMasterResponse);

		log.info("Recovered {} containers from previous attempts ({}).", containersFromPreviousAttempts.size(), containersFromPreviousAttempts);

		for (final Container container : containersFromPreviousAttempts) {
			workerNodeMap.put(new ResourceID(container.getId().toString()), new YarnWorkerNode(container));
			scheduleRunAsync(() -> checkAndRegisterContainer(
					new ResourceID(container.getId().toString())), containerRegisterTimeout);
		}
	}

	private NMClient createAndStartNodeManagerClient(YarnConfiguration yarnConfiguration) {
		// create the client to communicate with the node managers
		NMClient nodeManagerClient = NMClient.createNMClient();
		nodeManagerClient.init(yarnConfiguration);
		nodeManagerClient.start();
		nodeManagerClient.cleanupRunningContainersOnStop(false);
		return nodeManagerClient;
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		try {
			resourceManagerClient = createAndStartResourceManagerClient(
					yarnConfig,
					yarnHeartbeatIntervalMillis,
					webInterfaceUrl);
		} catch (Exception e) {
			throw new ResourceManagerException("Could not start resource manager client.", e);
		}
		nodeManagerClient = createAndStartNodeManagerClient(yarnConfig);

		try {
			String curDir = env.get(ApplicationConstants.Environment.PWD.key());
			Utils.uploadTaskManagerConf(flinkConfig, yarnConfig, env, curDir);
		} catch (Exception e) {
			throw new ResourceManagerException("Could not upload TaskManager config file.", e);
		}
		startClusterWorkers();
	}

	@Override
	public CompletableFuture<Void> postStop() {
		// shut down all components
		Throwable firstException = null;

		if (resourceManagerClient != null) {
			try {
				resourceManagerClient.stop();
			} catch (Throwable t) {
				firstException = ExceptionUtils.firstOrSuppressed(t, firstException);
			}
		}

		if (nodeManagerClient != null) {
			try {
				nodeManagerClient.stop();
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
	protected void internalDeregisterApplication(
		ApplicationStatus finalStatus,
		@Nullable String diagnostics) {

		// first, de-register from YARN
		FinalApplicationStatus yarnStatus = getYarnStatus(finalStatus);
		log.info("Unregister application from the YARN Resource Manager");
		try {
			resourceManagerClient.unregisterApplicationMaster(yarnStatus, diagnostics, "");
		} catch (Throwable t) {
			log.error("Could not unregister the application master.", t);
		}
	}

	@Override
	public void startNewWorker(ResourceProfile resourceProfile) {
		requestYarnContainers(workerResource, 1);
	}

	@Override
	public boolean stopWorker(YarnWorkerNode workerNode) {
		if (workerNode != null) {
			Container container = workerNode.getContainer();
			log.info("Release container {}.", container.getId());
			resourceManagerClient.releaseAssignedContainer(container.getId());
			YarnWorkerNode node = workerNodeMap.remove(workerNode.getResourceID());
			if (node != null) {
				requestYarnContainers(workerResource, 1);
			}
		}
		return true;
	}

	@Override
	protected YarnWorkerNode workerStarted(ResourceID resourceID) {
		return workerNodeMap.get(resourceID);
	}

	@Override
	public void cancelNewWorker(ResourceProfile resourceProfile) {
	}

	@Override
	protected int getNumberAllocatedWorkers() {
		return workerNodeMap.size();
	}

	// Utility methods

	/**
	 * Start a number of workers according to the configuration.
	 */
	@VisibleForTesting
	void startClusterWorkers() {
		int requiredWorkerNum = Math.max(workerNum - workerNodeMap.size(), 0);
		requestYarnContainers(workerResource, requiredWorkerNum);
	}

	private ResourceProfile initContainerResourceConfig() {

		double core = flinkConfig.getDouble(TaskManagerOptions.TASK_MANAGER_CORE, slotNumber);
		int heapMemory = flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY);
		int nativeMemory = flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_NATIVE_MEMORY);
		int directMemory = flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_DIRECT_MEMORY);
		int networkMemory = (int) Math.ceil(TaskManagerResourceCalculator.calculateNetworkBufferMemory(flinkConfig) / (1024.0 * 1024.0));

		// Add managed memory to extended resources.
		long managedMemory = flinkConfig.getLong(TaskManagerOptions.MANAGED_MEMORY_SIZE);
		Map<String, org.apache.flink.api.common.resources.Resource> resourceMap = getExtendedResources();
		resourceMap.put(ResourceSpec.MANAGED_MEMORY_NAME,
				new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, Math.max(0, managedMemory)));
		long floatingManagedMemory = flinkConfig.getLong(TaskManagerOptions.FLOATING_MANAGED_MEMORY_SIZE);
		resourceMap.put(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME,
				new CommonExtendedResource(ResourceSpec.FLOATING_MANAGED_MEMORY_NAME, Math.max(0, floatingManagedMemory)));
		return new ResourceProfile(
				core,
				heapMemory,
				directMemory,
				nativeMemory,
				networkMemory,
				resourceMap);
	}

	/**
	 * Get extended resources from config.
	 * @see org.apache.flink.yarn.configuration.YarnConfigOptions
	 * @return The extended resources.
	 */
	private Map<String, org.apache.flink.api.common.resources.Resource> getExtendedResources() {
		Map<String, org.apache.flink.api.common.resources.Resource> extendedResources = new HashMap<>();
		String resourcesStr = flinkConfig.getString(TaskManagerOptions.TASK_MANAGER_EXTENDED_RESOURCES);
		if (resourcesStr != null && !resourcesStr.isEmpty()) {
			for (String resource : resourcesStr.split(",")) {
				String[] splits = resource.split(":");
				if (splits.length == 2) {
					try {
						extendedResources.put(splits[0],
								new CommonExtendedResource(splits[0], Long.parseLong(splits[1])));
					} catch (NumberFormatException ex) {
						log.error("Parse extended resource {} error.", resource);
					}
				}
			}
		}
		return extendedResources;
	}

	/**
	 * Converts a Flink application status enum to a YARN application status enum.
	 *
	 * @param status The Flink application status.
	 * @return The corresponding YARN application status.
	 */
	private FinalApplicationStatus getYarnStatus(ApplicationStatus status) {
		if (status == null) {
			return FinalApplicationStatus.UNDEFINED;
		} else {
			switch (status) {
				case SUCCEEDED:
					return FinalApplicationStatus.SUCCEEDED;
				case FAILED:
					return FinalApplicationStatus.FAILED;
				case CANCELED:
					return FinalApplicationStatus.KILLED;
				default:
					return FinalApplicationStatus.UNDEFINED;
			}
		}
	}

	/**
	 * parse the host and port from akka address.
	 * the akka address is like akka.tcp://flink@100.81.153.180:49712/user/$a
	 * @param address akka address
	 * @return tuple of host and port
	 */
	private static Tuple2<String, Integer> parseHostPort(String address) {
		String[] hostPort = address.split("@")[1].split(":");
		String host = hostPort[0];
		String port = hostPort[1].split("/")[0];
		return new Tuple2(host, Integer.valueOf(port));
	}

	private synchronized void requestYarnContainers(Resource resource, int numContainers) {
		int requiredWorkerNum = workerNum - workerNodeMap.size() - numPendingContainerRequests.get();
		if (requiredWorkerNum < 1) {
			log.info("Allocated and pending containers have reached the limit {}, will not allocate more.", workerNum);
			return;
		}

		numPendingContainerRequests.addAndGet(numContainers);
		for (int i = 0; i < requiredWorkerNum; ++i) {
			resourceManagerClient.addContainerRequest(new AMRMClient.ContainerRequest(resource, null, null, resourcePriority));
		}

		// make sure we transmit the request fast and receive fast news of granted allocations
		resourceManagerClient.setHeartbeatInterval(FAST_YARN_HEARTBEAT_INTERVAL_MS);

		log.info("Requesting new container with resources {}. Number pending requests {}.",
				resource,
				numPendingContainerRequests);
	}

	private ContainerLaunchContext createTaskExecutorLaunchContext(Container container)
			throws Exception {

		// init the ContainerLaunchContext
		final String currDir = env.get(ApplicationConstants.Environment.PWD.key());

		final ContaineredTaskManagerParameters taskManagerParameters = ContaineredTaskManagerParameters.create(
				flinkConfig,
				container.getResource().getMemory(),
				Math.max(taskManagerResource.getTotalHeapMemory(),
						container.getResource().getMemory() -
						taskManagerResource.getTotalDirectMemory() -
						taskManagerResource.getTotalNativeMemory()),
				taskManagerResource.getTotalDirectMemory(),
				slotNumber,
				taskManagerResource.getYoungHeapMemory(),
				container.getResource().getVirtualCores() / yarnVcoreRatio);

		log.info("TaskExecutor {} will be started with container size {} MB, JVM heap size {} MB, " +
					"new generation size {} MB, JVM direct memory limit {} MB on {}",
				container.getId(),
				taskManagerParameters.taskManagerTotalMemoryMB(),
				taskManagerParameters.taskManagerHeapSizeMB(),
				taskManagerParameters.getYoungMemoryMB(),
				taskManagerParameters.taskManagerDirectMemoryLimitMB(),
				container.getNodeHttpAddress());

		final Configuration taskManagerConfig = BootstrapTools.generateTaskManagerConfiguration(
				flinkConfig, "", 0, slotNumber, null);

		ByteArrayOutputStream output = new ByteArrayOutputStream();
		ObjectOutputStream rpOutput = new ObjectOutputStream(output);
		rpOutput.writeObject(taskManagerResource.getTaskResourceProfile());
		rpOutput.close();
		taskManagerConfig.setString(TaskManagerOptions.TASK_MANAGER_TOTAL_RESOURCE_PROFILE_KEY,
			new String(Base64.encodeBase64(output.toByteArray())));

		// config the managed memory for task manager, fraction will be used if managed memory was not set.
		final int managedMemory = taskManagerResource.getManagedMemorySize();
		if (managedMemory > 1) {
			taskManagerConfig.setInteger(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), managedMemory);
		}

		// config the floating managed memory for task manager
		final int floatingMemory = taskManagerResource.getFloatingManagedMemorySize();
		taskManagerConfig.setInteger(TaskManagerOptions.FLOATING_MANAGED_MEMORY_SIZE.key(), floatingMemory);

		// config the netty framework memory of task manager
		taskManagerConfig.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NETTY_MEMORY.key(),
				taskManagerResource.getTaskManagerNettyMemorySizeMB());

		taskManagerConfig.setFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION,
			flinkConfig.getFloat(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_FRACTION));
		long networkBufBytes = ((long) taskManagerResource.getNetworkMemorySize()) << 20;
		taskManagerConfig.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MIN, networkBufBytes);
		taskManagerConfig.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, networkBufBytes);

		log.debug("TaskManager configuration: {}", taskManagerConfig);

		ContainerLaunchContext taskExecutorLaunchContext = Utils.createTaskExecutorContext(
				flinkConfig, yarnConfig, env,
				taskManagerParameters, taskManagerConfig,
				currDir, YarnTaskExecutorRunner.class, log);

		// set a special environment variable to uniquely identify this container
		taskExecutorLaunchContext.getEnvironment()
				.put(ENV_FLINK_CONTAINER_ID, container.getId().toString());
		taskExecutorLaunchContext.getEnvironment()
				.put(ENV_FLINK_NODE_ID, container.getNodeId().getHost());
		taskExecutorLaunchContext.getEnvironment()
				.put(YarnConfigKeys.ENV_APP_ID, env.get(YarnConfigKeys.ENV_APP_ID));

		return taskExecutorLaunchContext;
	}

	@VisibleForTesting
	boolean checkAndRegisterContainer(ResourceID containerId) {
		YarnWorkerNode node = workerNodeMap.get(containerId);
		if (node != null && !taskExecutorRegistered(containerId)) {
			log.info("Container {} did not register in {}, will stop it and request a new one.", containerId, containerRegisterTimeout);
			stopWorker(node);
			return false;
		}
		return node != null;
	}

	// Methods for test
	@VisibleForTesting
	void setAMRMClient(AMRMClientAsync client) {
		this.resourceManagerClient = client;
	}

	@VisibleForTesting
	void setNMClient(NMClient client) {
		this.nodeManagerClient = client;
	}

	@VisibleForTesting
	AtomicInteger getPendingContainerRequest() {
		return this.numPendingContainerRequests;
	}

	@VisibleForTesting
	YarnSessionResourceManager.YarnAMRMClientCallback getYarnAMRMClientCallback() {
		return yarnAMRMClientCallback;
	}

	@VisibleForTesting
	void setExecutor(Executor executor) {
		this.executor = executor;
	}

	// --------------------------------------------------------------------------------
	// Utility class
	// --------------------------------------------------------------------------------

	class YarnAMRMClientCallback implements AMRMClientAsync.CallbackHandler {

		@Override
		public float getProgress() {
			// Temporarily need not record the total size of asked and allocated containers
			return 1;
		}

		@Override
		public void onContainersCompleted(List<ContainerStatus> list) {
			for (ContainerStatus container : list) {
				log.info("Container {} finished with exit code {}", container.getContainerId(), container.getExitStatus());
				closeTaskManagerConnection(new ResourceID(
						container.getContainerId().toString()), new Exception(container.getDiagnostics()));

				// If a worker terminated exceptionally, start a new one;
				YarnWorkerNode node = workerNodeMap.remove(new ResourceID(container.getContainerId().toString()));
				if (node != null) {
					requestYarnContainers(workerResource, 1);
				}
			}
		}

		@Override
		public void onContainersAllocated(List<Container> containers) {
			for (Container container : containers) {
				if (numPendingContainerRequests.get() <= 0) {
					log.info("Received more than asked containers, will release the {} with resource {}",
							container.getId(), container.getResource());
					resourceManagerClient.releaseAssignedContainer(container.getId());
					continue;
				}

				log.info("Received new container: {} - Remaining pending container requests: {}",
						container.getId(), numPendingContainerRequests.get() - 1);
				numPendingContainerRequests.decrementAndGet();
				resourceManagerClient.removeContainerRequest(
						new AMRMClient.ContainerRequest(workerResource, null, null, resourcePriority));

				final String containerIdStr = container.getId().toString();
				workerNodeMap.put(new ResourceID(containerIdStr),
						new YarnWorkerNode(container));
				scheduleRunAsync(() -> checkAndRegisterContainer(
						new ResourceID(container.getId().toString())), containerRegisterTimeout);

				executor.execute(() -> {
					try {
						/* Context information used to start a TaskExecutor Java process */
						ContainerLaunchContext taskExecutorLaunchContext =
								createTaskExecutorLaunchContext(container);
						nodeManagerClient.startContainer(container, taskExecutorLaunchContext);
					} catch (Throwable t) {
						// failed to launch the container, will release the failed one and ask for a new one
						log.error("Could not start TaskManager in container {},", container, t);
						resourceManagerClient.releaseAssignedContainer(container.getId());
						YarnWorkerNode node = workerNodeMap.remove(new ResourceID(container.getId().toString()));
						if (node != null) {
							requestYarnContainers(workerResource, 1);
						}
					}
				});
			}

			if (numPendingContainerRequests.get() == 0) {
				resourceManagerClient.setHeartbeatInterval(yarnHeartbeatIntervalMillis);
			}
		}

		@Override
		public void onShutdownRequest() {
			try {
				shutDown();
			} catch (Exception e) {
				log.warn("Fail to shutdown the YARN resource manager.", e);
			}
		}

		@Override
		public void onNodesUpdated(List<NodeReport> list) {
			// We are not interested in node updates
		}

		@Override
		public void onError(Throwable error) {
			onFatalError(error);
		}
	}
}
