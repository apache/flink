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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
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
import org.apache.flink.yarn.configuration.YarnConfigOptions;

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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The yarn implementation of the resource manager. Used when the system is started
 * via the resource framework YARN.
 */
public class YarnResourceManager extends ResourceManager<YarnWorkerNode> implements AMRMClientAsync.CallbackHandler {

	/** The process environment variables. */
	private final Map<String, String> env;

	/** YARN container map. Package private for unit test purposes. */
	final ConcurrentMap<ResourceID, YarnWorkerNode> workerNodeMap;

	/** The heartbeat interval while the resource master is waiting for containers. */
	private static final int FAST_YARN_HEARTBEAT_INTERVAL_MS = 500;

	/** Environment variable name of the final container id used by the YarnResourceManager.
	 * Container ID generation may vary across Hadoop versions. */
	static final String ENV_FLINK_CONTAINER_ID = "_FLINK_CONTAINER_ID";

	/** Environment variable name of the hostname given by the YARN.
	 * In task executor we use the hostnames given by YARN consistently throughout akka */
	static final String ENV_FLINK_NODE_ID = "_FLINK_NODE_ID";

	/** Default heartbeat interval between this resource manager and the YARN ResourceManager. */
	private final int yarnHeartbeatIntervalMillis;

	private final Configuration flinkConfig;

	private final YarnConfiguration yarnConfig;

	@Nullable
	private final String webInterfaceUrl;

	private final int defaultTaskManagerMemoryMB;

	private final int defaultNumSlots;

	private final int defaultCpus;

	/** Client to communicate with the Resource Manager (YARN's master). */
	private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient;

	/** Client to communicate with the Node manager and launch TaskExecutor processes. */
	private NMClient nodeManagerClient;

	/** The number of containers requested, but not yet granted. */
	private int numPendingContainerRequests;

	private final Map<ResourceProfile, Integer> resourcePriorities = new HashMap<>();

	public YarnResourceManager(
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
		this.flinkConfig  = flinkConfig;
		this.yarnConfig = new YarnConfiguration();
		this.env = env;
		this.workerNodeMap = new ConcurrentHashMap<>();
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
		numPendingContainerRequests = 0;

		this.webInterfaceUrl = webInterfaceUrl;
		this.defaultTaskManagerMemoryMB = flinkConfig.getInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY);
		this.defaultNumSlots = flinkConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);
		this.defaultCpus = flinkConfig.getInteger(YarnConfigOptions.VCORES, defaultNumSlots);
	}

	protected AMRMClientAsync<AMRMClient.ContainerRequest> createAndStartResourceManagerClient(
			YarnConfiguration yarnConfiguration,
			int yarnHeartbeatIntervalMillis,
			@Nullable String webInterfaceUrl) throws Exception {
		AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient = AMRMClientAsync.createAMRMClientAsync(
			yarnHeartbeatIntervalMillis,
			this);

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

		final RegisterApplicationMasterResponse registerApplicationMasterResponse =
			resourceManagerClient.registerApplicationMaster(hostPort.f0, restPort, webInterfaceUrl);
		getContainersFromPreviousAttempts(registerApplicationMasterResponse);

		return resourceManagerClient;
	}

	private void getContainersFromPreviousAttempts(final RegisterApplicationMasterResponse registerApplicationMasterResponse) {
		final List<Container> containersFromPreviousAttempts =
			new RegisterApplicationMasterResponseReflector(log).getContainersFromPreviousAttempts(registerApplicationMasterResponse);

		log.info("Recovered {} containers from previous attempts ({}).", containersFromPreviousAttempts.size(), containersFromPreviousAttempts);

		for (final Container container : containersFromPreviousAttempts) {
			workerNodeMap.put(new ResourceID(container.getId().toString()), new YarnWorkerNode(container));
		}
	}

	protected NMClient createAndStartNodeManagerClient(YarnConfiguration yarnConfiguration) {
		// create the client to communicate with the node managers
		NMClient nodeManagerClient = NMClient.createNMClient();
		nodeManagerClient.init(yarnConfiguration);
		nodeManagerClient.start();
		nodeManagerClient.cleanupRunningContainersOnStop(true);
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
	}

	@Override
	public CompletableFuture<Void> postStop() {
		// shut down all components
		Throwable firstException = null;

		if (resourceManagerClient != null) {
			try {
				resourceManagerClient.stop();
			} catch (Throwable t) {
				firstException = t;
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
		log.info("Unregister application from the YARN Resource Manager with final status {}.", yarnStatus);

		try {
			resourceManagerClient.unregisterApplicationMaster(yarnStatus, diagnostics, "");
		} catch (Throwable t) {
			log.error("Could not unregister the application master.", t);
		}

		Utils.deleteApplicationFiles(env);
	}

	@Override
	public void startNewWorker(ResourceProfile resourceProfile) {
		// Priority for worker containers - priorities are intra-application
		//TODO: set priority according to the resource allocated
		Priority priority = Priority.newInstance(generatePriority(resourceProfile));
		int mem = resourceProfile.getMemoryInMB() < 0 ? defaultTaskManagerMemoryMB : resourceProfile.getMemoryInMB();
		int vcore = resourceProfile.getCpuCores() < 1 ? defaultCpus : (int) resourceProfile.getCpuCores();
		Resource capability = Resource.newInstance(mem, vcore);
		requestYarnContainer(capability, priority);
	}

	@Override
	public boolean stopWorker(YarnWorkerNode workerNode) {
		if (workerNode != null) {
			Container container = workerNode.getContainer();
			log.info("Stopping container {}.", container.getId());
			// release the container on the node manager
			try {
				nodeManagerClient.stopContainer(container.getId(), container.getNodeId());
			} catch (Throwable t) {
				log.warn("Error while calling YARN Node Manager to stop container", t);
			}
			resourceManagerClient.releaseAssignedContainer(container.getId());
			workerNodeMap.remove(workerNode.getResourceID());
		} else {
			log.error("Can not find container for null workerNode.");
		}
		return true;
	}

	@Override
	protected YarnWorkerNode workerStarted(ResourceID resourceID) {
		return workerNodeMap.get(resourceID);
	}

	// ------------------------------------------------------------------------
	//  AMRMClientAsync CallbackHandler methods
	// ------------------------------------------------------------------------

	@Override
	public float getProgress() {
		// Temporarily need not record the total size of asked and allocated containers
		return 1;
	}

	@Override
	public void onContainersCompleted(List<ContainerStatus> list) {
		runAsync(() -> {
				for (ContainerStatus container : list) {
					if (container.getExitStatus() < 0) {
						closeTaskManagerConnection(new ResourceID(
							container.getContainerId().toString()), new Exception(container.getDiagnostics()));
					}
					workerNodeMap.remove(new ResourceID(container.getContainerId().toString()));
				}
			}
		);
	}

	@Override
	public void onContainersAllocated(List<Container> containers) {
		runAsync(() -> {
			for (Container container : containers) {
				log.info(
					"Received new container: {} - Remaining pending container requests: {}",
					container.getId(),
					numPendingContainerRequests);

				if (numPendingContainerRequests > 0) {
					numPendingContainerRequests--;

					final String containerIdStr = container.getId().toString();

					workerNodeMap.put(new ResourceID(containerIdStr), new YarnWorkerNode(container));

					try {
						// Context information used to start a TaskExecutor Java process
						ContainerLaunchContext taskExecutorLaunchContext = createTaskExecutorLaunchContext(
							container.getResource(),
							containerIdStr,
							container.getNodeId().getHost());

						nodeManagerClient.startContainer(container, taskExecutorLaunchContext);
					} catch (Throwable t) {
						log.error("Could not start TaskManager in container {}.", container.getId(), t);

						// release the failed container
						resourceManagerClient.releaseAssignedContainer(container.getId());
						// and ask for a new one
						requestYarnContainer(container.getResource(), container.getPriority());
					}
				} else {
					// return the excessive containers
					log.info("Returning excess container {}.", container.getId());
					resourceManagerClient.releaseAssignedContainer(container.getId());
				}
			}

			// if we are waiting for no further containers, we can go to the
			// regular heartbeat interval
			if (numPendingContainerRequests <= 0) {
				resourceManagerClient.setHeartbeatInterval(yarnHeartbeatIntervalMillis);
			}
		});
	}

	@Override
	public void onShutdownRequest() {
		shutDown();
	}

	@Override
	public void onNodesUpdated(List<NodeReport> list) {
		// We are not interested in node updates
	}

	@Override
	public void onError(Throwable error) {
		onFatalError(error);
	}

	// ------------------------------------------------------------------------
	//  Utility methods
	// ------------------------------------------------------------------------

	/**
	 * Converts a Flink application status enum to a YARN application status enum.
	 * @param status The Flink application status.
	 * @return The corresponding YARN application status.
	 */
	private FinalApplicationStatus getYarnStatus(ApplicationStatus status) {
		if (status == null) {
			return FinalApplicationStatus.UNDEFINED;
		}
		else {
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

	// parse the host and port from akka address,
	// the akka address is like akka.tcp://flink@100.81.153.180:49712/user/$a
	private static Tuple2<String, Integer> parseHostPort(String address) {
		String[] hostPort = address.split("@")[1].split(":");
		String host = hostPort[0];
		String port = hostPort[1].split("/")[0];
		return new Tuple2<>(host, Integer.valueOf(port));
	}

	private void requestYarnContainer(Resource resource, Priority priority) {
		resourceManagerClient.addContainerRequest(new AMRMClient.ContainerRequest(resource, null, null, priority));

		// make sure we transmit the request fast and receive fast news of granted allocations
		resourceManagerClient.setHeartbeatInterval(FAST_YARN_HEARTBEAT_INTERVAL_MS);

		numPendingContainerRequests++;

		log.info("Requesting new TaskExecutor container with resources {}. Number pending requests {}.",
			resource,
			numPendingContainerRequests);
	}

	private ContainerLaunchContext createTaskExecutorLaunchContext(Resource resource, String containerId, String host)
			throws Exception {
		// init the ContainerLaunchContext
		final String currDir = env.get(ApplicationConstants.Environment.PWD.key());

		final int numSlots = flinkConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);

		final ContaineredTaskManagerParameters taskManagerParameters =
				ContaineredTaskManagerParameters.create(flinkConfig, resource.getMemory(), numSlots);

		log.debug("TaskExecutor {} will be started with container size {} MB, JVM heap size {} MB, " +
				"JVM direct memory limit {} MB",
				containerId,
				taskManagerParameters.taskManagerTotalMemoryMB(),
				taskManagerParameters.taskManagerHeapSizeMB(),
				taskManagerParameters.taskManagerDirectMemoryLimitMB());

		log.debug("TaskManager configuration: {}", flinkConfig);

		ContainerLaunchContext taskExecutorLaunchContext = Utils.createTaskExecutorContext(
			flinkConfig,
			yarnConfig,
			env,
			taskManagerParameters,
			flinkConfig,
			currDir,
			YarnTaskExecutorRunner.class,
			log);

		// set a special environment variable to uniquely identify this container
		taskExecutorLaunchContext.getEnvironment()
				.put(ENV_FLINK_CONTAINER_ID, containerId);
		taskExecutorLaunchContext.getEnvironment()
				.put(ENV_FLINK_NODE_ID, host);
		return taskExecutorLaunchContext;
	}



	/**
	 * Generate priority by given resource profile.
	 * Priority is only used for distinguishing request of different resource.
	 * @param resourceProfile The resource profile of a request
	 * @return The priority of this resource profile.
	 */
	private int generatePriority(ResourceProfile resourceProfile) {
		if (resourcePriorities.containsKey(resourceProfile)) {
			return resourcePriorities.get(resourceProfile);
		} else {
			int priority = resourcePriorities.size();
			resourcePriorities.put(resourceProfile, priority);
			return priority;
		}
	}

}
