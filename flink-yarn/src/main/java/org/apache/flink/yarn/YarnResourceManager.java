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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
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
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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

	private static final Priority RM_REQUEST_PRIORITY = Priority.newInstance(1);
	/** The process environment variables. */
	private final Map<String, String> env;

	/** YARN container map. Package private for unit test purposes. */
	private final ConcurrentMap<ResourceID, YarnWorkerNode> workerNodeMap;
	/** Environment variable name of the final container id used by the YarnResourceManager.
	 * Container ID generation may vary across Hadoop versions. */
	private static final String ENV_FLINK_CONTAINER_ID = "_FLINK_CONTAINER_ID";

	/** Environment variable name of the hostname given by the YARN.
	 * In task executor we use the hostnames given by YARN consistently throughout akka */
	static final String ENV_FLINK_NODE_ID = "_FLINK_NODE_ID";

	/** Default heartbeat interval between this resource manager and the YARN ResourceManager. */
	private final int yarnHeartbeatIntervalMillis;

	private final Configuration flinkConfig;

	private final YarnConfiguration yarnConfig;

	@Nullable
	private final String webInterfaceUrl;

	private final int numberOfTaskSlots;

	private final int defaultTaskManagerMemoryMB;

	private final int defaultCpus;

	/** The heartbeat interval while the resource master is waiting for containers. */
	private final int containerRequestHeartbeatIntervalMillis;

	/** Client to communicate with the Resource Manager (YARN's master). */
	private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient;

	/** Client to communicate with the Node manager and launch TaskExecutor processes. */
	private NMClient nodeManagerClient;

	/** The number of containers requested, but not yet granted. */
	private int numPendingContainerRequests;

	private final Map<ResourceProfile, Integer> resourcePriorities = new HashMap<>();

	private final Collection<ResourceProfile> slotsPerWorker;

	private final Resource resource;

	public YarnResourceManager(
			RpcService rpcService,
			String resourceManagerEndpointId,
			ResourceID resourceId,
			Configuration flinkConfig,
			Map<String, String> env,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			MetricRegistry metricRegistry,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			@Nullable String webInterfaceUrl,
			JobManagerMetricGroup jobManagerMetricGroup) {
		super(
			rpcService,
			resourceManagerEndpointId,
			resourceId,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			metricRegistry,
			jobLeaderIdService,
			clusterInformation,
			fatalErrorHandler,
			jobManagerMetricGroup);
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
		containerRequestHeartbeatIntervalMillis = flinkConfig.getInteger(YarnConfigOptions.CONTAINER_REQUEST_HEARTBEAT_INTERVAL_MILLISECONDS);
		numPendingContainerRequests = 0;

		this.webInterfaceUrl = webInterfaceUrl;
		this.numberOfTaskSlots = flinkConfig.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);
		this.defaultTaskManagerMemoryMB = ConfigurationUtils.getTaskManagerHeapMemory(flinkConfig).getMebiBytes();
		this.defaultCpus = flinkConfig.getInteger(YarnConfigOptions.VCORES, numberOfTaskSlots);
		this.resource = Resource.newInstance(defaultTaskManagerMemoryMB, defaultCpus);

		this.slotsPerWorker = createSlotsPerWorker(numberOfTaskSlots);
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
	public Collection<ResourceProfile> startNewWorker(ResourceProfile resourceProfile) {
		Preconditions.checkArgument(
			ResourceProfile.UNKNOWN.equals(resourceProfile),
			"The YarnResourceManager does not support custom ResourceProfiles yet. It assumes that all containers have the same resources.");
		requestYarnContainer();

		return slotsPerWorker;
	}

	@VisibleForTesting
	Resource getContainerResource() {
		return resource;
	}

	@Override
	public boolean stopWorker(final YarnWorkerNode workerNode) {
		final Container container = workerNode.getContainer();
		log.info("Stopping container {}.", container.getId());
		try {
			nodeManagerClient.stopContainer(container.getId(), container.getNodeId());
		} catch (final Exception e) {
			log.warn("Error while calling YARN Node Manager to stop container", e);
		}
		resourceManagerClient.releaseAssignedContainer(container.getId());
		workerNodeMap.remove(workerNode.getResourceID());
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
	public void onContainersCompleted(final List<ContainerStatus> statuses) {
		runAsync(() -> {
				log.debug("YARN ResourceManager reported the following containers completed: {}.", statuses);
				for (final ContainerStatus containerStatus : statuses) {

					final ResourceID resourceId = new ResourceID(containerStatus.getContainerId().toString());
					final YarnWorkerNode yarnWorkerNode = workerNodeMap.remove(resourceId);

					if (yarnWorkerNode != null) {
						// Container completed unexpectedly ~> start a new one
						requestYarnContainerIfRequired();
					}
					// Eagerly close the connection with task manager.
					closeTaskManagerConnection(resourceId, new Exception(containerStatus.getDiagnostics()));
				}
			}
		);
	}

	@Override
	public void onContainersAllocated(List<Container> containers) {
		runAsync(() -> {
			final Collection<AMRMClient.ContainerRequest> pendingRequests = getPendingRequests();
			final Iterator<AMRMClient.ContainerRequest> pendingRequestsIterator = pendingRequests.iterator();

			for (Container container : containers) {
				log.info(
					"Received new container: {} - Remaining pending container requests: {}",
					container.getId(),
					numPendingContainerRequests);

				if (numPendingContainerRequests > 0) {
					removeContainerRequest(pendingRequestsIterator.next());

					final String containerIdStr = container.getId().toString();
					final ResourceID resourceId = new ResourceID(containerIdStr);

					workerNodeMap.put(resourceId, new YarnWorkerNode(container));

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
						workerNodeMap.remove(resourceId);
						resourceManagerClient.releaseAssignedContainer(container.getId());
						// and ask for a new one
						requestYarnContainerIfRequired();
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

	private void removeContainerRequest(AMRMClient.ContainerRequest pendingContainerRequest) {
		numPendingContainerRequests--;

		log.info("Removing container request {}. Pending container requests {}.", pendingContainerRequest, numPendingContainerRequests);

		resourceManagerClient.removeContainerRequest(pendingContainerRequest);
	}

	private Collection<AMRMClient.ContainerRequest> getPendingRequests() {
		final List<? extends Collection<AMRMClient.ContainerRequest>> matchingRequests = resourceManagerClient.getMatchingRequests(
			RM_REQUEST_PRIORITY,
			ResourceRequest.ANY,
			getContainerResource());

		final Collection<AMRMClient.ContainerRequest> matchingContainerRequests;

		if (matchingRequests.isEmpty()) {
			matchingContainerRequests = Collections.emptyList();
		} else {
			final Collection<AMRMClient.ContainerRequest> collection = matchingRequests.get(0);
			matchingContainerRequests = new ArrayList<>(collection);
		}

		Preconditions.checkState(
			matchingContainerRequests.size() == numPendingContainerRequests,
			"The RMClient's and YarnResourceManagers internal state about the number of pending container requests has diverged. Number client's pending container requests %s != Number RM's pending container requests %s.", matchingContainerRequests.size(), numPendingContainerRequests);

		return matchingContainerRequests;
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

	/**
	 * Request new container if pending containers cannot satisfies pending slot requests.
	 */
	private void requestYarnContainerIfRequired() {
		int requiredTaskManagerSlots = getNumberRequiredTaskManagerSlots();
		int pendingTaskManagerSlots = numPendingContainerRequests * numberOfTaskSlots;

		if (requiredTaskManagerSlots > pendingTaskManagerSlots) {
			requestYarnContainer();
		}
	}

	private void requestYarnContainer() {
		resourceManagerClient.addContainerRequest(getContainerRequest());

		// make sure we transmit the request fast and receive fast news of granted allocations
		resourceManagerClient.setHeartbeatInterval(containerRequestHeartbeatIntervalMillis);
		numPendingContainerRequests++;

		log.info("Requesting new TaskExecutor container with resources {}. Number pending requests {}.",
			resource,
			numPendingContainerRequests);
	}

	@Nonnull
	@VisibleForTesting
	AMRMClient.ContainerRequest getContainerRequest() {
		return new AMRMClient.ContainerRequest(
			getContainerResource(),
			null,
			null,
			RM_REQUEST_PRIORITY);
	}

	private ContainerLaunchContext createTaskExecutorLaunchContext(Resource resource, String containerId, String host)
			throws Exception {
		// init the ContainerLaunchContext
		final String currDir = env.get(ApplicationConstants.Environment.PWD.key());

		final ContaineredTaskManagerParameters taskManagerParameters =
				ContaineredTaskManagerParameters.create(flinkConfig, resource.getMemory(), numberOfTaskSlots);

		log.debug("TaskExecutor {} will be started with container size {} MB, JVM heap size {} MB, " +
				"JVM direct memory limit {} MB",
				containerId,
				taskManagerParameters.taskManagerTotalMemoryMB(),
				taskManagerParameters.taskManagerHeapSizeMB(),
				taskManagerParameters.taskManagerDirectMemoryLimitMB());

		Configuration taskManagerConfig = BootstrapTools.cloneConfiguration(flinkConfig);

		log.debug("TaskManager configuration: {}", taskManagerConfig);

		ContainerLaunchContext taskExecutorLaunchContext = Utils.createTaskExecutorContext(
			flinkConfig,
			yarnConfig,
			env,
			taskManagerParameters,
			taskManagerConfig,
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
}
