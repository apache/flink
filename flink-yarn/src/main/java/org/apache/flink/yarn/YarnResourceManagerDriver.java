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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.TaskManagerOptionsInternal;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.resourcemanager.active.AbstractResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.active.ResourceManagerDriver;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.webmonitor.history.HistoryServerUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.apache.flink.yarn.configuration.YarnResourceManagerDriverConfiguration;

import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Implementation of {@link ResourceManagerDriver} for Yarn deployment.
 */
public class YarnResourceManagerDriver extends AbstractResourceManagerDriver<YarnWorkerNode> {

	private static final Priority RM_REQUEST_PRIORITY = Priority.newInstance(1);

	/** Environment variable name of the hostname given by YARN.
	 * In task executor we use the hostnames given by YARN consistently throughout akka */
	static final String ENV_FLINK_NODE_ID = "_FLINK_NODE_ID";

	static final String ERROR_MESSAGE_ON_SHUTDOWN_REQUEST = "Received shutdown request from YARN ResourceManager.";

	private final YarnConfiguration yarnConfig;

	/** The process environment variables. */
	private final YarnResourceManagerDriverConfiguration configuration;

	/** Default heartbeat interval between this resource manager and the YARN ResourceManager. */
	private final int yarnHeartbeatIntervalMillis;

	/** The heartbeat interval while the resource master is waiting for containers. */
	private final int containerRequestHeartbeatIntervalMillis;

	/** Request resource futures, keyed by container's TaskExecutorProcessSpec. */
	private final Map<TaskExecutorProcessSpec, Queue<CompletableFuture<YarnWorkerNode>>> requestResourceFutures;

	private final TaskExecutorProcessSpecContainerResourceAdapter taskExecutorProcessSpecContainerResourceAdapter;

	private final RegisterApplicationMasterResponseReflector registerApplicationMasterResponseReflector;

	private final YarnResourceManagerClientFactory yarnResourceManagerClientFactory;

	private final YarnNodeManagerClientFactory yarnNodeManagerClientFactory;

	/** Client to communicate with the Resource Manager (YARN's master). */
	private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient;

	/** Client to communicate with the Node manager and launch TaskExecutor processes. */
	private NMClientAsync nodeManagerClient;

	private TaskExecutorProcessSpecContainerResourceAdapter.MatchingStrategy matchingStrategy;

	public YarnResourceManagerDriver(
		Configuration flinkConfig,
		YarnResourceManagerDriverConfiguration configuration,
		YarnResourceManagerClientFactory yarnResourceManagerClientFactory,
		YarnNodeManagerClientFactory yarnNodeManagerClientFactory) {
		super(flinkConfig, GlobalConfiguration.loadConfiguration(configuration.getCurrentDir()));

		this.yarnConfig = new YarnConfiguration();
		this.requestResourceFutures = new HashMap<>();
		this.configuration = configuration;

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

		this.taskExecutorProcessSpecContainerResourceAdapter = Utils.createTaskExecutorProcessSpecContainerResourceAdapter(flinkConfig, yarnConfig);
		this.registerApplicationMasterResponseReflector = new RegisterApplicationMasterResponseReflector(log);

		this.matchingStrategy = flinkConfig.getBoolean(YarnConfigOptionsInternal.MATCH_CONTAINER_VCORES) ?
			TaskExecutorProcessSpecContainerResourceAdapter.MatchingStrategy.MATCH_VCORE :
			TaskExecutorProcessSpecContainerResourceAdapter.MatchingStrategy.IGNORE_VCORE;

		this.yarnResourceManagerClientFactory = yarnResourceManagerClientFactory;
		this.yarnNodeManagerClientFactory = yarnNodeManagerClientFactory;
	}

	// ------------------------------------------------------------------------
	//  ResourceManagerDriver
	// ------------------------------------------------------------------------

	@Override
	protected void initializeInternal() throws Exception {
		final YarnContainerEventHandler yarnContainerEventHandler = new YarnContainerEventHandler();
		try {
			resourceManagerClient = yarnResourceManagerClientFactory.createResourceManagerClient(
				yarnHeartbeatIntervalMillis,
				yarnContainerEventHandler);
			resourceManagerClient.init(yarnConfig);
			resourceManagerClient.start();

			final RegisterApplicationMasterResponse registerApplicationMasterResponse = registerApplicationMaster();
			getContainersFromPreviousAttempts(registerApplicationMasterResponse);
			updateMatchingStrategy(registerApplicationMasterResponse);
		} catch (Exception e) {
			throw new ResourceManagerException("Could not start resource manager client.", e);
		}

		nodeManagerClient = yarnNodeManagerClientFactory.createNodeManagerClient(yarnContainerEventHandler);
		nodeManagerClient.init(yarnConfig);
		nodeManagerClient.start();
	}

	@Override
	public CompletableFuture<Void> terminate() {
		// shut down all components
		Exception exception = null;

		if (resourceManagerClient != null) {
			try {
				resourceManagerClient.stop();
			} catch (Exception e) {
				exception = e;
			}
		}

		if (nodeManagerClient != null) {
			try {
				nodeManagerClient.stop();
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}
		}

		return exception == null ?
			FutureUtils.completedVoidFuture() :
			FutureUtils.completedExceptionally(exception);
	}

	@Override
	public void deregisterApplication(ApplicationStatus finalStatus, @Nullable String optionalDiagnostics) {
		// first, de-register from YARN
		final FinalApplicationStatus yarnStatus = getYarnStatus(finalStatus);
		log.info("Unregister application from the YARN Resource Manager with final status {}.", yarnStatus);

		final Optional<URL> historyServerURL = HistoryServerUtils.getHistoryServerURL(flinkConfig);

		final String appTrackingUrl = historyServerURL.map(URL::toString).orElse("");

		try {
			resourceManagerClient.unregisterApplicationMaster(yarnStatus, optionalDiagnostics, appTrackingUrl);
		} catch (YarnException | IOException e) {
			log.error("Could not unregister the application master.", e);
		}

		Utils.deleteApplicationFiles(configuration.getYarnFiles());
	}

	@Override
	public CompletableFuture<YarnWorkerNode> requestResource(TaskExecutorProcessSpec taskExecutorProcessSpec) {
		final Optional<Resource> containerResourceOptional = getContainerResource(taskExecutorProcessSpec);
		final CompletableFuture<YarnWorkerNode> requestResourceFuture = new CompletableFuture<>();

		if (containerResourceOptional.isPresent()) {
			resourceManagerClient.addContainerRequest(getContainerRequest(containerResourceOptional.get()));

			// make sure we transmit the request fast and receive fast news of granted allocations
			resourceManagerClient.setHeartbeatInterval(containerRequestHeartbeatIntervalMillis);

			requestResourceFutures.computeIfAbsent(taskExecutorProcessSpec, ignore -> new LinkedList<>()).add(requestResourceFuture);

			log.info("Requesting new TaskExecutor container with resource {}.", taskExecutorProcessSpec);
		} else {
			requestResourceFuture.completeExceptionally(
				new ResourceManagerException(String.format("Could not compute the container Resource from the given TaskExecutorProcessSpec %s.", taskExecutorProcessSpec)));
		}

		return requestResourceFuture;
	}

	@Override
	public void releaseResource(YarnWorkerNode workerNode) {
		final Container container = workerNode.getContainer();
		log.info("Stopping container {}.", workerNode.getResourceID().getStringWithMetadata());
		nodeManagerClient.stopContainerAsync(container.getId(), container.getNodeId());
		resourceManagerClient.releaseAssignedContainer(container.getId());
	}

	// ------------------------------------------------------------------------
	//  Internal
	// ------------------------------------------------------------------------

	private void onContainersOfResourceAllocated(Resource resource, List<Container> containers) {
		final List<TaskExecutorProcessSpec> pendingTaskExecutorProcessSpecs =
			taskExecutorProcessSpecContainerResourceAdapter.getTaskExecutorProcessSpec(resource, matchingStrategy).stream()
				.flatMap(spec -> Collections.nCopies(getNumRequestedNotAllocatedWorkersFor(spec), spec).stream())
				.collect(Collectors.toList());

		int numPending = pendingTaskExecutorProcessSpecs.size();
		log.info("Received {} containers with resource {}, {} pending container requests.",
			containers.size(),
			resource,
			numPending);

		final Iterator<Container> containerIterator = containers.iterator();
		final Iterator<TaskExecutorProcessSpec> pendingTaskExecutorProcessSpecIterator = pendingTaskExecutorProcessSpecs.iterator();
		final Iterator<AMRMClient.ContainerRequest> pendingRequestsIterator =
			getPendingRequestsAndCheckConsistency(resource, pendingTaskExecutorProcessSpecs.size()).iterator();

		int numAccepted = 0;
		while (containerIterator.hasNext() && pendingTaskExecutorProcessSpecIterator.hasNext()) {
			final TaskExecutorProcessSpec taskExecutorProcessSpec = pendingTaskExecutorProcessSpecIterator.next();
			final Container container = containerIterator.next();
			final AMRMClient.ContainerRequest pendingRequest = pendingRequestsIterator.next();
			final ResourceID resourceId = getContainerResourceId(container);
			final CompletableFuture<YarnWorkerNode> requestResourceFuture =
				Preconditions.checkNotNull(
					Preconditions.checkNotNull(
						requestResourceFutures.get(taskExecutorProcessSpec),
						"The requestResourceFuture for TasExecutorProcessSpec %s should not be null.", taskExecutorProcessSpec).poll(),
					"The requestResourceFuture queue for TasExecutorProcessSpec %s should not be empty.", taskExecutorProcessSpec);
			if (requestResourceFutures.get(taskExecutorProcessSpec).isEmpty()) {
				requestResourceFutures.remove(taskExecutorProcessSpec);
			}

			startTaskExecutorInContainer(container, taskExecutorProcessSpec, resourceId, requestResourceFuture);
			removeContainerRequest(pendingRequest);

			numAccepted++;
		}
		numPending -= numAccepted;

		int numExcess = 0;
		while (containerIterator.hasNext()) {
			returnExcessContainer(containerIterator.next());
			numExcess++;
		}

		log.info("Accepted {} requested containers, returned {} excess containers, {} pending container requests of resource {}.",
			numAccepted, numExcess, numPending, resource);
	}

	private int getNumRequestedNotAllocatedWorkers() {
		return requestResourceFutures.values().stream().mapToInt(Queue::size).sum();
	}

	private int getNumRequestedNotAllocatedWorkersFor(TaskExecutorProcessSpec taskExecutorProcessSpec) {
		return requestResourceFutures.getOrDefault(taskExecutorProcessSpec, new LinkedList<>()).size();
	}

	private void removeContainerRequest(AMRMClient.ContainerRequest pendingContainerRequest) {
		log.info("Removing container request {}.", pendingContainerRequest);
		resourceManagerClient.removeContainerRequest(pendingContainerRequest);
	}

	private void returnExcessContainer(Container excessContainer) {
		log.info("Returning excess container {}.", excessContainer.getId());
		resourceManagerClient.releaseAssignedContainer(excessContainer.getId());
	}

	private void startTaskExecutorInContainer(Container container, TaskExecutorProcessSpec taskExecutorProcessSpec, ResourceID resourceId, CompletableFuture<YarnWorkerNode> requestResourceFuture) {
		final YarnWorkerNode yarnWorkerNode = new YarnWorkerNode(container, resourceId);

		try {
			// Context information used to start a TaskExecutor Java process
			ContainerLaunchContext taskExecutorLaunchContext = createTaskExecutorLaunchContext(
				resourceId,
				container.getNodeId().getHost(),
				taskExecutorProcessSpec);

			nodeManagerClient.startContainerAsync(container, taskExecutorLaunchContext);
			requestResourceFuture.complete(yarnWorkerNode);
		} catch (Throwable t) {
			requestResourceFuture.completeExceptionally(t);
		}
	}

	private Collection<AMRMClient.ContainerRequest> getPendingRequestsAndCheckConsistency(Resource resource, int expectedNum) {
		final Collection<Resource> equivalentResources = taskExecutorProcessSpecContainerResourceAdapter.getEquivalentContainerResource(resource, matchingStrategy);
		final List<? extends Collection<AMRMClient.ContainerRequest>> matchingRequests =
			equivalentResources.stream()
				.flatMap(equivalentResource -> resourceManagerClient.getMatchingRequests(
					RM_REQUEST_PRIORITY,
					ResourceRequest.ANY,
					equivalentResource).stream())
				.collect(Collectors.toList());

		final Collection<AMRMClient.ContainerRequest> matchingContainerRequests;

		if (matchingRequests.isEmpty()) {
			matchingContainerRequests = Collections.emptyList();
		} else {
			final Collection<AMRMClient.ContainerRequest> collection = matchingRequests.get(0);
			matchingContainerRequests = new ArrayList<>(collection);
		}

		Preconditions.checkState(
			matchingContainerRequests.size() == expectedNum,
			"The RMClient's and YarnResourceManagers internal state about the number of pending container requests for resource %s has diverged. " +
				"Number client's pending container requests %s != Number RM's pending container requests %s.",
			resource, matchingContainerRequests.size(), expectedNum);

		return matchingContainerRequests;
	}

	private ContainerLaunchContext createTaskExecutorLaunchContext(
		ResourceID containerId,
		String host,
		TaskExecutorProcessSpec taskExecutorProcessSpec) throws Exception {

		// init the ContainerLaunchContext
		final String currDir = configuration.getCurrentDir();

		final ContaineredTaskManagerParameters taskManagerParameters =
			ContaineredTaskManagerParameters.create(flinkConfig, taskExecutorProcessSpec);

		log.info("TaskExecutor {} will be started on {} with {}.",
			containerId.getStringWithMetadata(),
			host,
			taskExecutorProcessSpec);

		final Configuration taskManagerConfig = BootstrapTools.cloneConfiguration(flinkConfig);
		taskManagerConfig.set(TaskManagerOptions.TASK_MANAGER_RESOURCE_ID, containerId.getResourceIdString());
		taskManagerConfig.set(TaskManagerOptionsInternal.TASK_MANAGER_RESOURCE_ID_METADATA, containerId.getMetadata());

		final String taskManagerDynamicProperties =
			BootstrapTools.getDynamicPropertiesAsString(flinkClientConfig, taskManagerConfig);

		log.debug("TaskManager configuration: {}", taskManagerConfig);

		final ContainerLaunchContext taskExecutorLaunchContext = Utils.createTaskExecutorContext(
			flinkConfig,
			yarnConfig,
			configuration,
			taskManagerParameters,
			taskManagerDynamicProperties,
			currDir,
			YarnTaskExecutorRunner.class,
			log);

		taskExecutorLaunchContext.getEnvironment()
			.put(ENV_FLINK_NODE_ID, host);
		return taskExecutorLaunchContext;
	}

	@VisibleForTesting
	Optional<Resource> getContainerResource(TaskExecutorProcessSpec taskExecutorProcessSpec) {
		return taskExecutorProcessSpecContainerResourceAdapter.tryComputeContainerResource(taskExecutorProcessSpec);
	}

	private RegisterApplicationMasterResponse registerApplicationMaster() throws Exception {
		final int restPort;
		final String webInterfaceUrl = configuration.getWebInterfaceUrl();
		final String rpcAddress = configuration.getRpcAddress();

		if (webInterfaceUrl != null) {
			final int lastColon = webInterfaceUrl.lastIndexOf(':');

			if (lastColon == -1) {
				restPort = -1;
			} else {
				restPort = Integer.parseInt(webInterfaceUrl.substring(lastColon + 1));
			}
		} else {
			restPort = -1;
		}

		return resourceManagerClient.registerApplicationMaster(rpcAddress, restPort, webInterfaceUrl);
	}

	private void getContainersFromPreviousAttempts(final RegisterApplicationMasterResponse registerApplicationMasterResponse) {
		final List<Container> containersFromPreviousAttempts =
			registerApplicationMasterResponseReflector.getContainersFromPreviousAttempts(registerApplicationMasterResponse);
		final List<YarnWorkerNode> recoveredWorkers = new ArrayList<>();

		log.info("Recovered {} containers from previous attempts ({}).", containersFromPreviousAttempts.size(), containersFromPreviousAttempts);

		for (Container container : containersFromPreviousAttempts) {
			final YarnWorkerNode worker = new YarnWorkerNode(container, getContainerResourceId(container));
			recoveredWorkers.add(worker);
		}

		// Should not invoke resource event handler on the main thread executor.
		// We are in the initializing thread. The main thread executor is not yet ready.
		getResourceEventHandler().onPreviousAttemptWorkersRecovered(recoveredWorkers);
	}

	private void updateMatchingStrategy(final RegisterApplicationMasterResponse registerApplicationMasterResponse) {
		final Optional<Set<String>> schedulerResourceTypesOptional =
			registerApplicationMasterResponseReflector.getSchedulerResourceTypeNames(registerApplicationMasterResponse);

		if (schedulerResourceTypesOptional.isPresent()) {
			Set<String> types = schedulerResourceTypesOptional.get();
			log.info("Register application master response contains scheduler resource types: {}.", types);
			matchingStrategy = types.contains("CPU") ?
				TaskExecutorProcessSpecContainerResourceAdapter.MatchingStrategy.MATCH_VCORE :
				TaskExecutorProcessSpecContainerResourceAdapter.MatchingStrategy.IGNORE_VCORE;
		} else {
			log.info("Register application master response does not contain scheduler resource types, use '{}'.",
				YarnConfigOptionsInternal.MATCH_CONTAINER_VCORES.key());
		}
		log.info("Container matching strategy: {}.", matchingStrategy);
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

	@Nonnull
	@VisibleForTesting
	static AMRMClient.ContainerRequest getContainerRequest(Resource containerResource) {
		return new AMRMClient.ContainerRequest(
			containerResource,
			null,
			null,
			RM_REQUEST_PRIORITY);
	}

	@VisibleForTesting
	private static ResourceID getContainerResourceId(Container container) {
		return new ResourceID(container.getId().toString(), container.getNodeId().toString());
	}

	private Map<Resource, List<Container>> groupContainerByResource(List<Container> containers) {
		return containers.stream().collect(Collectors.groupingBy(Container::getResource));
	}

	// ------------------------------------------------------------------------
	//  Event handlers
	// ------------------------------------------------------------------------

	class YarnContainerEventHandler implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

		@Override
		public void onContainersCompleted(List<ContainerStatus> statuses) {
			getMainThreadExecutor().execute(() -> {
					log.debug("YARN ResourceManager reported the following containers completed: {}.", statuses);
					for (final ContainerStatus containerStatus : statuses) {

						final String containerId = containerStatus.getContainerId().toString();
						getResourceEventHandler().onWorkerTerminated(new ResourceID(containerId), containerStatus.getDiagnostics());
					}
				}
			);
		}

		@Override
		public void onContainersAllocated(List<Container> containers) {
			getMainThreadExecutor().execute(() -> {
				log.info("Received {} containers.", containers.size());

				for (Map.Entry<Resource, List<Container>> entry : groupContainerByResource(containers).entrySet()) {
					onContainersOfResourceAllocated(entry.getKey(), entry.getValue());
				}

				// if we are waiting for no further containers, we can go to the
				// regular heartbeat interval
				if (getNumRequestedNotAllocatedWorkers() <= 0) {
					resourceManagerClient.setHeartbeatInterval(yarnHeartbeatIntervalMillis);
				}
			});
		}

		@Override
		public void onShutdownRequest() {
			getResourceEventHandler().onError(new ResourceManagerException(ERROR_MESSAGE_ON_SHUTDOWN_REQUEST));
		}

		@Override
		public void onNodesUpdated(List<NodeReport> list) {
			// We are not interested in node updates
		}

		@Override
		public float getProgress() {
			// Temporarily need not record the total size of asked and allocated containers
			return 1;
		}

		@Override
		public void onError(Throwable throwable) {
			getResourceEventHandler().onError(throwable);
		}

		@Override
		public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {
			log.debug("Succeeded to call YARN Node Manager to start container {}.", containerId);
		}

		@Override
		public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
			// We are not interested in getting container status
		}

		@Override
		public void onContainerStopped(ContainerId containerId) {
			log.debug("Succeeded to call YARN Node Manager to stop container {}.", containerId);
		}

		@Override
		public void onStartContainerError(ContainerId containerId, Throwable throwable) {
			getMainThreadExecutor().execute(() -> {
				resourceManagerClient.releaseAssignedContainer(containerId);
				getResourceEventHandler().onWorkerTerminated(new ResourceID(containerId.toString()), throwable.getMessage());
			});
		}

		@Override
		public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {
			// We are not interested in getting container status
		}

		@Override
		public void onStopContainerError(ContainerId containerId, Throwable throwable) {
			log.warn("Error while calling YARN Node Manager to stop container {}.", containerId, throwable);
		}
	}
}
