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
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.TaskManagerOptionsInternal;
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
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.active.LegacyActiveResourceManager;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.webmonitor.history.HistoryServerUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;

import org.apache.hadoop.yarn.api.ApplicationConstants;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * The yarn implementation of the resource manager. Used when the system is started
 * via the resource framework YARN.
 */
public class YarnResourceManager extends LegacyActiveResourceManager<YarnWorkerNode>
		implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

	private static final Priority RM_REQUEST_PRIORITY = Priority.newInstance(1);

	/** YARN container map. */
	private final ConcurrentMap<ResourceID, YarnWorkerNode> workerNodeMap;

	/** Environment variable name of the hostname given by the YARN.
	 * In task executor we use the hostnames given by YARN consistently throughout akka */
	static final String ENV_FLINK_NODE_ID = "_FLINK_NODE_ID";

	static final String ERROR_MASSAGE_ON_SHUTDOWN_REQUEST = "Received shutdown request from YARN ResourceManager.";

	/** Default heartbeat interval between this resource manager and the YARN ResourceManager. */
	private final int yarnHeartbeatIntervalMillis;

	private final YarnConfiguration yarnConfig;

	@Nullable
	private final String webInterfaceUrl;

	/** The heartbeat interval while the resource master is waiting for containers. */
	private final int containerRequestHeartbeatIntervalMillis;

	/** Client to communicate with the Resource Manager (YARN's master). */
	private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient;

	/** Client to communicate with the Node manager and launch TaskExecutor processes. */
	private NMClientAsync nodeManagerClient;

	private final WorkerSpecContainerResourceAdapter workerSpecContainerResourceAdapter;

	private final RegisterApplicationMasterResponseReflector registerApplicationMasterResponseReflector;

	private WorkerSpecContainerResourceAdapter.MatchingStrategy matchingStrategy;

	public YarnResourceManager(
			RpcService rpcService,
			ResourceID resourceId,
			Configuration flinkConfig,
			Map<String, String> env,
			HighAvailabilityServices highAvailabilityServices,
			HeartbeatServices heartbeatServices,
			SlotManager slotManager,
			ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
			JobLeaderIdService jobLeaderIdService,
			ClusterInformation clusterInformation,
			FatalErrorHandler fatalErrorHandler,
			@Nullable String webInterfaceUrl,
			ResourceManagerMetricGroup resourceManagerMetricGroup) {
		super(
			flinkConfig,
			env,
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
		this.yarnConfig = new YarnConfiguration();
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

		this.webInterfaceUrl = webInterfaceUrl;

		this.workerSpecContainerResourceAdapter = new WorkerSpecContainerResourceAdapter(
			flinkConfig,
			yarnConfig.getInt(
				YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_MB,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_MB),
			yarnConfig.getInt(
				YarnConfiguration.RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_MINIMUM_ALLOCATION_VCORES),
			yarnConfig.getInt(
				YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_MB,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_MB),
			yarnConfig.getInt(
				YarnConfiguration.RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES,
				YarnConfiguration.DEFAULT_RM_SCHEDULER_MAXIMUM_ALLOCATION_VCORES),
			ExternalResourceUtils.getExternalResources(flinkConfig, YarnConfigOptions.EXTERNAL_RESOURCE_YARN_CONFIG_KEY_SUFFIX));
		this.registerApplicationMasterResponseReflector = new RegisterApplicationMasterResponseReflector(log);

		this.matchingStrategy = flinkConfig.getBoolean(YarnConfigOptionsInternal.MATCH_CONTAINER_VCORES) ?
			WorkerSpecContainerResourceAdapter.MatchingStrategy.MATCH_VCORE :
			WorkerSpecContainerResourceAdapter.MatchingStrategy.IGNORE_VCORE;
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
		updateMatchingStrategy(registerApplicationMasterResponse);

		return resourceManagerClient;
	}

	private void getContainersFromPreviousAttempts(final RegisterApplicationMasterResponse registerApplicationMasterResponse) {
		final List<Container> containersFromPreviousAttempts =
			registerApplicationMasterResponseReflector.getContainersFromPreviousAttempts(registerApplicationMasterResponse);

		log.info("Recovered {} containers from previous attempts ({}).", containersFromPreviousAttempts.size(), containersFromPreviousAttempts);

		for (final Container container : containersFromPreviousAttempts) {
			final ResourceID resourceID = getContainerResourceId(container);
			workerNodeMap.put(resourceID, new YarnWorkerNode(container, resourceID));
		}
	}

	private void updateMatchingStrategy(final RegisterApplicationMasterResponse registerApplicationMasterResponse) {
		final Optional<Set<String>> schedulerResourceTypesOptional =
			registerApplicationMasterResponseReflector.getSchedulerResourceTypeNames(registerApplicationMasterResponse);

		final WorkerSpecContainerResourceAdapter.MatchingStrategy strategy;
		if (schedulerResourceTypesOptional.isPresent()) {
			Set<String> types = schedulerResourceTypesOptional.get();
			log.info("Register application master response contains scheduler resource types: {}.", types);
			matchingStrategy = types.contains("CPU") ?
				WorkerSpecContainerResourceAdapter.MatchingStrategy.MATCH_VCORE :
				WorkerSpecContainerResourceAdapter.MatchingStrategy.IGNORE_VCORE;
		} else {
			log.info("Register application master response does not contain scheduler resource types, use '{}'.",
				YarnConfigOptionsInternal.MATCH_CONTAINER_VCORES.key());
		}
		log.info("Container matching strategy: {}.", matchingStrategy);
	}

	protected NMClientAsync createAndStartNodeManagerClient(YarnConfiguration yarnConfiguration) {
		// create the client to communicate with the node managers
		NMClientAsync nodeManagerClient = NMClientAsync.createNMClientAsync(this);
		nodeManagerClient.init(yarnConfiguration);
		nodeManagerClient.start();
		return nodeManagerClient;
	}

	@Override
	protected Configuration loadClientConfiguration() {
		return GlobalConfiguration.loadConfiguration(env.get(ApplicationConstants.Environment.PWD.key()));
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
	public void terminate() throws Exception {
		// shut down all components
		Exception firstException = null;

		if (resourceManagerClient != null) {
			try {
				resourceManagerClient.stop();
			} catch (Exception e) {
				firstException = e;
			}
		}

		if (nodeManagerClient != null) {
			try {
				nodeManagerClient.stop();
			} catch (Exception e) {
				firstException = ExceptionUtils.firstOrSuppressed(e, firstException);
			}
		}

		ExceptionUtils.tryRethrowException(firstException);
	}

	@Override
	protected void internalDeregisterApplication(
			ApplicationStatus finalStatus,
			@Nullable String diagnostics) {

		// first, de-register from YARN
		FinalApplicationStatus yarnStatus = getYarnStatus(finalStatus);
		log.info("Unregister application from the YARN Resource Manager with final status {}.", yarnStatus);

		final Optional<URL> historyServerURL = HistoryServerUtils.getHistoryServerURL(flinkConfig);

		final String appTrackingUrl = historyServerURL.map(URL::toString).orElse("");

		try {
			resourceManagerClient.unregisterApplicationMaster(yarnStatus, diagnostics, appTrackingUrl);
		} catch (Throwable t) {
			log.error("Could not unregister the application master.", t);
		}

		Utils.deleteApplicationFiles(env);
	}

	@Override
	public boolean startNewWorker(WorkerResourceSpec workerResourceSpec) {
		return requestYarnContainer(workerResourceSpec);
	}

	@VisibleForTesting
	Optional<Resource> getContainerResource(WorkerResourceSpec workerResourceSpec) {
		return workerSpecContainerResourceAdapter.tryComputeContainerResource(workerResourceSpec);
	}

	@Override
	public boolean stopWorker(final YarnWorkerNode workerNode) {
		final Container container = workerNode.getContainer();
		log.info("Stopping container {}.", workerNode.getResourceID().getStringWithMetadata());
		nodeManagerClient.stopContainerAsync(container.getId(), container.getNodeId());
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

					notifyAllocatedWorkerStopped(resourceId);

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

	private Map<Resource, List<Container>> groupContainerByResource(List<Container> containers) {
		return containers.stream().collect(Collectors.groupingBy(Container::getResource));
	}

	private void onContainersOfResourceAllocated(Resource resource, List<Container> containers) {
		final List<WorkerResourceSpec> pendingWorkerResourceSpecs =
			workerSpecContainerResourceAdapter.getWorkerSpecs(resource, matchingStrategy).stream()
				.flatMap(spec -> Collections.nCopies(getNumRequestedNotAllocatedWorkersFor(spec), spec).stream())
				.collect(Collectors.toList());

		int numPending = pendingWorkerResourceSpecs.size();
		log.info("Received {} containers with resource {}, {} pending container requests.",
			containers.size(),
			resource,
			numPending);

		final Iterator<Container> containerIterator = containers.iterator();
		final Iterator<WorkerResourceSpec> pendingWorkerSpecIterator = pendingWorkerResourceSpecs.iterator();
		final Iterator<AMRMClient.ContainerRequest> pendingRequestsIterator =
			getPendingRequestsAndCheckConsistency(resource, pendingWorkerResourceSpecs.size()).iterator();

		int numAccepted = 0;
		while (containerIterator.hasNext() && pendingWorkerSpecIterator.hasNext()) {
			final WorkerResourceSpec workerResourceSpec = pendingWorkerSpecIterator.next();
			final Container container = containerIterator.next();
			final AMRMClient.ContainerRequest pendingRequest = pendingRequestsIterator.next();
			final ResourceID resourceId = getContainerResourceId(container);

			notifyNewWorkerAllocated(workerResourceSpec, resourceId);
			startTaskExecutorInContainer(container, workerResourceSpec, resourceId);
			removeContainerRequest(pendingRequest, workerResourceSpec);

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

	@VisibleForTesting
	static ResourceID getContainerResourceId(Container container) {
		return new ResourceID(container.getId().toString(), container.getNodeId().toString());
	}

	private void startTaskExecutorInContainer(Container container, WorkerResourceSpec workerResourceSpec, ResourceID resourceId) {
		workerNodeMap.put(resourceId, new YarnWorkerNode(container, resourceId));

		try {
			// Context information used to start a TaskExecutor Java process
			ContainerLaunchContext taskExecutorLaunchContext = createTaskExecutorLaunchContext(
				resourceId,
				container.getNodeId().getHost(),
				TaskExecutorProcessUtils.processSpecFromWorkerResourceSpec(flinkConfig, workerResourceSpec));

			nodeManagerClient.startContainerAsync(container, taskExecutorLaunchContext);
		} catch (Throwable t) {
			releaseFailedContainerAndRequestNewContainerIfRequired(container.getId(), t);
		}
	}

	private void releaseFailedContainerAndRequestNewContainerIfRequired(ContainerId containerId, Throwable throwable) {
		validateRunsInMainThread();

		log.error("Could not start TaskManager in container {}.", containerId, throwable);

		final ResourceID resourceId = new ResourceID(containerId.toString());
		// release the failed container
		workerNodeMap.remove(resourceId);
		resourceManagerClient.releaseAssignedContainer(containerId);
		notifyAllocatedWorkerStopped(resourceId);
		// and ask for a new one
		requestYarnContainerIfRequired();
	}

	private void returnExcessContainer(Container excessContainer) {
		log.info("Returning excess container {}.", excessContainer.getId());
		resourceManagerClient.releaseAssignedContainer(excessContainer.getId());
	}

	private void removeContainerRequest(AMRMClient.ContainerRequest pendingContainerRequest, WorkerResourceSpec workerResourceSpec) {
		log.info("Removing container request {}.", pendingContainerRequest);
		resourceManagerClient.removeContainerRequest(pendingContainerRequest);
	}

	private Collection<AMRMClient.ContainerRequest> getPendingRequestsAndCheckConsistency(Resource resource, int expectedNum) {
		final Collection<Resource> equivalentResources = workerSpecContainerResourceAdapter.getEquivalentContainerResource(resource, matchingStrategy);
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

	@Override
	public void onShutdownRequest() {
		onFatalError(new ResourceManagerException(ERROR_MASSAGE_ON_SHUTDOWN_REQUEST));
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
	//  NMClientAsync CallbackHandler methods
	// ------------------------------------------------------------------------
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
	public void onStartContainerError(ContainerId containerId, Throwable t) {
		runAsync(() -> releaseFailedContainerAndRequestNewContainerIfRequired(containerId, t));
	}

	@Override
	public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {
		// We are not interested in getting container status
	}

	@Override
	public void onStopContainerError(ContainerId containerId, Throwable throwable) {
		log.warn("Error while calling YARN Node Manager to stop container {}.", containerId, throwable);
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
	 * Request new container if pending containers cannot satisfy pending slot requests.
	 */
	private void requestYarnContainerIfRequired() {
		for (Map.Entry<WorkerResourceSpec, Integer> requiredWorkersPerResourceSpec : getRequiredResources().entrySet()) {
			final WorkerResourceSpec workerResourceSpec = requiredWorkersPerResourceSpec.getKey();
			while (requiredWorkersPerResourceSpec.getValue() > getNumRequestedNotRegisteredWorkersFor(workerResourceSpec)) {
				final boolean requestContainerSuccess = requestYarnContainer(workerResourceSpec);
				Preconditions.checkState(requestContainerSuccess,
					"Cannot request container for worker resource spec {}.", workerResourceSpec);
			}
		}
	}

	private boolean requestYarnContainer(WorkerResourceSpec workerResourceSpec) {
		Optional<Resource> containerResourceOptional = getContainerResource(workerResourceSpec);

		if (containerResourceOptional.isPresent()) {
			resourceManagerClient.addContainerRequest(getContainerRequest(containerResourceOptional.get()));

			// make sure we transmit the request fast and receive fast news of granted allocations
			resourceManagerClient.setHeartbeatInterval(containerRequestHeartbeatIntervalMillis);
			int numPendingWorkers = notifyNewWorkerRequested(workerResourceSpec).getNumNotAllocated();

			log.info("Requesting new TaskExecutor container with resource {}. Number pending workers of this resource is {}.",
				workerResourceSpec,
				numPendingWorkers);
			return true;
		} else {
			return false;
		}
	}

	@Nonnull
	@VisibleForTesting
	AMRMClient.ContainerRequest getContainerRequest(Resource containerResource) {
		return new AMRMClient.ContainerRequest(
			containerResource,
			null,
			null,
			RM_REQUEST_PRIORITY);
	}

	private ContainerLaunchContext createTaskExecutorLaunchContext(
		ResourceID containerId,
		String host,
		TaskExecutorProcessSpec taskExecutorProcessSpec) throws Exception {

		// init the ContainerLaunchContext
		final String currDir = env.get(ApplicationConstants.Environment.PWD.key());

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

		ContainerLaunchContext taskExecutorLaunchContext = Utils.createTaskExecutorContext(
			flinkConfig,
			yarnConfig,
			env,
			taskManagerParameters,
			taskManagerDynamicProperties,
			currDir,
			YarnTaskExecutorRunner.class,
			log);

		taskExecutorLaunchContext.getEnvironment()
				.put(ENV_FLINK_NODE_ID, host);
		return taskExecutorLaunchContext;
	}
}
