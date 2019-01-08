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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.messages.StopCluster;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.messages.ContainersAllocated;
import org.apache.flink.yarn.messages.ContainersComplete;

import akka.actor.ActorRef;
import akka.actor.Props;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * Specialized Flink Resource Manager implementation for YARN clusters. It is started as the
 * YARN ApplicationMaster and implements the YARN-specific logic for container requests and failure
 * monitoring.
 */
public class YarnFlinkResourceManager extends FlinkResourceManager<RegisteredYarnWorkerNode> {

	/** The heartbeat interval while the resource master is waiting for containers. */
	private static final int FAST_YARN_HEARTBEAT_INTERVAL_MS = 500;

	/** The default heartbeat interval during regular operation. */
	private static final int DEFAULT_YARN_HEARTBEAT_INTERVAL_MS = 5000;

	/** Environment variable name of the final container id used by the Flink ResourceManager.
	 * Container ID generation may vary across Hadoop versions. */
	static final String ENV_FLINK_CONTAINER_ID = "_FLINK_CONTAINER_ID";

	private static final Priority RM_REQUEST_PRIORITY = Priority.newInstance(0);

	/** The containers where a TaskManager is starting and we are waiting for it to register. */
	private final Map<ResourceID, YarnContainerInLaunch> containersInLaunch;

	/** Containers we have released, where we are waiting for an acknowledgement that
	 * they are released. */
	private final Map<ContainerId, Container> containersBeingReturned;

	/** The YARN / Hadoop configuration object. */
	private final YarnConfiguration yarnConfig;

	/** The TaskManager container parameters (like container memory size). */
	private final ContaineredTaskManagerParameters taskManagerParameters;

	/** Context information used to start a TaskManager Java process. */
	private final ContainerLaunchContext taskManagerLaunchContext;

	/** Host name for the container running this process. */
	private final String applicationMasterHostName;

	/** Web interface URL, may be null. */
	private final String webInterfaceURL;

	/** Default heartbeat interval between this actor and the YARN ResourceManager. */
	private final int yarnHeartbeatIntervalMillis;

	/** Number of failed TaskManager containers before stopping the application. -1 means infinite. */
	private final int maxFailedContainers;

	/** Callback handler for the asynchronous resourceManagerClient. */
	private YarnResourceManagerCallbackHandler resourceManagerCallbackHandler;

	/** Client to communicate with the Resource Manager (YARN's master). */
	private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient;

	/** Client to communicate with the Node manager and launch TaskManager processes. */
	private NMClient nodeManagerClient;

	/** The number of containers requested, but not yet granted. */
	private int numPendingContainerRequests;

	/** The number of failed containers since the master became active. */
	private int failedContainersSoFar;

	/** A reference to the reflector to look up previous session containers. */
	private RegisterApplicationMasterResponseReflector applicationMasterResponseReflector =
		new RegisterApplicationMasterResponseReflector(LOG);

	public YarnFlinkResourceManager(
		Configuration flinkConfig,
		YarnConfiguration yarnConfig,
		LeaderRetrievalService leaderRetrievalService,
		String applicationMasterHostName,
		String webInterfaceURL,
		ContaineredTaskManagerParameters taskManagerParameters,
		ContainerLaunchContext taskManagerLaunchContext,
		int yarnHeartbeatIntervalMillis,
		int maxFailedContainers,
		int numInitialTaskManagers) {

		this(
			flinkConfig,
			yarnConfig,
			leaderRetrievalService,
			applicationMasterHostName,
			webInterfaceURL,
			taskManagerParameters,
			taskManagerLaunchContext,
			yarnHeartbeatIntervalMillis,
			maxFailedContainers,
			numInitialTaskManagers,
			new YarnResourceManagerCallbackHandler());
	}

	public YarnFlinkResourceManager(
		Configuration flinkConfig,
		YarnConfiguration yarnConfig,
		LeaderRetrievalService leaderRetrievalService,
		String applicationMasterHostName,
		String webInterfaceURL,
		ContaineredTaskManagerParameters taskManagerParameters,
		ContainerLaunchContext taskManagerLaunchContext,
		int yarnHeartbeatIntervalMillis,
		int maxFailedContainers,
		int numInitialTaskManagers,
		YarnResourceManagerCallbackHandler callbackHandler) {

		this(
			flinkConfig,
			yarnConfig,
			leaderRetrievalService,
			applicationMasterHostName,
			webInterfaceURL,
			taskManagerParameters,
			taskManagerLaunchContext,
			yarnHeartbeatIntervalMillis,
			maxFailedContainers,
			numInitialTaskManagers,
			callbackHandler,
			AMRMClientAsync.createAMRMClientAsync(yarnHeartbeatIntervalMillis, callbackHandler),
			NMClient.createNMClient());
	}

	public YarnFlinkResourceManager(
		Configuration flinkConfig,
		YarnConfiguration yarnConfig,
		LeaderRetrievalService leaderRetrievalService,
		String applicationMasterHostName,
		String webInterfaceURL,
		ContaineredTaskManagerParameters taskManagerParameters,
		ContainerLaunchContext taskManagerLaunchContext,
		int yarnHeartbeatIntervalMillis,
		int maxFailedContainers,
		int numInitialTaskManagers,
		YarnResourceManagerCallbackHandler callbackHandler,
		AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient,
		NMClient nodeManagerClient) {

		super(numInitialTaskManagers, flinkConfig, leaderRetrievalService);

		this.yarnConfig = requireNonNull(yarnConfig);
		this.taskManagerParameters = requireNonNull(taskManagerParameters);
		this.taskManagerLaunchContext = requireNonNull(taskManagerLaunchContext);
		this.applicationMasterHostName = requireNonNull(applicationMasterHostName);
		this.webInterfaceURL = webInterfaceURL;
		this.yarnHeartbeatIntervalMillis = yarnHeartbeatIntervalMillis;
		this.maxFailedContainers = maxFailedContainers;

		this.resourceManagerCallbackHandler = Preconditions.checkNotNull(callbackHandler);
		this.resourceManagerClient = Preconditions.checkNotNull(resourceManagerClient);
		this.nodeManagerClient = Preconditions.checkNotNull(nodeManagerClient);

		this.containersInLaunch = new HashMap<>();
		this.containersBeingReturned = new HashMap<>();
	}

	// ------------------------------------------------------------------------
	//  Actor messages
	// ------------------------------------------------------------------------

	@Override
	protected void handleMessage(Object message) {

		// check for YARN specific actor messages first

		if (message instanceof ContainersAllocated) {
			containersAllocated(((ContainersAllocated) message).containers());

		} else if (message instanceof ContainersComplete) {
			containersComplete(((ContainersComplete) message).containers());

		} else {
			// message handled by the generic resource master code
			super.handleMessage(message);
		}
	}

	// ------------------------------------------------------------------------
	//  YARN specific behavior
	// ------------------------------------------------------------------------

	@Override
	protected void initialize() throws Exception {
		LOG.info("Initializing YARN resource master");

		resourceManagerCallbackHandler.initialize(self());

		resourceManagerClient.init(yarnConfig);
		resourceManagerClient.start();

		// create the client to communicate with the node managers
		nodeManagerClient.init(yarnConfig);
		nodeManagerClient.start();
		nodeManagerClient.cleanupRunningContainersOnStop(true);

		// register with Resource Manager
		LOG.info("Registering Application Master with tracking url {}", webInterfaceURL);

		scala.Option<Object> portOption = AkkaUtils.getAddress(getContext().system()).port();
		int actorSystemPort = portOption.isDefined() ? (int) portOption.get() : -1;

		RegisterApplicationMasterResponse response = resourceManagerClient.registerApplicationMaster(
			applicationMasterHostName, actorSystemPort, webInterfaceURL);

		// if this application master starts as part of an ApplicationMaster/JobManager recovery,
		// then some worker containers are most likely still alive and we can re-obtain them
		List<Container> containersFromPreviousAttempts =
			applicationMasterResponseReflector.getContainersFromPreviousAttempts(response);

		if (!containersFromPreviousAttempts.isEmpty()) {
			LOG.info("Retrieved {} TaskManagers from previous attempt", containersFromPreviousAttempts.size());

			final long now = System.currentTimeMillis();
			for (Container c : containersFromPreviousAttempts) {
				YarnContainerInLaunch containerInLaunch = new YarnContainerInLaunch(c, now);
				containersInLaunch.put(containerInLaunch.getResourceID(), containerInLaunch);
			}

			// adjust the progress indicator
			updateProgress();
		}
	}

	@Override
	protected void shutdownApplication(ApplicationStatus finalStatus, String optionalDiagnostics) {
		// first, de-register from YARN
		FinalApplicationStatus yarnStatus = getYarnStatus(finalStatus);
		LOG.info("Unregistering application from the YARN Resource Manager");
		try {
			resourceManagerClient.unregisterApplicationMaster(yarnStatus, optionalDiagnostics, "");
		} catch (Throwable t) {
			LOG.error("Could not unregister the application master.", t);
		}

		// now shut down all our components
		try {
			resourceManagerClient.stop();
		} catch (Throwable t) {
			LOG.error("Could not cleanly shut down the Asynchronous Resource Manager Client", t);
		}
		try {
			nodeManagerClient.stop();
		} catch (Throwable t) {
			LOG.error("Could not cleanly shut down the Node Manager Client", t);
		}

		// stop the actor after finishing processing the stop message
		getContext().system().stop(getSelf());
	}

	@Override
	protected void fatalError(String message, Throwable error) {
		// we do not unregister, but cause a hard fail of this process, to have it
		// restarted by YARN
		LOG.error("FATAL ERROR IN YARN APPLICATION MASTER: " + message, error);
		LOG.error("Shutting down process");

		// kill this process, this will make YARN restart the process
		System.exit(EXIT_CODE_FATAL_ERROR);
	}

	@Override
	protected void requestNewWorkers(int numWorkers) {
		final Resource capability = getContainerResource();

		for (int i = 0; i < numWorkers; i++) {
			numPendingContainerRequests++;
			LOG.info("Requesting new TaskManager container with {} megabytes memory. Pending requests: {}",
				capability.getMemory(), numPendingContainerRequests);

			resourceManagerClient.addContainerRequest(createContainerRequest(capability));
		}

		// make sure we transmit the request fast and receive fast news of granted allocations
		resourceManagerClient.setHeartbeatInterval(FAST_YARN_HEARTBEAT_INTERVAL_MS);
	}

	private Resource getContainerResource() {
		final long mem = taskManagerParameters.taskManagerTotalMemoryMB();
		final int containerMemorySizeMB;

		if (mem <= Integer.MAX_VALUE) {
			containerMemorySizeMB = (int) mem;
		} else {
			containerMemorySizeMB = Integer.MAX_VALUE;
			LOG.error("Decreasing container size from {} MB to {} MB (integer value overflow)",
				mem, containerMemorySizeMB);
		}

		// Resource requirements for worker containers
		int taskManagerSlots = taskManagerParameters.numSlots();
		int vcores = config.getInteger(YarnConfigOptions.VCORES, Math.max(taskManagerSlots, 1));
		return Resource.newInstance(containerMemorySizeMB, vcores);
	}

	@Nonnull
	private AMRMClient.ContainerRequest createContainerRequest(Resource capability) {
		return new AMRMClient.ContainerRequest(capability, null, null, RM_REQUEST_PRIORITY);
	}

	@Override
	protected void releasePendingWorker(ResourceID id) {
		YarnContainerInLaunch container = containersInLaunch.remove(id);
		if (container != null) {
			releaseYarnContainer(container.container());
		} else {
			LOG.error("Cannot find container {} to release. Ignoring request.", id);
		}
	}

	@Override
	protected void releaseStartedWorker(RegisteredYarnWorkerNode worker) {
		releaseYarnContainer(worker.yarnContainer());
	}

	private void releaseYarnContainer(Container container) {
		LOG.info("Releasing YARN container {}", container.getId());

		containersBeingReturned.put(container.getId(), container);

		// release the container on the node manager
		try {
			nodeManagerClient.stopContainer(container.getId(), container.getNodeId());
		} catch (Throwable t) {
			// we only log this error. since the ResourceManager also gets the release
			// notification, the container should be eventually cleaned up
			LOG.error("Error while calling YARN Node Manager to release container", t);
		}

		// tell the master that the container is no longer needed
		resourceManagerClient.releaseAssignedContainer(container.getId());
	}

	@Override
	protected RegisteredYarnWorkerNode workerStarted(ResourceID resourceID) {
		YarnContainerInLaunch inLaunch = containersInLaunch.remove(resourceID);
		if (inLaunch == null) {
			// Container was not in state "being launched", this can indicate that the TaskManager
			// in this container was already registered or that the container was not started
			// by this resource manager. Simply ignore this resourceID.
			return null;
		} else {
			return new RegisteredYarnWorkerNode(inLaunch.container());
		}
	}

	@Override
	protected Collection<RegisteredYarnWorkerNode> reacceptRegisteredWorkers(Collection<ResourceID> toConsolidate) {
		// we check for each task manager if we recognize its container
		List<RegisteredYarnWorkerNode> accepted = new ArrayList<>();
		for (ResourceID resourceID : toConsolidate) {
			YarnContainerInLaunch yci = containersInLaunch.remove(resourceID);

			if (yci != null) {
				LOG.info("YARN container consolidation recognizes Resource {} ", resourceID);

				accepted.add(new RegisteredYarnWorkerNode(yci.container()));
			}
			else {
				if (isStarted(resourceID)) {
					LOG.info("TaskManager {} has already been registered at the resource manager.", resourceID);
				} else {
					LOG.info("YARN container consolidation does not recognize TaskManager {}",
						resourceID);
				}
			}
		}
		return accepted;
	}

	@Override
	protected int getNumWorkerRequestsPending() {
		return numPendingContainerRequests;
	}

	@Override
	protected int getNumWorkersPendingRegistration() {
		return containersInLaunch.size();
	}

	// ------------------------------------------------------------------------
	//  Callbacks from the YARN Resource Manager
	// ------------------------------------------------------------------------

	private void containersAllocated(List<Container> containers) {
		final int numRequired = getDesignatedWorkerPoolSize();
		final int numRegistered = getNumberOfStartedTaskManagers();

		final Collection<AMRMClient.ContainerRequest> pendingRequests = getPendingRequests();
		final Iterator<AMRMClient.ContainerRequest> pendingRequestsIterator = pendingRequests.iterator();

		for (Container container : containers) {
			if (numPendingContainerRequests > 0) {
				numPendingContainerRequests -= 1;
				resourceManagerClient.removeContainerRequest(pendingRequestsIterator.next());
			}
			numPendingContainerRequests = Math.max(0, numPendingContainerRequests - 1);
			LOG.info("Received new container: {} - Remaining pending container requests: {}",
				container.getId(), numPendingContainerRequests);

			// decide whether to return the container, or whether to start a TaskManager
			if (numRegistered + containersInLaunch.size() < numRequired) {
				// start a TaskManager
				final YarnContainerInLaunch containerInLaunch = new YarnContainerInLaunch(container);
				final ResourceID resourceID = containerInLaunch.getResourceID();
				containersInLaunch.put(resourceID, containerInLaunch);

				String message = "Launching TaskManager in container " + containerInLaunch
					+ " on host " + container.getNodeId().getHost();
				LOG.info(message);
				sendInfoMessage(message);

				try {
					// set a special environment variable to uniquely identify this container
					taskManagerLaunchContext.getEnvironment()
						.put(ENV_FLINK_CONTAINER_ID, resourceID.getResourceIdString());
					nodeManagerClient.startContainer(container, taskManagerLaunchContext);
				}
				catch (Throwable t) {
					// failed to launch the container
					containersInLaunch.remove(resourceID);

					// return container, a new one will be requested eventually
					LOG.error("Could not start TaskManager in container " + containerInLaunch, t);
					containersBeingReturned.put(container.getId(), container);
					resourceManagerClient.releaseAssignedContainer(container.getId());
				}
			} else {
				// return excessive container
				LOG.info("Returning excess container {}", container.getId());
				containersBeingReturned.put(container.getId(), container);
				resourceManagerClient.releaseAssignedContainer(container.getId());
			}
		}

		updateProgress();

		// if we are waiting for no further containers, we can go to the
		// regular heartbeat interval
		if (numPendingContainerRequests <= 0) {
			resourceManagerClient.setHeartbeatInterval(yarnHeartbeatIntervalMillis);
		}

		// make sure we re-check the status of workers / containers one more time at least,
		// in case some containers did not come up properly
		triggerCheckWorkers();
	}

	private Collection<AMRMClient.ContainerRequest> getPendingRequests() {
		final List<? extends Collection<AMRMClient.ContainerRequest>> matchingRequests = resourceManagerClient.getMatchingRequests(RM_REQUEST_PRIORITY, ResourceRequest.ANY, getContainerResource());

		final Collection<AMRMClient.ContainerRequest> result;

		if (matchingRequests.isEmpty()) {
			result = Collections.emptyList();
		} else {
			result = new ArrayList<>(matchingRequests.get(0));
		}

		Preconditions.checkState(
			result.size() == numPendingContainerRequests,
			"The RMClient's and YarnResourceManagers internal state about the number of pending container requests has diverged. Number client's pending container requests %s != Number RM's pending container requests %s.", result.size(), numPendingContainerRequests);

		return result;
	}

	/**
	 * Invoked when the ResourceManager informs of completed containers.
	 * Called via an actor message by the callback from the ResourceManager client.
	 *
	 * @param containers The containers that have completed.
	 */
	private void containersComplete(List<ContainerStatus> containers) {
		// the list contains both failed containers, as well as containers that
		// were gracefully returned by this application master

		for (ContainerStatus status : containers) {
			final ResourceID id = new ResourceID(status.getContainerId().toString());

			// check if this is a failed container or a completed container
			if (containersBeingReturned.remove(status.getContainerId()) != null) {
				// regular completed container that we released
				LOG.info("Container {} completed successfully with diagnostics: {}",
					id, status.getDiagnostics());
			} else {
				// failed container, either at startup, or running
				final String exitStatus;
				switch (status.getExitStatus()) {
					case -103:
						exitStatus = "Vmem limit exceeded (-103)";
						break;
					case -104:
						exitStatus = "Pmem limit exceeded (-104)";
						break;
					default:
						exitStatus = String.valueOf(status.getExitStatus());
				}

				final YarnContainerInLaunch launched = containersInLaunch.remove(id);
				if (launched != null) {
					LOG.info("Container {} failed, with a TaskManager in launch or registration. " +
							"Exit status: {}", id, exitStatus);
					// we will trigger re-acquiring new containers at the end
				} else {
					// failed registered worker
					LOG.info("Container {} failed. Exit status: {}", id, exitStatus);

					// notify the generic logic, which notifies the JobManager, etc.
					notifyWorkerFailed(id, "Container " + id + " failed. " + "Exit status: {}" + exitStatus);
				}

				// general failure logging
				failedContainersSoFar++;

				String diagMessage = String.format("Diagnostics for container %s in state %s : " +
					"exitStatus=%s diagnostics=%s",
					id, status.getState(), exitStatus, status.getDiagnostics());
				sendInfoMessage(diagMessage);

				LOG.info(diagMessage);
				LOG.info("Total number of failed containers so far: " + failedContainersSoFar);

				// maxFailedContainers == -1 is infinite number of retries.
				if (maxFailedContainers >= 0 && failedContainersSoFar > maxFailedContainers) {
					String msg = "Stopping YARN session because the number of failed containers ("
						+ failedContainersSoFar + ") exceeded the maximum failed containers ("
						+ maxFailedContainers + "). This number is controlled by the '"
						+ YarnConfigOptions.MAX_FAILED_CONTAINERS.key() + "' configuration setting. "
						+ "By default its the number of requested containers.";

					LOG.error(msg);
					self().tell(decorateMessage(new StopCluster(ApplicationStatus.FAILED, msg)),
						ActorRef.noSender());

					// no need to do anything else
					return;
				}
			}
		}

		updateProgress();

		// in case failed containers were among the finished containers, make
		// sure we re-examine and request new ones
		triggerCheckWorkers();
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Extracts a unique ResourceID from the Yarn Container.
	 * @param container The Yarn container
	 * @return The ResourceID for the container
	 */
	static ResourceID extractResourceID(Container container) {
		return new ResourceID(container.getId().toString());
	}

	private void updateProgress() {
		final int required = getDesignatedWorkerPoolSize();
		final int available = getNumberOfStartedTaskManagers() + containersInLaunch.size();
		final float progress = (required <= 0) ? 1.0f : available / (float) required;

		if (resourceManagerCallbackHandler != null) {
			resourceManagerCallbackHandler.setCurrentProgress(progress);
		}
	}

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

	// ------------------------------------------------------------------------
	//  Actor props factory
	// ------------------------------------------------------------------------

	/**
	 * Creates the props needed to instantiate this actor.
	 *
	 * <p>Rather than extracting and validating parameters in the constructor, this factory method takes
	 * care of that. That way, errors occur synchronously, and are not swallowed simply in a
	 * failed asynchronous attempt to start the actor.
	 *
	 * @param actorClass
	 *             The actor class, to allow overriding this actor with subclasses for testing.
	 * @param flinkConfig
	 *             The Flink configuration object.
	 * @param yarnConfig
	 *             The YARN configuration object.
	 * @param applicationMasterHostName
	 *             The hostname where this application master actor runs.
	 * @param webFrontendURL
	 *             The URL of the tracking web frontend.
	 * @param taskManagerParameters
	 *             The parameters for launching TaskManager containers.
	 * @param taskManagerLaunchContext
	 *             The parameters for launching the TaskManager processes in the TaskManager containers.
	 * @param numInitialTaskManagers
	 *             The initial number of TaskManagers to allocate.
	 * @param log
	 *             The logger to log to.
	 *
	 * @return The Props object to instantiate the YarnFlinkResourceManager actor.
	 */
	public static Props createActorProps(Class<? extends YarnFlinkResourceManager> actorClass,
			Configuration flinkConfig,
			YarnConfiguration yarnConfig,
			LeaderRetrievalService leaderRetrievalService,
			String applicationMasterHostName,
			String webFrontendURL,
			ContaineredTaskManagerParameters taskManagerParameters,
			ContainerLaunchContext taskManagerLaunchContext,
			int numInitialTaskManagers,
			Logger log) {

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

		final int maxFailedContainers = flinkConfig.getInteger(
			YarnConfigOptions.MAX_FAILED_CONTAINERS.key(), numInitialTaskManagers);
		if (maxFailedContainers >= 0) {
			log.info("YARN application tolerates {} failed TaskManager containers before giving up",
				maxFailedContainers);
		}

		return Props.create(actorClass,
			flinkConfig,
			yarnConfig,
			leaderRetrievalService,
			applicationMasterHostName,
			webFrontendURL,
			taskManagerParameters,
			taskManagerLaunchContext,
			yarnHeartbeatIntervalMS,
			maxFailedContainers,
			numInitialTaskManagers);
	}
}
