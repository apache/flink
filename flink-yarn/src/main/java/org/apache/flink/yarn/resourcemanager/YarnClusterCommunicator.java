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

package org.apache.flink.yarn.resourcemanager;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.yarn.YarnContainerInLaunch;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class YarnClusterCommunicator {

	/** The heartbeat interval while the resource master is waiting for containers */
	private static final int FAST_YARN_HEARTBEAT_INTERVAL_MS = 500;

	/** The default heartbeat interval during regular operation */
	private static final int DEFAULT_YARN_HEARTBEAT_INTERVAL_MS = 5000;

	protected final Logger LOG = LoggerFactory.getLogger(getClass());

	/** Environment variable name of the final container id used by the Flink ResourceManager.
	 * Container ID generation may vary across Hadoop versions. */
	final static String ENV_FLINK_CONTAINER_ID = "_FLINK_CONTAINER_ID";

	/** The containers where a TaskManager is starting and we are waiting for it to register */
	private final Map<ResourceID, YarnContainerInLaunch> containersInLaunch;

	/** Containers we have released, where we are waiting for an acknowledgement that
	 * they are released */
	private final Map<ContainerId, Container> containersBeingReturned;

	/** The YARN / Hadoop configuration object */
	private final YarnConfiguration yarnConfig;

	/** The TaskManager container parameters (like container memory size) */
	private final ContaineredTaskManagerParameters taskManagerParameters;

	/** Context information used to start a TaskManager Java process */
	private final ContainerLaunchContext taskManagerLaunchContext;

	/** Host name for the container running this process */
	private final String applicationMasterHostName;

	/** Rpc port for the container running this process **/
	private final int applicationMasterRpcPort;

	/** Web interface URL, may be null */
	private final String webInterfaceURL;

	/** Default heartbeat interval between this actor and the YARN ResourceManager */
	private final int yarnHeartbeatIntervalMillis;

	/** Number of failed TaskManager containers before stopping the application. -1 means infinite. */
	private final int maxFailedContainers;

	/** Callback handler for the asynchronous resourceManagerClient */
	private CallbackHandler resourceManagerCallbackHandler;

	/** Client to communicate with the Resource Manager (YARN's master) */
	private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient;

	/** Client to communicate with the Node manager and launch TaskManager processes */
	private NMClient nodeManagerClient;

	/** The number of containers requested, but not yet granted */
	private int numPendingContainerRequests;

	/** The number of failed containers since the master became active */
	private int failedContainersSoFar;

	/** A reference to the reflector to look up previous session containers. */
	private RegisterApplicationMasterResponseReflector applicationMasterResponseReflector =
		new RegisterApplicationMasterResponseReflector(LOG);

	private ResourceManager resourceManager;


	public YarnClusterCommunicator(
		YarnConfiguration yarnConfig,
		String applicationMasterHostName,
		String webInterfaceURL,
		int applicationMasterRpcPort,
		ContaineredTaskManagerParameters taskManagerParameters,
		ContainerLaunchContext taskManagerLaunchContext,
		int yarnHeartbeatIntervalMillis,
		int maxFailedContainers) {

		this(
			yarnConfig,
			applicationMasterHostName,
			webInterfaceURL,
			applicationMasterRpcPort,
			taskManagerParameters,
			taskManagerLaunchContext,
			yarnHeartbeatIntervalMillis,
			maxFailedContainers,
			new CallbackHandler());
	}

	public YarnClusterCommunicator(
		YarnConfiguration yarnConfig,
		String applicationMasterHostName,
		String webInterfaceURL,
		int applicationMasterRpcPort,
		ContaineredTaskManagerParameters taskManagerParameters,
		ContainerLaunchContext taskManagerLaunchContext,
		int yarnHeartbeatIntervalMillis,
		int maxFailedContainers,
		CallbackHandler callbackHandler) {

		this(
			yarnConfig,
			applicationMasterHostName,
			webInterfaceURL,
			applicationMasterRpcPort,
			taskManagerParameters,
			taskManagerLaunchContext,
			yarnHeartbeatIntervalMillis,
			maxFailedContainers,
			callbackHandler,
			AMRMClientAsync.createAMRMClientAsync(yarnHeartbeatIntervalMillis, callbackHandler),
			NMClient.createNMClient());
	}

	public YarnClusterCommunicator(
		YarnConfiguration yarnConfig,
		String applicationMasterHostName,
		String webInterfaceURL,
		int applicationMasterRpcPort,
		ContaineredTaskManagerParameters taskManagerParameters,
		ContainerLaunchContext taskManagerLaunchContext,
		int yarnHeartbeatIntervalMillis,
		int maxFailedContainers,
		CallbackHandler callbackHandler,
		AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient,
		NMClient nodeManagerClient) {
		checkArgument(applicationMasterRpcPort > 0, "application master rpc port must be greater than zero");

		this.yarnConfig = requireNonNull(yarnConfig);
		this.taskManagerParameters = requireNonNull(taskManagerParameters);
		this.taskManagerLaunchContext = requireNonNull(taskManagerLaunchContext);
		this.applicationMasterHostName = requireNonNull(applicationMasterHostName);
		this.applicationMasterRpcPort = applicationMasterRpcPort;
		this.webInterfaceURL = webInterfaceURL;
		this.yarnHeartbeatIntervalMillis = yarnHeartbeatIntervalMillis;
		this.maxFailedContainers = maxFailedContainers;

		this.resourceManagerCallbackHandler = checkNotNull(callbackHandler);
		this.resourceManagerClient = checkNotNull(resourceManagerClient);
		this.nodeManagerClient = checkNotNull(nodeManagerClient);

		this.containersInLaunch = new HashMap<>();
		this.containersBeingReturned = new HashMap<>();
	}

	/**
	 * binding resourceManager
	 * @param resourceManager
	 */
	public void bindResourceManager(ResourceManager resourceManager) {
		checkState(this.resourceManager == null, "Already bind resourceManager");
		this.resourceManager = checkNotNull(resourceManager);
	}

	// ------------------------------------------------------------------------
	//  YARN specific behavior
	// ------------------------------------------------------------------------

	public void initialize() throws Exception {
		LOG.info("Initializing Yarn cluster communicator");

		resourceManagerCallbackHandler.initialize(this);
		resourceManagerClient.init(yarnConfig);
		resourceManagerClient.start();

		// create the client to communicate with the node managers
		nodeManagerClient.init(yarnConfig);
		nodeManagerClient.start();
		nodeManagerClient.cleanupRunningContainersOnStop(true);

		// register with Resource Manager
		LOG.info("Registering Application Master with tracking url {}", webInterfaceURL);

		RegisterApplicationMasterResponse response = resourceManagerClient.registerApplicationMaster(
			applicationMasterHostName, applicationMasterRpcPort, webInterfaceURL);

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

	public void requestNewContainer(ResourceProfile resourceProfile) {
		numPendingContainerRequests++;

		// Priority for worker containers - priorities are intra-application
		Priority priority = Priority.newInstance(0);

		Resource capability = Resource.newInstance((int)resourceProfile.getMemoryInMB(), (int)Math.max(resourceProfile.getCpuCores(), 1));

		resourceManagerClient.addContainerRequest(
				new AMRMClient.ContainerRequest(capability, null, null, priority));

		// make sure we transmit the request fast and receive fast news of granted allocations
		resourceManagerClient.setHeartbeatInterval(FAST_YARN_HEARTBEAT_INTERVAL_MS);
	}

	private void containersAllocated(List<Container> containers) {
		for (Container container : containers) {
			// decide whether to return the container, or whether to start a TaskManager
			if (numPendingContainerRequests > 0) {
				numPendingContainerRequests = Math.max(0, numPendingContainerRequests - 1);
				// start a TaskManager
				final YarnContainerInLaunch containerInLaunch = new YarnContainerInLaunch(container);
				final ResourceID resourceID = containerInLaunch.getResourceID();
				containersInLaunch.put(resourceID, containerInLaunch);

				String message = "Launching TaskManager in container " + containerInLaunch
					+ " on host " + container.getNodeId().getHost();
				LOG.info(message);
				resourceManager.sendInfoMessage(message);

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
	}

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
					resourceManager.notifyWorkerFailed(id, "Container " + id + " failed. " + "Exit status: {}" + exitStatus);
				}

				// general failure logging
				failedContainersSoFar++;

				String diagMessage = String.format("Diagnostics for container %s in state %s : " +
						"exitStatus=%s diagnostics=%s",
					id, status.getState(), exitStatus, status.getDiagnostics());
				resourceManager.sendInfoMessage(diagMessage);

				LOG.info(diagMessage);
				LOG.info("Total number of failed containers so far: " + failedContainersSoFar);

				// maxFailedContainers == -1 is infinite number of retries.
				if (maxFailedContainers >= 0 && failedContainersSoFar > maxFailedContainers) {
					String msg = "Stopping YARN session because the number of failed containers ("
						+ failedContainersSoFar + ") exceeded the maximum failed containers ("
						+ maxFailedContainers + "). This number is controlled by the '"
						+ ConfigConstants.YARN_MAX_FAILED_CONTAINERS + "' configuration setting. "
						+ "By default its the number of requested containers.";

					LOG.error(msg);
					resourceManager.shutDownCluster(ApplicationStatus.FAILED, msg);

					// no need to do anything else
					return;
				}
			}
		}

		updateProgress();
	}


	private void fatalError(String message, Throwable error) {
		resourceManager.onFatalError(message, error);
	}

	private void updateProgress() {
		final int available = resourceManager.getNumberOfStartedTaskManagers() + containersInLaunch.size();
		final float progress = (numPendingContainerRequests <= 0) ? 1.0f : available / (float) (numPendingContainerRequests + available);

		if (resourceManagerCallbackHandler != null) {
			resourceManagerCallbackHandler.setCurrentProgress(progress);
		}
	}

		/**
		 * Looks up the getContainersFromPreviousAttempts method on RegisterApplicationMasterResponse
		 * once and saves the method. This saves computation time on the sequent calls.
		 */
	private static class RegisterApplicationMasterResponseReflector {

		private Logger logger;
		private Method method;

		public RegisterApplicationMasterResponseReflector(Logger LOG) {
			this.logger = LOG;

			try {
				method = RegisterApplicationMasterResponse.class
					.getMethod("getContainersFromPreviousAttempts");

			} catch (NoSuchMethodException e) {
				// that happens in earlier Hadoop versions
				logger.info("Cannot reconnect to previously allocated containers. " +
					"This YARN version does not support 'getContainersFromPreviousAttempts()'");
			}
		}

		/**
		 * Checks if a YARN application still has registered containers. If the application master
		 * registered at the ResourceManager for the first time, this list will be empty. If the
		 * application master registered a repeated time (after a failure and recovery), this list
		 * will contain the containers that were previously allocated.
		 *
		 * @param response The response object from the registration at the ResourceManager.
		 * @return A list with containers from previous application attempt.
		 */
		private List<Container> getContainersFromPreviousAttempts(RegisterApplicationMasterResponse response) {
			if (method != null && response != null) {
				try {
					@SuppressWarnings("unchecked")
					List<Container> list = (List<Container>) method.invoke(response);
					if (list != null && !list.isEmpty()) {
						return list;
					}
				} catch (Throwable t) {
					logger.error("Error invoking 'getContainersFromPreviousAttempts()'", t);
				}
			}

			return Collections.emptyList();
		}

	}

	private static class CallbackHandler implements AMRMClientAsync.CallbackHandler {

		private YarnClusterCommunicator yarnClusterCommunicator;
		/** The progress we report */
		private float currentProgress;

		private CallbackHandler() {
		}

		private void initialize(YarnClusterCommunicator yarnClusterCommunicator) {
			this.yarnClusterCommunicator = yarnClusterCommunicator;
		}

		/**
		 * Sets the current progress.
		 * @param progress The current progress fraction.
		 */
		public void setCurrentProgress(float progress) {
			progress = Math.max(progress, 0.0f);
			progress = Math.min(progress, 1.0f);
			this.currentProgress = progress;
		}

		@Override
		public float getProgress() {
			return currentProgress;
		}

		@Override
		public void onContainersCompleted(List<ContainerStatus> list) {
			yarnClusterCommunicator.containersComplete(list);
		}

		@Override
		public void onContainersAllocated(List<Container> containers) {
			yarnClusterCommunicator.containersAllocated(containers);
		}

		@Override
		public void onShutdownRequest() {
			// We are getting killed anyway
		}

		@Override
		public void onNodesUpdated(List<NodeReport> list) {
			// We are not interested in node updates
		}

		@Override
		public void onError(Throwable error) {
			yarnClusterCommunicator.fatalError("Connection to YARN Resource Manager failed", error);
		}
	}
}
