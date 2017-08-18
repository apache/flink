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
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.ContaineredTaskManagerParameters;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
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
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.yarn.api.ApplicationConstants;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import scala.concurrent.duration.FiniteDuration;

/**
 * The yarn implementation of the resource manager. Used when the system is started
 * via the resource framework YARN.
 */
public class YarnResourceManager extends ResourceManager<ResourceID> implements AMRMClientAsync.CallbackHandler {

	/** The process environment variables. */
	private final Map<String, String> env;

	/** The default registration timeout for task executor in seconds. */
	private static final int DEFAULT_TASK_MANAGER_REGISTRATION_DURATION = 300;

	/** The heartbeat interval while the resource master is waiting for containers. */
	private static final int FAST_YARN_HEARTBEAT_INTERVAL_MS = 500;

	/** The default heartbeat interval during regular operation. */
	private static final int DEFAULT_YARN_HEARTBEAT_INTERVAL_MS = 5000;

	/** The default memory of task executor to allocate (in MB). */
	private static final int DEFAULT_TSK_EXECUTOR_MEMORY_SIZE = 1024;

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
			fatalErrorHandler);
		this.flinkConfig  = flinkConfig;
		this.yarnConfig = new YarnConfiguration();
		this.env = env;
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
	}

	@Override
	protected void initialize() throws ResourceManagerException {
		resourceManagerClient = AMRMClientAsync.createAMRMClientAsync(yarnHeartbeatIntervalMillis, this);
		resourceManagerClient.init(yarnConfig);
		resourceManagerClient.start();
		try {
			//TODO: change akka address to tcp host and port, the getAddress() interface should return a standard tcp address
			Tuple2<String, Integer> hostPort = parseHostPort(getAddress());
			//TODO: the third paramter should be the webmonitor address
			resourceManagerClient.registerApplicationMaster(hostPort.f0, hostPort.f1, getAddress());
		} catch (Exception e) {
			log.info("registerApplicationMaster fail", e);
		}

		// create the client to communicate with the node managers
		nodeManagerClient = NMClient.createNMClient();
		nodeManagerClient.init(yarnConfig);
		nodeManagerClient.start();
		nodeManagerClient.cleanupRunningContainersOnStop(true);
	}

	@Override
	public void postStop() throws Exception {
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

		try {
			super.postStop();
		} catch (Throwable t) {
			firstException = ExceptionUtils.firstOrSuppressed(t, firstException);
		}

		if (firstException != null) {
			ExceptionUtils.rethrowException(firstException, "Error while shutting down YARN resource manager");
		}
	}

	@Override
	protected void shutDownApplication(ApplicationStatus finalStatus, String optionalDiagnostics) {

		// first, de-register from YARN
		FinalApplicationStatus yarnStatus = getYarnStatus(finalStatus);
		log.info("Unregistering application from the YARN Resource Manager");
		try {
			resourceManagerClient.unregisterApplicationMaster(yarnStatus, optionalDiagnostics, "");
		} catch (Throwable t) {
			log.error("Could not unregister the application master.", t);
		}
	}

	@Override
	public void startNewWorker(ResourceProfile resourceProfile) {
		// Priority for worker containers - priorities are intra-application
		//TODO: set priority according to the resource allocated
		Priority priority = Priority.newInstance(generatePriority(resourceProfile));
		int mem = resourceProfile.getMemoryInMB() < 0 ? DEFAULT_TSK_EXECUTOR_MEMORY_SIZE : (int) resourceProfile.getMemoryInMB();
		int vcore = resourceProfile.getCpuCores() < 1 ? 1 : (int) resourceProfile.getCpuCores();
		Resource capability = Resource.newInstance(mem, vcore);
		requestYarnContainer(capability, priority);
	}

	@Override
	public void stopWorker(ResourceID resourceID) {
		// TODO: Implement to stop the worker
	}

	@Override
	protected ResourceID workerStarted(ResourceID resourceID) {
		return resourceID;
	}

	// AMRMClientAsync CallbackHandler methods
	@Override
	public float getProgress() {
		// Temporarily need not record the total size of asked and allocated containers
		return 1;
	}

	@Override
	public void onContainersCompleted(List<ContainerStatus> list) {
		for (ContainerStatus container : list) {
			if (container.getExitStatus() < 0) {
				closeTaskManagerConnection(new ResourceID(
					container.getContainerId().toString()), new Exception(container.getDiagnostics()));
			}
		}
	}

	@Override
	public void onContainersAllocated(List<Container> containers) {
		for (Container container : containers) {
			numPendingContainerRequests = Math.max(0, numPendingContainerRequests - 1);
			log.info("Received new container: {} - Remaining pending container requests: {}",
					container.getId(), numPendingContainerRequests);
			try {
				/** Context information used to start a TaskExecutor Java process */
				ContainerLaunchContext taskExecutorLaunchContext =
						createTaskExecutorLaunchContext(container.getResource(), container.getId().toString(), container.getNodeId().getHost());
				nodeManagerClient.startContainer(container, taskExecutorLaunchContext);
			}
			catch (Throwable t) {
				// failed to launch the container, will release the failed one and ask for a new one
				log.error("Could not start TaskManager in container {},", container, t);
				resourceManagerClient.releaseAssignedContainer(container.getId());
				requestYarnContainer(container.getResource(), container.getPriority());
			}
		}
		if (numPendingContainerRequests <= 0) {
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
		onFatalErrorAsync(error);
	}

	//Utility methods
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
		return new Tuple2(host, Integer.valueOf(port));
	}

	private void requestYarnContainer(Resource resource, Priority priority) {
		resourceManagerClient.addContainerRequest(
				new AMRMClient.ContainerRequest(resource, null, null, priority));
		// make sure we transmit the request fast and receive fast news of granted allocations
		resourceManagerClient.setHeartbeatInterval(FAST_YARN_HEARTBEAT_INTERVAL_MS);

		numPendingContainerRequests++;
		log.info("Requesting new TaskManager container pending requests: {}",
				numPendingContainerRequests);
	}

	private ContainerLaunchContext createTaskExecutorLaunchContext(Resource resource, String containerId, String host)
			throws Exception {
		// init the ContainerLaunchContext
		final String currDir = env.get(ApplicationConstants.Environment.PWD.key());

		final ContaineredTaskManagerParameters taskManagerParameters =
				ContaineredTaskManagerParameters.create(flinkConfig, resource.getMemory(), 1);

		log.info("TaskExecutor{} will be started with container size {} MB, JVM heap size {} MB, " +
				"JVM direct memory limit {} MB",
				containerId,
				taskManagerParameters.taskManagerTotalMemoryMB(),
				taskManagerParameters.taskManagerHeapSizeMB(),
				taskManagerParameters.taskManagerDirectMemoryLimitMB());
		int timeout = flinkConfig.getInteger(ConfigConstants.TASK_MANAGER_MAX_REGISTRATION_DURATION,
				DEFAULT_TASK_MANAGER_REGISTRATION_DURATION);
		FiniteDuration teRegistrationTimeout = new FiniteDuration(timeout, TimeUnit.SECONDS);
		final Configuration taskManagerConfig = BootstrapTools.generateTaskManagerConfiguration(
				flinkConfig, "", 0, 1, teRegistrationTimeout);
		log.debug("TaskManager configuration: {}", taskManagerConfig);

		ContainerLaunchContext taskExecutorLaunchContext = Utils.createTaskExecutorContext(
				flinkConfig, yarnConfig, env,
				taskManagerParameters, taskManagerConfig,
				currDir, YarnTaskExecutorRunner.class, log);

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
