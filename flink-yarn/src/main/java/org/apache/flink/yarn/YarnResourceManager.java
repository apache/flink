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
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;
import org.apache.flink.util.ExceptionUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.yarn.YarnConfigKeys.ENV_FLINK_CLASSPATH;

/**
 * The yarn implementation of the resource manager. Used when the system is started
 * via the resource framework YARN.
 */
public class YarnResourceManager extends ResourceManager<ResourceID> implements AMRMClientAsync.CallbackHandler {
	protected final Logger LOG = LoggerFactory.getLogger(getClass());

	/** The process environment variables */
	private final Map<String, String> ENV;

	/** The default registration timeout for task executor in seconds. */
	private final static int DEFAULT_TASK_MANAGER_REGISTRATION_DURATION = 300;

	/** The heartbeat interval while the resource master is waiting for containers */
	private static final int FAST_YARN_HEARTBEAT_INTERVAL_MS = 500;

	/** The default heartbeat interval during regular operation */
	private static final int DEFAULT_YARN_HEARTBEAT_INTERVAL_MS = 5000;

	/** The default memory of task executor to allocate (in MB) */
	private static final int DEFAULT_TSK_EXECUTOR_MEMORY_SIZE = 1024;

	/** Environment variable name of the final container id used by the YarnResourceManager.
	 * Container ID generation may vary across Hadoop versions. */
	final static String ENV_FLINK_CONTAINER_ID = "_FLINK_CONTAINER_ID";
	
	/** Environment variable name of the hostname given by the YARN.
	 * In task executor we use the hostnames given by YARN consistently throughout akka */
	final static String ENV_FLINK_NODE_ID = "_FLINK_NODE_ID";

	/** Default heartbeat interval between this resource manager and the YARN ResourceManager */
	private final int yarnHeartbeatIntervalMillis;

	private final Configuration flinkConfig;

	private final YarnConfiguration yarnConfig;

	/** Client to communicate with the Resource Manager (YARN's master) */
	private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManagerClient;

	/** Client to communicate with the Node manager and launch TaskExecutor processes */
	private NMClient nodeManagerClient;

	/** The number of containers requested, but not yet granted */
	private int numPendingContainerRequests;

	final private Map<ResourceProfile, Integer> resourcePriorities = new HashMap<>();

	public YarnResourceManager(
			Configuration flinkConfig,
			Map<String, String> env,
			RpcService rpcService,
			ResourceManagerConfiguration resourceManagerConfiguration,
			HighAvailabilityServices highAvailabilityServices,
			SlotManagerFactory slotManagerFactory,
			MetricRegistry metricRegistry,
			JobLeaderIdService jobLeaderIdService,
			FatalErrorHandler fatalErrorHandler) {
		super(
			rpcService,
			resourceManagerConfiguration,
			highAvailabilityServices,
			slotManagerFactory,
			metricRegistry,
			jobLeaderIdService,
			fatalErrorHandler);
		this.flinkConfig  = flinkConfig;
		this.yarnConfig = new YarnConfiguration();
		this.ENV = env;
		final int yarnHeartbeatIntervalMS = flinkConfig.getInteger(
				ConfigConstants.YARN_HEARTBEAT_DELAY_SECONDS, DEFAULT_YARN_HEARTBEAT_INTERVAL_MS / 1000) * 1000;

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
			LOG.info("registerApplicationMaster fail", e);
		}

		// create the client to communicate with the node managers
		nodeManagerClient = NMClient.createNMClient();
		nodeManagerClient.init(yarnConfig);
		nodeManagerClient.start();
		nodeManagerClient.cleanupRunningContainersOnStop(true);
	}

	@Override
	public void shutDown() throws Exception {
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
				if (firstException == null) {
					firstException = t;
				} else {
					firstException.addSuppressed(t);
				}
			}
		}
		if (firstException != null) {
			ExceptionUtils.rethrowException(firstException, "Error while shutting down YARN resource manager");
		}
		super.shutDown();
	}

	@Override
	protected void shutDownApplication(ApplicationStatus finalStatus, String optionalDiagnostics) {

		// first, de-register from YARN
		FinalApplicationStatus yarnStatus = getYarnStatus(finalStatus);
		LOG.info("Unregistering application from the YARN Resource Manager");
		try {
			resourceManagerClient.unregisterApplicationMaster(yarnStatus, optionalDiagnostics, "");
		} catch (Throwable t) {
			LOG.error("Could not unregister the application master.", t);
		}
	}

	@Override
	public void startNewWorker(ResourceProfile resourceProfile) {
		// Priority for worker containers - priorities are intra-application
		//TODO: set priority according to the resource allocated
		Priority priority = Priority.newInstance(generatePriority(resourceProfile));
		int mem = resourceProfile.getMemoryInMB() < 0 ? DEFAULT_TSK_EXECUTOR_MEMORY_SIZE : (int)resourceProfile.getMemoryInMB();
		int vcore = resourceProfile.getCpuCores() < 1 ? 1 : (int)resourceProfile.getCpuCores();
		Resource capability = Resource.newInstance(mem, vcore);
		requestYarnContainer(capability, priority);
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
				notifyWorkerFailed(new ResourceID(container.getContainerId().toString()), container.getDiagnostics());
			}
		}
	}

	@Override
	public void onContainersAllocated(List<Container> containers) {
		for (Container container : containers) {
			numPendingContainerRequests = Math.max(0, numPendingContainerRequests - 1);
			LOG.info("Received new container: {} - Remaining pending container requests: {}",
					container.getId(), numPendingContainerRequests);
			try {
				/** Context information used to start a TaskExecutor Java process */
				ContainerLaunchContext taskExecutorLaunchContext =
						createTaskExecutorLaunchContext(container.getResource(), container.getId().toString(), container.getNodeId().getHost());
				nodeManagerClient.startContainer(container, taskExecutorLaunchContext);
			}
			catch (Throwable t) {
				// failed to launch the container, will release the failed one and ask for a new one
				LOG.error("Could not start TaskManager in container {},", container, t);
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
			LOG.warn("Fail to shutdown the YARN resource manager.", e);
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
		LOG.info("Requesting new TaskManager container pending requests: {}",
				numPendingContainerRequests);
	}

	private ContainerLaunchContext createTaskExecutorLaunchContext(Resource resource, String containerId, String host)
			throws Exception {
		// init the ContainerLaunchContext
		final String currDir = ENV.get(ApplicationConstants.Environment.PWD.key());

		final ContaineredTaskManagerParameters taskManagerParameters =
				ContaineredTaskManagerParameters.create(flinkConfig, resource.getMemory(), 1);

		LOG.info("TaskExecutor{} will be started with container size {} MB, JVM heap size {} MB, " +
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
		LOG.debug("TaskManager configuration: {}", taskManagerConfig);

		ContainerLaunchContext taskExecutorLaunchContext = createTaskExecutorContext(
				flinkConfig, yarnConfig, ENV,
				taskManagerParameters, taskManagerConfig,
				currDir, YarnTaskExecutorRunner.class, LOG);

		// set a special environment variable to uniquely identify this container
		taskExecutorLaunchContext.getEnvironment()
				.put(ENV_FLINK_CONTAINER_ID, containerId);
		taskExecutorLaunchContext.getEnvironment()
				.put(ENV_FLINK_NODE_ID, host);
		return taskExecutorLaunchContext;
	}


	/**
	 * Creates the launch context, which describes how to bring up a TaskExecutor process in
	 * an allocated YARN container.
	 *
	 * <p>This code is extremely YARN specific and registers all the resources that the TaskExecutor
	 * needs (such as JAR file, config file, ...) and all environment variables in a YARN
	 * container launch context. The launch context then ensures that those resources will be
	 * copied into the containers transient working directory.
	 *
	 * @param flinkConfig
	 *		 The Flink configuration object.
	 * @param yarnConfig
	 *		 The YARN configuration object.
	 * @param env
	 *		 The environment variables.
	 * @param tmParams
	 *		 The TaskExecutor container memory parameters.
	 * @param taskManagerConfig
	 *		 The configuration for the TaskExecutors.
	 * @param workingDirectory
	 *		 The current application master container's working directory.
	 * @param taskManagerMainClass
	 *		 The class with the main method.
	 * @param log
	 *		 The logger.
	 *
	 * @return The launch context for the TaskManager processes.
	 *
	 * @throws Exception Thrown if teh launch context could not be created, for example if
	 *				   the resources could not be copied.
	 */
	private static ContainerLaunchContext createTaskExecutorContext(
			Configuration flinkConfig,
			YarnConfiguration yarnConfig,
			Map<String, String> env,
			ContaineredTaskManagerParameters tmParams,
			Configuration taskManagerConfig,
			String workingDirectory,
			Class<?> taskManagerMainClass,
			Logger log) throws Exception {

		// get and validate all relevant variables

		String remoteFlinkJarPath = env.get(YarnConfigKeys.FLINK_JAR_PATH);
		
		String appId = env.get(YarnConfigKeys.ENV_APP_ID);

		String clientHomeDir = env.get(YarnConfigKeys.ENV_CLIENT_HOME_DIR);

		String shipListString = env.get(YarnConfigKeys.ENV_CLIENT_SHIP_FILES);

		String yarnClientUsername = env.get(YarnConfigKeys.ENV_HADOOP_USER_NAME);

		final String remoteKeytabPath = env.get(YarnConfigKeys.KEYTAB_PATH);
		log.info("TM:remote keytab path obtained {}", remoteKeytabPath);

		final String remoteKeytabPrincipal = env.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
		log.info("TM:remote keytab principal obtained {}", remoteKeytabPrincipal);

		final String remoteYarnConfPath = env.get(YarnConfigKeys.ENV_YARN_SITE_XML_PATH);
		log.info("TM:remote yarn conf path obtained {}", remoteYarnConfPath);

		final String remoteKrb5Path = env.get(YarnConfigKeys.ENV_KRB5_PATH);
		log.info("TM:remote krb5 path obtained {}", remoteKrb5Path);

		String classPathString = env.get(YarnConfigKeys.ENV_FLINK_CLASSPATH);

		// obtain a handle to the file system used by YARN
		final org.apache.hadoop.fs.FileSystem yarnFileSystem;
		try {
			yarnFileSystem = org.apache.hadoop.fs.FileSystem.get(yarnConfig);
		} catch (IOException e) {
			throw new Exception("Could not access YARN's default file system", e);
		}

		//register keytab
		LocalResource keytabResource = null;
		if(remoteKeytabPath != null) {
			log.info("Adding keytab {} to the AM container local resource bucket", remoteKeytabPath);
			keytabResource = Records.newRecord(LocalResource.class);
			Path keytabPath = new Path(remoteKeytabPath);
			Utils.registerLocalResource(yarnFileSystem, keytabPath, keytabResource);
		}

		//To support Yarn Secure Integration Test Scenario
		LocalResource yarnConfResource = null;
		LocalResource krb5ConfResource = null;
		boolean hasKrb5 = false;
		if(remoteYarnConfPath != null && remoteKrb5Path != null) {
			log.info("TM:Adding remoteYarnConfPath {} to the container local resource bucket", remoteYarnConfPath);
			yarnConfResource = Records.newRecord(LocalResource.class);
			Path yarnConfPath = new Path(remoteYarnConfPath);
			Utils.registerLocalResource(yarnFileSystem, yarnConfPath, yarnConfResource);

			log.info("TM:Adding remoteKrb5Path {} to the container local resource bucket", remoteKrb5Path);
			krb5ConfResource = Records.newRecord(LocalResource.class);
			Path krb5ConfPath = new Path(remoteKrb5Path);
			Utils.registerLocalResource(yarnFileSystem, krb5ConfPath, krb5ConfResource);

			hasKrb5 = true;
		}

		// register Flink Jar with remote HDFS
		LocalResource flinkJar = Records.newRecord(LocalResource.class);
		{
			Path remoteJarPath = new Path(remoteFlinkJarPath);
			Utils.registerLocalResource(yarnFileSystem, remoteJarPath, flinkJar);
		}

		// register conf with local fs
		LocalResource flinkConf = Records.newRecord(LocalResource.class);
		{
			// write the TaskManager configuration to a local file
			final File taskManagerConfigFile =
					new File(workingDirectory, UUID.randomUUID() + "-taskmanager-conf.yaml");
			log.debug("Writing TaskManager configuration to {}", taskManagerConfigFile.getAbsolutePath());
			BootstrapTools.writeConfiguration(taskManagerConfig, taskManagerConfigFile);

			Utils.setupLocalResource(yarnFileSystem, appId,
					new Path(taskManagerConfigFile.toURI()), flinkConf, new Path(clientHomeDir));

			log.info("Prepared local resource for modified yaml: {}", flinkConf);
		}

		Map<String, LocalResource> taskManagerLocalResources = new HashMap<>();
		taskManagerLocalResources.put("flink.jar", flinkJar);
		taskManagerLocalResources.put("flink-conf.yaml", flinkConf);

		//To support Yarn Secure Integration Test Scenario
		if(yarnConfResource != null && krb5ConfResource != null) {
			taskManagerLocalResources.put(Utils.YARN_SITE_FILE_NAME, yarnConfResource);
			taskManagerLocalResources.put(Utils.KRB5_FILE_NAME, krb5ConfResource);
		}

		if(keytabResource != null) {
			taskManagerLocalResources.put(Utils.KEYTAB_FILE_NAME, keytabResource);
		}

		// prepare additional files to be shipped
		for (String pathStr : shipListString.split(",")) {
			if (!pathStr.isEmpty()) {
				LocalResource resource = Records.newRecord(LocalResource.class);
				Path path = new Path(pathStr);
				Utils.registerLocalResource(yarnFileSystem, path, resource);
				taskManagerLocalResources.put(path.getName(), resource);
			}
		}

		// now that all resources are prepared, we can create the launch context

		log.info("Creating container launch context for TaskManagers");

		boolean hasLogback = new File(workingDirectory, "logback.xml").exists();
		boolean hasLog4j = new File(workingDirectory, "log4j.properties").exists();

		String launchCommand = BootstrapTools.getTaskManagerShellCommand(
				flinkConfig, tmParams, ".", ApplicationConstants.LOG_DIR_EXPANSION_VAR,
				hasLogback, hasLog4j, hasKrb5, taskManagerMainClass);

		log.info("Starting TaskManagers with command: " + launchCommand);

		ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
		ctx.setCommands(Collections.singletonList(launchCommand));
		ctx.setLocalResources(taskManagerLocalResources);

		Map<String, String> containerEnv = new HashMap<>();
		containerEnv.putAll(tmParams.taskManagerEnv());

		// add YARN classpath, etc to the container environment
		containerEnv.put(ENV_FLINK_CLASSPATH, classPathString);
		Utils.setupYarnClassPath(yarnConfig, containerEnv);

		containerEnv.put(YarnConfigKeys.ENV_HADOOP_USER_NAME, UserGroupInformation.getCurrentUser().getUserName());

		if(remoteKeytabPath != null && remoteKeytabPrincipal != null) {
			containerEnv.put(YarnConfigKeys.KEYTAB_PATH, remoteKeytabPath);
			containerEnv.put(YarnConfigKeys.KEYTAB_PRINCIPAL, remoteKeytabPrincipal);
		}

		ctx.setEnvironment(containerEnv);

		try (DataOutputBuffer dob = new DataOutputBuffer()) {
			log.debug("Adding security tokens to Task Executor Container launch Context....");
			UserGroupInformation user = UserGroupInformation.getCurrentUser();
			Credentials credentials = user.getCredentials();
			credentials.writeTokenStorageToStream(dob);
			ByteBuffer securityTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
			ctx.setTokens(securityTokens);
		}
		catch (Throwable t) {
			log.error("Getting current user info failed when trying to launch the container", t);
		}

		return ctx;
	}
	
	/**
	 * Generate priority by given resouce profile. 
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
