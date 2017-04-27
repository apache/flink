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

import org.apache.flink.client.CliFrontend;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_LIB_DIR;
import static org.apache.flink.yarn.cli.FlinkYarnSessionCli.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.yarn.cli.FlinkYarnSessionCli.CONFIG_FILE_LOGBACK_NAME;
import static org.apache.flink.yarn.cli.FlinkYarnSessionCli.getDynamicProperties;

/**
 * The descriptor with deployment information for spawning or resuming a {@link YarnClusterClient}.
 */
public abstract class AbstractYarnClusterDescriptor implements ClusterDescriptor<YarnClusterClient> {
	private static final Logger LOG = LoggerFactory.getLogger(YarnClusterDescriptor.class);

	/**
	 * Minimum memory requirements, checked by the Client.
	 */
	private static final int MIN_JM_MEMORY = 768; // the minimum memory should be higher than the min heap cutoff
	private static final int MIN_TM_MEMORY = 768;

	private Configuration conf = new YarnConfiguration();

	/**
	 * Files (usually in a distributed file system) used for the YARN session of Flink.
	 * Contains configuration files and jar files.
	 */
	private Path sessionFilesDir;

	/**
	 * If the user has specified a different number of slots, we store them here
	 */
	private int slots = -1;

	private int jobManagerMemoryMb = JobManagerOptions.JOB_MANAGER_HEAP_MEMORY.defaultValue();

	private int taskManagerMemoryMb = TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY.defaultValue();

	private int taskManagerCount = 1;

	private String yarnQueue;

	private String configurationDirectory;

	private Path flinkConfigurationPath;

	private Path flinkJarPath;

	private String dynamicPropertiesEncoded;

	/** Lazily initialized list of files to ship */
	protected List<File> shipFiles = new LinkedList<>();

	private org.apache.flink.configuration.Configuration flinkConfiguration;

	private boolean detached;

	private String customName;

	private String zookeeperNamespace;

	/** Optional Jar file to include in the system class loader of all application nodes
	 * (for per-job submission) */
	private Set<File> userJarFiles;

	public AbstractYarnClusterDescriptor() {
		// for unit tests only
		if(System.getenv("IN_TESTS") != null) {
			try {
				conf.addResource(new File(System.getenv("YARN_CONF_DIR") + "/yarn-site.xml").toURI().toURL());
			} catch (Throwable t) {
				throw new RuntimeException("Error",t);
			}
		}

		// tries to load the config through the environment, if it fails it can still be set through the setters
		try {
			this.configurationDirectory = CliFrontend.getConfigurationDirectoryFromEnv();
			this.flinkConfiguration = GlobalConfiguration.loadConfiguration(configurationDirectory);

			File confFile = new File(configurationDirectory + File.separator + GlobalConfiguration.FLINK_CONF_FILENAME);
			if (!confFile.exists()) {
				throw new RuntimeException("Unable to locate configuration file in " + confFile);
			}
			flinkConfigurationPath = new Path(confFile.getAbsolutePath());

			slots = flinkConfiguration.getInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 1);

			jobManagerMemoryMb = flinkConfiguration.getInteger(JobManagerOptions.JOB_MANAGER_HEAP_MEMORY);
			taskManagerMemoryMb = flinkConfiguration.getInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY);

		} catch (Exception e) {
			LOG.debug("Config couldn't be loaded from environment variable.", e);
		}
	}

	/**
	 * The class to bootstrap the application master of the Yarn cluster (runs main method).
	 */
	protected abstract Class<?> getApplicationMasterClass();

	public void setJobManagerMemory(int memoryMb) {
		if(memoryMb < MIN_JM_MEMORY) {
			throw new IllegalArgumentException("The JobManager memory (" + memoryMb + ") is below the minimum required memory amount "
				+ "of " + MIN_JM_MEMORY+ " MB");
		}
		this.jobManagerMemoryMb = memoryMb;
	}

	public void setTaskManagerMemory(int memoryMb) {
		if(memoryMb < MIN_TM_MEMORY) {
			throw new IllegalArgumentException("The TaskManager memory (" + memoryMb + ") is below the minimum required memory amount "
				+ "of " + MIN_TM_MEMORY+ " MB");
		}
		this.taskManagerMemoryMb = memoryMb;
	}

	public void setFlinkConfiguration(org.apache.flink.configuration.Configuration conf) {
		this.flinkConfiguration = conf;
	}

	public org.apache.flink.configuration.Configuration getFlinkConfiguration() {
		return flinkConfiguration;
	}

	public void setTaskManagerSlots(int slots) {
		if(slots <= 0) {
			throw new IllegalArgumentException("Number of TaskManager slots must be positive");
		}
		this.slots = slots;
	}

	public int getTaskManagerSlots() {
		return this.slots;
	}

	public void setQueue(String queue) {
		this.yarnQueue = queue;
	}

	public void setLocalJarPath(Path localJarPath) {
		if(!localJarPath.toString().endsWith("jar")) {
			throw new IllegalArgumentException("The passed jar path ('" + localJarPath + "') does not end with the 'jar' extension");
		}
		this.flinkJarPath = localJarPath;
	}

	public void setConfigurationFilePath(Path confPath) {
		flinkConfigurationPath = confPath;
	}

	public void setConfigurationDirectory(String configurationDirectory) {
		this.configurationDirectory = configurationDirectory;
	}

	public void setTaskManagerCount(int tmCount) {
		if(tmCount < 1) {
			throw new IllegalArgumentException("The TaskManager count has to be at least 1.");
		}
		this.taskManagerCount = tmCount;
	}

	public int getTaskManagerCount() {
		return this.taskManagerCount;
	}

	public void addShipFiles(List<File> shipFiles) {
		for (File shipFile: shipFiles) {
			// remove uberjar from ship list (by default everything in the lib/ folder is added to
			// the list of files to ship, but we handle the uberjar separately.
			if(!(shipFile.getName().startsWith("flink-dist") && shipFile.getName().endsWith("jar"))) {
				this.shipFiles.add(shipFile);
			}
		}
	}

	public void setDynamicPropertiesEncoded(String dynamicPropertiesEncoded) {
		this.dynamicPropertiesEncoded = dynamicPropertiesEncoded;
	}

	/**
	 * Returns true if the descriptor has the job jars to include in the classpath.
	 */
	public boolean hasUserJarFiles(List<URL> requiredJarFiles) {
		if (userJarFiles == null || userJarFiles.size() != requiredJarFiles.size()) {
			return false;
		}
		try {
			for(URL jarFile : requiredJarFiles) {
				if (!userJarFiles.contains(new File(jarFile.toURI()))) {
					return false;
				}
			}
		} catch (URISyntaxException e) {
			return false;
		}
		return true;
	}

	/**
	 * Sets the user jar which is included in the system classloader of all nodes.
	 */
	public void setProvidedUserJarFiles(List<URL> userJarFiles) {
		Set<File> localUserJarFiles = new HashSet<>(userJarFiles.size());
		for (URL jarFile : userJarFiles) {
			try {
				localUserJarFiles.add(new File(jarFile.toURI()));
			} catch (URISyntaxException e) {
				throw new IllegalArgumentException("Couldn't add local user jar: " + jarFile
					+ " Currently only file:/// URLs are supported.");
			}
		}
		this.userJarFiles = localUserJarFiles;
	}

	public String getDynamicPropertiesEncoded() {
		return this.dynamicPropertiesEncoded;
	}


	private void isReadyForDeployment() throws YarnDeploymentException {
		if(taskManagerCount <= 0) {
			throw new YarnDeploymentException("Taskmanager count must be positive");
		}
		if(this.flinkJarPath == null) {
			throw new YarnDeploymentException("The Flink jar path is null");
		}
		if(this.configurationDirectory == null) {
			throw new YarnDeploymentException("Configuration directory not set");
		}
		if(this.flinkConfigurationPath == null) {
			throw new YarnDeploymentException("Configuration path not set");
		}
		if(this.flinkConfiguration == null) {
			throw new YarnDeploymentException("Flink configuration object has not been set");
		}

		// Check if we don't exceed YARN's maximum virtual cores.
		// The number of cores can be configured in the config.
		// If not configured, it is set to the number of task slots
		int numYarnVcores = conf.getInt(YarnConfiguration.NM_VCORES, YarnConfiguration.DEFAULT_NM_VCORES);
		int configuredVcores = flinkConfiguration.getInteger(ConfigConstants.YARN_VCORES, slots);
		// don't configure more than the maximum configured number of vcores
		if (configuredVcores > numYarnVcores) {
			throw new IllegalConfigurationException(
				String.format("The number of virtual cores per node were configured with %d" +
						" but Yarn only has %d virtual cores available. Please note that the number" +
						" of virtual cores is set to the number of task slots by default unless configured" +
						" in the Flink config with '%s.'",
					configuredVcores, numYarnVcores, ConfigConstants.YARN_VCORES));
		}

		// check if required Hadoop environment variables are set. If not, warn user
		if(System.getenv("HADOOP_CONF_DIR") == null &&
			System.getenv("YARN_CONF_DIR") == null) {
			LOG.warn("Neither the HADOOP_CONF_DIR nor the YARN_CONF_DIR environment variable is set. " +
				"The Flink YARN Client needs one of these to be set to properly load the Hadoop " +
				"configuration for accessing YARN.");
		}
	}

	private static boolean allocateResource(int[] nodeManagers, int toAllocate) {
		for(int i = 0; i < nodeManagers.length; i++) {
			if(nodeManagers[i] >= toAllocate) {
				nodeManagers[i] -= toAllocate;
				return true;
			}
		}
		return false;
	}

	public void setDetachedMode(boolean detachedMode) {
		this.detached = detachedMode;
	}

	public boolean isDetachedMode() {
		return detached;
	}

	public String getZookeeperNamespace() {
		return zookeeperNamespace;
	}

	public void setZookeeperNamespace(String zookeeperNamespace) {
		this.zookeeperNamespace = zookeeperNamespace;
	}

	/**
	 * Gets a Hadoop Yarn client
	 * @return Returns a YarnClient which has to be shutdown manually
	 */
	protected YarnClient getYarnClient() {
		YarnClient yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);
		yarnClient.start();
		return yarnClient;
	}

	@Override
	public YarnClusterClient retrieve(String applicationID) {

		try {
			// check if required Hadoop environment variables are set. If not, warn user
			if (System.getenv("HADOOP_CONF_DIR") == null &&
				System.getenv("YARN_CONF_DIR") == null) {
				LOG.warn("Neither the HADOOP_CONF_DIR nor the YARN_CONF_DIR environment variable is set." +
					"The Flink YARN Client needs one of these to be set to properly load the Hadoop " +
					"configuration for accessing YARN.");
			}

			final ApplicationId yarnAppId = ConverterUtils.toApplicationId(applicationID);
			final YarnClient yarnClient = getYarnClient();
			final ApplicationReport appReport = yarnClient.getApplicationReport(yarnAppId);

			if (appReport.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
				// Flink cluster is not running anymore
				LOG.error("The application {} doesn't run anymore. It has previously completed with final status: {}",
					applicationID, appReport.getFinalApplicationStatus());
				throw new RuntimeException("The Yarn application " + applicationID + " doesn't run anymore.");
			}

			LOG.info("Found application JobManager host name '{}' and port '{}' from supplied application id '{}'",
				appReport.getHost(), appReport.getRpcPort(), applicationID);

			flinkConfiguration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, appReport.getHost());
			flinkConfiguration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, appReport.getRpcPort());

			return createYarnClusterClient(this, yarnClient, appReport, flinkConfiguration, sessionFilesDir, false);
		} catch (Exception e) {
			throw new RuntimeException("Couldn't retrieve Yarn cluster", e);
		}
	}

	@Override
	public YarnClusterClient deploy() {
		try {
			if(UserGroupInformation.isSecurityEnabled()) {
				// note: UGI::hasKerberosCredentials inaccurately reports false
				// for logins based on a keytab (fixed in Hadoop 2.6.1, see HADOOP-10786),
				// so we check only in ticket cache scenario.
				boolean useTicketCache = flinkConfiguration.getBoolean(SecurityOptions.KERBEROS_LOGIN_USETICKETCACHE);

				UserGroupInformation loginUser = UserGroupInformation.getCurrentUser();
				if (loginUser.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.KERBEROS
						&& useTicketCache && !loginUser.hasKerberosCredentials()) {
					LOG.error("Hadoop security with Kerberos is enabled but the login user does not have Kerberos credentials");
					throw new RuntimeException("Hadoop security with Kerberos is enabled but the login user " +
							"does not have Kerberos credentials");
				}
			}
			return deployInternal();
		} catch (Exception e) {
			throw new RuntimeException("Couldn't deploy Yarn cluster", e);
		}
	}

	/**
	 * This method will block until the ApplicationMaster/JobManager have been
	 * deployed on YARN.
	 */
	protected YarnClusterClient deployInternal() throws Exception {
		isReadyForDeployment();
		LOG.info("Using values:");
		LOG.info("\tTaskManager count = {}", taskManagerCount);
		LOG.info("\tJobManager memory = {}", jobManagerMemoryMb);
		LOG.info("\tTaskManager memory = {}", taskManagerMemoryMb);

		final YarnClient yarnClient = getYarnClient();


		// ------------------ Check if the specified queue exists --------------------

		try {
			List<QueueInfo> queues = yarnClient.getAllQueues();
			if (queues.size() > 0 && this.yarnQueue != null) { // check only if there are queues configured in yarn and for this session.
				boolean queueFound = false;
				for (QueueInfo queue : queues) {
					if (queue.getQueueName().equals(this.yarnQueue)) {
						queueFound = true;
						break;
					}
				}
				if (!queueFound) {
					String queueNames = "";
					for (QueueInfo queue : queues) {
						queueNames += queue.getQueueName() + ", ";
					}
					LOG.warn("The specified queue '" + this.yarnQueue + "' does not exist. " +
						"Available queues: " + queueNames);
				}
			} else {
				LOG.debug("The YARN cluster does not have any queues configured");
			}
		} catch(Throwable e) {
			LOG.warn("Error while getting queue information from YARN: " + e.getMessage());
			if(LOG.isDebugEnabled()) {
				LOG.debug("Error details", e);
			}
		}

		// ------------------ Add dynamic properties to local flinkConfiguraton ------
		Map<String, String> dynProperties = getDynamicProperties(dynamicPropertiesEncoded);
		for (Map.Entry<String, String> dynProperty : dynProperties.entrySet()) {
			flinkConfiguration.setString(dynProperty.getKey(), dynProperty.getValue());
		}

		// ------------------ Check if the YARN ClusterClient has the requested resources --------------

		// the yarnMinAllocationMB specifies the smallest possible container allocation size.
		// all allocations below this value are automatically set to this value.
		final int yarnMinAllocationMB = conf.getInt("yarn.scheduler.minimum-allocation-mb", 0);
		if(jobManagerMemoryMb < yarnMinAllocationMB || taskManagerMemoryMb < yarnMinAllocationMB) {
			LOG.warn("The JobManager or TaskManager memory is below the smallest possible YARN Container size. "
				+ "The value of 'yarn.scheduler.minimum-allocation-mb' is '" + yarnMinAllocationMB + "'. Please increase the memory size." +
				"YARN will allocate the smaller containers but the scheduler will account for the minimum-allocation-mb, maybe not all instances " +
				"you requested will start.");
		}

		// set the memory to minAllocationMB to do the next checks correctly
		if(jobManagerMemoryMb < yarnMinAllocationMB) {
			jobManagerMemoryMb =  yarnMinAllocationMB;
		}
		if(taskManagerMemoryMb < yarnMinAllocationMB) {
			taskManagerMemoryMb =  yarnMinAllocationMB;
		}

		// Create application via yarnClient
		final YarnClientApplication yarnApplication = yarnClient.createApplication();
		GetNewApplicationResponse appResponse = yarnApplication.getNewApplicationResponse();

		Resource maxRes = appResponse.getMaximumResourceCapability();
		final String NOTE = "Please check the 'yarn.scheduler.maximum-allocation-mb' and the 'yarn.nodemanager.resource.memory-mb' configuration values\n";
		if(jobManagerMemoryMb > maxRes.getMemory() ) {
			failSessionDuringDeployment(yarnClient, yarnApplication);
			throw new YarnDeploymentException("The cluster does not have the requested resources for the JobManager available!\n"
				+ "Maximum Memory: " + maxRes.getMemory() + "MB Requested: " + jobManagerMemoryMb + "MB. " + NOTE);
		}

		if(taskManagerMemoryMb > maxRes.getMemory() ) {
			failSessionDuringDeployment(yarnClient, yarnApplication);
			throw new YarnDeploymentException("The cluster does not have the requested resources for the TaskManagers available!\n"
				+ "Maximum Memory: " + maxRes.getMemory() + " Requested: " + taskManagerMemoryMb + "MB. " + NOTE);
		}

		final String NOTE_RSC = "\nThe Flink YARN client will try to allocate the YARN session, but maybe not all TaskManagers are " +
			"connecting from the beginning because the resources are currently not available in the cluster. " +
			"The allocation might take more time than usual because the Flink YARN client needs to wait until " +
			"the resources become available.";
		int totalMemoryRequired = jobManagerMemoryMb + taskManagerMemoryMb * taskManagerCount;
		ClusterResourceDescription freeClusterMem = getCurrentFreeClusterResources(yarnClient);
		if(freeClusterMem.totalFreeMemory < totalMemoryRequired) {
			LOG.warn("This YARN session requires " + totalMemoryRequired + "MB of memory in the cluster. "
				+ "There are currently only " + freeClusterMem.totalFreeMemory + "MB available." + NOTE_RSC);

		}
		if(taskManagerMemoryMb > freeClusterMem.containerLimit) {
			LOG.warn("The requested amount of memory for the TaskManagers (" + taskManagerMemoryMb + "MB) is more than "
				+ "the largest possible YARN container: " + freeClusterMem.containerLimit + NOTE_RSC);
		}
		if(jobManagerMemoryMb > freeClusterMem.containerLimit) {
			LOG.warn("The requested amount of memory for the JobManager (" + jobManagerMemoryMb + "MB) is more than "
				+ "the largest possible YARN container: " + freeClusterMem.containerLimit + NOTE_RSC);
		}

		// ----------------- check if the requested containers fit into the cluster.

		int[] nmFree = Arrays.copyOf(freeClusterMem.nodeManagersFree, freeClusterMem.nodeManagersFree.length);
		// first, allocate the jobManager somewhere.
		if(!allocateResource(nmFree, jobManagerMemoryMb)) {
			LOG.warn("Unable to find a NodeManager that can fit the JobManager/Application master. " +
				"The JobManager requires " + jobManagerMemoryMb + "MB. NodeManagers available: " +
				Arrays.toString(freeClusterMem.nodeManagersFree) + NOTE_RSC);
		}
		// allocate TaskManagers
		for(int i = 0; i < taskManagerCount; i++) {
			if(!allocateResource(nmFree, taskManagerMemoryMb)) {
				LOG.warn("There is not enough memory available in the YARN cluster. " +
					"The TaskManager(s) require " + taskManagerMemoryMb + "MB each. " +
					"NodeManagers available: " + Arrays.toString(freeClusterMem.nodeManagersFree) + "\n" +
					"After allocating the JobManager (" + jobManagerMemoryMb + "MB) and (" + i + "/" + taskManagerCount + ") TaskManagers, " +
					"the following NodeManagers are available: " + Arrays.toString(nmFree)  + NOTE_RSC );
			}
		}

		ApplicationReport report = startAppMaster(null, yarnClient, yarnApplication);

		String host = report.getHost();
		int port = report.getRpcPort();

		// Correctly initialize the Flink config
		flinkConfiguration.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, host);
		flinkConfiguration.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, port);

		// the Flink cluster is deployed in YARN. Represent cluster
		return createYarnClusterClient(this, yarnClient, report, flinkConfiguration, sessionFilesDir, true);
	}

	public ApplicationReport startAppMaster(JobGraph jobGraph, YarnClient yarnClient, YarnClientApplication yarnApplication) throws Exception {

		// ------------------ Set default file system scheme -------------------------

		try {
			org.apache.flink.core.fs.FileSystem.setDefaultScheme(flinkConfiguration);
		} catch (IOException e) {
			throw new IOException("Error while setting the default " +
					"filesystem scheme from configuration.", e);
		}

		// initialize file system
		// Copy the application master jar to the filesystem
		// Create a local resource to point to the destination jar path
		final FileSystem fs = FileSystem.get(conf);

		// hard coded check for the GoogleHDFS client because its not overriding the getScheme() method.
		if (!fs.getClass().getSimpleName().equals("GoogleHadoopFileSystem") &&
				fs.getScheme().startsWith("file")) {
			LOG.warn("The file system scheme is '" + fs.getScheme() + "'. This indicates that the "
					+ "specified Hadoop configuration path is wrong and the system is using the default Hadoop configuration values."
					+ "The Flink YARN client needs to store its files in a distributed file system");
		}

		ApplicationSubmissionContext appContext = yarnApplication.getApplicationSubmissionContext();
		Set<File> effectiveShipFiles = new HashSet<>(shipFiles.size());
		for (File file : shipFiles) {
			effectiveShipFiles.add(file.getAbsoluteFile());
		}

		//check if there is a logback or log4j file
		File logbackFile = new File(configurationDirectory + File.separator + CONFIG_FILE_LOGBACK_NAME);
		final boolean hasLogback = logbackFile.exists();
		if (hasLogback) {
			effectiveShipFiles.add(logbackFile);
		}

		File log4jFile = new File(configurationDirectory + File.separator + CONFIG_FILE_LOG4J_NAME);
		final boolean hasLog4j = log4jFile.exists();
		if (hasLog4j) {
			effectiveShipFiles.add(log4jFile);
			if (hasLogback) {
				// this means there is already a logback configuration file --> fail
				LOG.warn("The configuration directory ('" + configurationDirectory + "') contains both LOG4J and " +
					"Logback configuration files. Please delete or rename one of them.");
			}
		}

		addLibFolderToShipFiles(effectiveShipFiles);

		// add the user jar to the classpath of the to-be-created cluster
		if (userJarFiles != null) {
			effectiveShipFiles.addAll(userJarFiles);
		}


		// Set-up ApplicationSubmissionContext for the application

		final ApplicationId appId = appContext.getApplicationId();

		// ------------------ Add Zookeeper namespace to local flinkConfiguraton ------
		String zkNamespace = getZookeeperNamespace();
		// no user specified cli argument for namespace?
		if (zkNamespace == null || zkNamespace.isEmpty()) {
			// namespace defined in config? else use applicationId as default.
			zkNamespace = flinkConfiguration.getString(HighAvailabilityOptions.HA_CLUSTER_ID, String.valueOf(appId));
			setZookeeperNamespace(zkNamespace);
		}

		flinkConfiguration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, zkNamespace);

		if (HighAvailabilityMode.isHighAvailabilityModeActivated(flinkConfiguration)) {
			// activate re-execution of failed applications
			appContext.setMaxAppAttempts(
				flinkConfiguration.getInteger(
					ConfigConstants.YARN_APPLICATION_ATTEMPTS,
					YarnConfiguration.DEFAULT_RM_AM_MAX_ATTEMPTS));

			activateHighAvailabilitySupport(appContext);
		} else {
			// set number of application retries to 1 in the default case
			appContext.setMaxAppAttempts(
				flinkConfiguration.getInteger(
					ConfigConstants.YARN_APPLICATION_ATTEMPTS,
					1));
		}

		// local resource map for Yarn
		final Map<String, LocalResource> localResources = new HashMap<>(2 + effectiveShipFiles.size());
		// list of remote paths (after upload)
		final List<Path> paths = new ArrayList<>(2 + effectiveShipFiles.size());
		// ship list that enables reuse of resources for task manager containers
		StringBuilder envShipFileList = new StringBuilder();

		// upload and register ship files
		final List<String> classPaths = new ArrayList<>();
		for (File shipFile : effectiveShipFiles) {
			LocalResource shipResources = Records.newRecord(LocalResource.class);

			Path shipLocalPath = new Path("file://" + shipFile.getAbsolutePath());
			Path remotePath =
				Utils.setupLocalResource(fs, appId.toString(), shipLocalPath, shipResources, fs.getHomeDirectory());

			paths.add(remotePath);

			localResources.put(shipFile.getName(), shipResources);

			if (shipFile.isDirectory()) {
				// add directories to the classpath
				java.nio.file.Path shipPath = shipFile.toPath();
				final java.nio.file.Path parentPath = shipPath.getParent();

				Files.walkFileTree(shipPath, new SimpleFileVisitor<java.nio.file.Path>() {
					@Override
					public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs)
							throws IOException {
						java.nio.file.Path relativePath = parentPath.relativize(file);

						classPaths.add(relativePath.toString());

						return FileVisitResult.CONTINUE;
					}
				});
			} else {
				// add files to the classpath
				classPaths.add(shipFile.getName());
			}

			envShipFileList.append(remotePath).append(",");
		}

		// normalize classpath by sorting
		Collections.sort(classPaths);

		// classpath assembler
		StringBuilder classPathBuilder = new StringBuilder();
		for (String classPath : classPaths) {
			classPathBuilder.append(classPath).append(File.pathSeparator);
		}

		// Setup jar for ApplicationMaster
		LocalResource appMasterJar = Records.newRecord(LocalResource.class);
		LocalResource flinkConf = Records.newRecord(LocalResource.class);
		Path remotePathJar =
			Utils.setupLocalResource(fs, appId.toString(), flinkJarPath, appMasterJar, fs.getHomeDirectory());
		Path remotePathConf =
			Utils.setupLocalResource(fs, appId.toString(), flinkConfigurationPath, flinkConf, fs.getHomeDirectory());
		localResources.put("flink.jar", appMasterJar);
		localResources.put("flink-conf.yaml", flinkConf);

		paths.add(remotePathJar);
		classPathBuilder.append("flink.jar").append(File.pathSeparator);
		paths.add(remotePathConf);
		classPathBuilder.append("flink-conf.yaml").append(File.pathSeparator);

		// write job graph to tmp file and add it to local resource
		// TODO: server use user main method to generate job graph
		if (jobGraph != null) {
			try {
				File fp = File.createTempFile(appId.toString(), null);
				fp.deleteOnExit();
				try (FileOutputStream output = new FileOutputStream(fp);
					ObjectOutputStream obOutput = new ObjectOutputStream(output);){
					obOutput.writeObject(jobGraph);
				}
				LocalResource jobgraph = Records.newRecord(LocalResource.class);
				Path remoteJobGraph =
						Utils.setupLocalResource(fs, appId.toString(), new Path(fp.toURI()), jobgraph, fs.getHomeDirectory());
				localResources.put("job.graph", jobgraph);
				paths.add(remoteJobGraph);
				classPathBuilder.append("job.graph").append(File.pathSeparator);
			} catch (Exception e) {
				LOG.warn("Add job graph to local resource fail");
				throw e;
			}
		}

		sessionFilesDir = new Path(fs.getHomeDirectory(), ".flink/" + appId.toString() + "/");

		FsPermission permission = new FsPermission(FsAction.ALL, FsAction.NONE, FsAction.NONE);
		fs.setPermission(sessionFilesDir, permission); // set permission for path.

		//To support Yarn Secure Integration Test Scenario
		//In Integration test setup, the Yarn containers created by YarnMiniCluster does not have the Yarn site XML
		//and KRB5 configuration files. We are adding these files as container local resources for the container
		//applications (JM/TMs) to have proper secure cluster setup
		Path remoteKrb5Path = null;
		Path remoteYarnSiteXmlPath = null;
		boolean hasKrb5 = false;
		if(System.getenv("IN_TESTS") != null) {
			String krb5Config = System.getProperty("java.security.krb5.conf");
			if(krb5Config != null && krb5Config.length() != 0) {
				File krb5 = new File(krb5Config);
				LOG.info("Adding KRB5 configuration {} to the AM container local resource bucket", krb5.getAbsolutePath());
				LocalResource krb5ConfResource = Records.newRecord(LocalResource.class);
				Path krb5ConfPath = new Path(krb5.getAbsolutePath());
				remoteKrb5Path = Utils.setupLocalResource(fs, appId.toString(), krb5ConfPath, krb5ConfResource, fs.getHomeDirectory());
				localResources.put(Utils.KRB5_FILE_NAME, krb5ConfResource);

				File f = new File(System.getenv("YARN_CONF_DIR"),Utils.YARN_SITE_FILE_NAME);
				LOG.info("Adding Yarn configuration {} to the AM container local resource bucket", f.getAbsolutePath());
				LocalResource yarnConfResource = Records.newRecord(LocalResource.class);
				Path yarnSitePath = new Path(f.getAbsolutePath());
				remoteYarnSiteXmlPath = Utils.setupLocalResource(fs, appId.toString(), yarnSitePath, yarnConfResource, fs.getHomeDirectory());
				localResources.put(Utils.YARN_SITE_FILE_NAME, yarnConfResource);

				hasKrb5 = true;
			}
		}

		// setup security tokens
		LocalResource keytabResource = null;
		Path remotePathKeytab = null;
		String keytab = flinkConfiguration.getString(SecurityOptions.KERBEROS_LOGIN_KEYTAB);
		if(keytab != null) {
			LOG.info("Adding keytab {} to the AM container local resource bucket", keytab);
			keytabResource = Records.newRecord(LocalResource.class);
			Path keytabPath = new Path(keytab);
			remotePathKeytab = Utils.setupLocalResource(fs, appId.toString(), keytabPath, keytabResource, fs.getHomeDirectory());
			localResources.put(Utils.KEYTAB_FILE_NAME, keytabResource);
		}

		final ContainerLaunchContext amContainer = setupApplicationMasterContainer(hasLogback, hasLog4j, hasKrb5);

		if ( UserGroupInformation.isSecurityEnabled() && keytab == null ) {
			//set tokens only when keytab is not provided
			LOG.info("Adding delegation token to the AM container..");
			Utils.setTokensFor(amContainer, paths, conf);
		}

		amContainer.setLocalResources(localResources);
		fs.close();

		// Setup CLASSPATH and environment variables for ApplicationMaster
		final Map<String, String> appMasterEnv = new HashMap<>();
		// set user specified app master environment variables
		appMasterEnv.putAll(Utils.getEnvironmentVariables(ConfigConstants.YARN_APPLICATION_MASTER_ENV_PREFIX, flinkConfiguration));
		// set Flink app class path
		appMasterEnv.put(YarnConfigKeys.ENV_FLINK_CLASSPATH, classPathBuilder.toString());

		// set Flink on YARN internal configuration values
		appMasterEnv.put(YarnConfigKeys.ENV_TM_COUNT, String.valueOf(taskManagerCount));
		appMasterEnv.put(YarnConfigKeys.ENV_TM_MEMORY, String.valueOf(taskManagerMemoryMb));
		appMasterEnv.put(YarnConfigKeys.FLINK_JAR_PATH, remotePathJar.toString() );
		appMasterEnv.put(YarnConfigKeys.ENV_APP_ID, appId.toString());
		appMasterEnv.put(YarnConfigKeys.ENV_CLIENT_HOME_DIR, fs.getHomeDirectory().toString());
		appMasterEnv.put(YarnConfigKeys.ENV_CLIENT_SHIP_FILES, envShipFileList.toString());
		appMasterEnv.put(YarnConfigKeys.ENV_SLOTS, String.valueOf(slots));
		appMasterEnv.put(YarnConfigKeys.ENV_DETACHED, String.valueOf(detached));
		appMasterEnv.put(YarnConfigKeys.ENV_ZOOKEEPER_NAMESPACE, getZookeeperNamespace());

		// https://github.com/apache/hadoop/blob/trunk/hadoop-yarn-project/hadoop-yarn/hadoop-yarn-site/src/site/markdown/YarnApplicationSecurity.md#identity-on-an-insecure-cluster-hadoop_user_name
		appMasterEnv.put(YarnConfigKeys.ENV_HADOOP_USER_NAME, UserGroupInformation.getCurrentUser().getUserName());

		if(keytabResource != null) {
			appMasterEnv.put(YarnConfigKeys.KEYTAB_PATH, remotePathKeytab.toString() );
			String principal = flinkConfiguration.getString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL);
			appMasterEnv.put(YarnConfigKeys.KEYTAB_PRINCIPAL, principal );
		}

		//To support Yarn Secure Integration Test Scenario
		if(remoteYarnSiteXmlPath != null && remoteKrb5Path != null) {
			appMasterEnv.put(YarnConfigKeys.ENV_YARN_SITE_XML_PATH, remoteYarnSiteXmlPath.toString());
			appMasterEnv.put(YarnConfigKeys.ENV_KRB5_PATH, remoteKrb5Path.toString() );
		}

		if(dynamicPropertiesEncoded != null) {
			appMasterEnv.put(YarnConfigKeys.ENV_DYNAMIC_PROPERTIES, dynamicPropertiesEncoded);
		}

		// set classpath from YARN configuration
		Utils.setupYarnClassPath(conf, appMasterEnv);

		amContainer.setEnvironment(appMasterEnv);

		// Set up resource type requirements for ApplicationMaster
		Resource capability = Records.newRecord(Resource.class);
		capability.setMemory(jobManagerMemoryMb);
		capability.setVirtualCores(1);

		String name;
		if(customName == null) {
			name = "Flink session with " + taskManagerCount + " TaskManagers";
			if(detached) {
				name += " (detached)";
			}
		} else {
			name = customName;
		}

		appContext.setApplicationName(name);
		appContext.setApplicationType("Apache Flink");
		appContext.setAMContainerSpec(amContainer);
		appContext.setResource(capability);
		if(yarnQueue != null) {
			appContext.setQueue(yarnQueue);
		}

		setApplicationTags(appContext);

		// add a hook to clean up in case deployment fails
		Thread deploymentFailureHook = new DeploymentFailureHook(yarnClient, yarnApplication);
		Runtime.getRuntime().addShutdownHook(deploymentFailureHook);
		LOG.info("Submitting application master " + appId);
		yarnClient.submitApplication(appContext);

		LOG.info("Waiting for the cluster to be allocated");
		final long startTime = System.currentTimeMillis();
		ApplicationReport report;
		YarnApplicationState lastAppState = YarnApplicationState.NEW;
		loop: while( true ) {
			try {
				report = yarnClient.getApplicationReport(appId);
			} catch (IOException e) {
				throw new YarnDeploymentException("Failed to deploy the cluster.", e);
			}
			YarnApplicationState appState = report.getYarnApplicationState();
			LOG.debug("Application State: {}", appState);
			switch(appState) {
				case FAILED:
				case FINISHED: //TODO: the finished state may be valid in flip-6
				case KILLED:
					throw new YarnDeploymentException("The YARN application unexpectedly switched to state "
						+ appState + " during deployment. \n" +
						"Diagnostics from YARN: " + report.getDiagnostics() + "\n" +
						"If log aggregation is enabled on your cluster, use this command to further investigate the issue:\n" +
						"yarn logs -applicationId " + appId);
					//break ..
				case RUNNING:
					LOG.info("YARN application has been deployed successfully.");
					break loop;
				default:
					if (appState != lastAppState) {
						LOG.info("Deploying cluster, current state " + appState);
					}
					if(System.currentTimeMillis() - startTime > 60000) {
						LOG.info("Deployment took more than 60 seconds. Please check if the requested resources are available in the YARN cluster");
					}

			}
			lastAppState = appState;
			Thread.sleep(250);
		}
		// print the application id for user to cancel themselves.
		if (isDetachedMode()) {
			LOG.info("The Flink YARN client has been started in detached mode. In order to stop " +
					"Flink on YARN, use the following command or a YARN web interface to stop " +
					"it:\nyarn application -kill " + appId + "\nPlease also note that the " +
					"temporary files of the YARN session in the home directoy will not be removed.");
		}
		// since deployment was successful, remove the hook
		try {
			Runtime.getRuntime().removeShutdownHook(deploymentFailureHook);
		} catch (IllegalStateException e) {
			// we're already in the shut down hook.
		}
		return report;
	}

	/**
	 * Kills YARN application and stops YARN client.
	 *
	 * Use this method to kill the App before it has been properly deployed
	 */
	private void failSessionDuringDeployment(YarnClient yarnClient, YarnClientApplication yarnApplication) {
		LOG.info("Killing YARN application");

		try {
			yarnClient.killApplication(yarnApplication.getNewApplicationResponse().getApplicationId());
		} catch (Exception e) {
			// we only log a debug message here because the "killApplication" call is a best-effort
			// call (we don't know if the application has been deployed when the error occured).
			LOG.debug("Error while killing YARN application", e);
		}
		yarnClient.stop();
	}


	private static class ClusterResourceDescription {
		final public int totalFreeMemory;
		final public int containerLimit;
		final public int[] nodeManagersFree;

		public ClusterResourceDescription(int totalFreeMemory, int containerLimit, int[] nodeManagersFree) {
			this.totalFreeMemory = totalFreeMemory;
			this.containerLimit = containerLimit;
			this.nodeManagersFree = nodeManagersFree;
		}
	}

	private ClusterResourceDescription getCurrentFreeClusterResources(YarnClient yarnClient) throws YarnException, IOException {
		List<NodeReport> nodes = yarnClient.getNodeReports(NodeState.RUNNING);

		int totalFreeMemory = 0;
		int containerLimit = 0;
		int[] nodeManagersFree = new int[nodes.size()];

		for(int i = 0; i < nodes.size(); i++) {
			NodeReport rep = nodes.get(i);
			int free = rep.getCapability().getMemory() - (rep.getUsed() != null ? rep.getUsed().getMemory() : 0 );
			nodeManagersFree[i] = free;
			totalFreeMemory += free;
			if(free > containerLimit) {
				containerLimit = free;
			}
		}
		return new ClusterResourceDescription(totalFreeMemory, containerLimit, nodeManagersFree);
	}

	@Override
	public String getClusterDescription() {

		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream(baos);

			YarnClient yarnClient = getYarnClient();
			YarnClusterMetrics metrics = yarnClient.getYarnClusterMetrics();

			ps.append("NodeManagers in the ClusterClient " + metrics.getNumNodeManagers());
			List<NodeReport> nodes = yarnClient.getNodeReports(NodeState.RUNNING);
			final String format = "|%-16s |%-16s %n";
			ps.printf("|Property         |Value          %n");
			ps.println("+---------------------------------------+");
			int totalMemory = 0;
			int totalCores = 0;
			for (NodeReport rep : nodes) {
				final Resource res = rep.getCapability();
				totalMemory += res.getMemory();
				totalCores += res.getVirtualCores();
				ps.format(format, "NodeID", rep.getNodeId());
				ps.format(format, "Memory", res.getMemory() + " MB");
				ps.format(format, "vCores", res.getVirtualCores());
				ps.format(format, "HealthReport", rep.getHealthReport());
				ps.format(format, "Containers", rep.getNumContainers());
				ps.println("+---------------------------------------+");
			}
			ps.println("Summary: totalMemory " + totalMemory + " totalCores " + totalCores);
			List<QueueInfo> qInfo = yarnClient.getAllQueues();
			for (QueueInfo q : qInfo) {
				ps.println("Queue: " + q.getQueueName() + ", Current Capacity: " + q.getCurrentCapacity() + " Max Capacity: " +
					q.getMaximumCapacity() + " Applications: " + q.getApplications().size());
			}
			yarnClient.stop();
			return baos.toString();
		} catch (Exception e) {
			throw new RuntimeException("Couldn't get cluster description", e);
		}
	}

	public String getSessionFilesDir() {
		return sessionFilesDir.toString();
	}

	public void setName(String name) {
		if(name == null) {
			throw new IllegalArgumentException("The passed name is null");
		}
		customName = name;
	}

	private void activateHighAvailabilitySupport(ApplicationSubmissionContext appContext) throws
		InvocationTargetException, IllegalAccessException {

		ApplicationSubmissionContextReflector reflector = ApplicationSubmissionContextReflector.getInstance();

		reflector.setKeepContainersAcrossApplicationAttempts(appContext, true);
		reflector.setAttemptFailuresValidityInterval(appContext, AkkaUtils.getTimeout(flinkConfiguration).toMillis());
	}

	private void setApplicationTags(final ApplicationSubmissionContext appContext) throws InvocationTargetException,
		IllegalAccessException {

		final ApplicationSubmissionContextReflector reflector = ApplicationSubmissionContextReflector.getInstance();
		final String tagsString = flinkConfiguration.getString(ConfigConstants.YARN_APPLICATION_TAGS, "");

		final Set<String> applicationTags = new HashSet<>();

		// Trim whitespace and cull empty tags
		for (final String tag : tagsString.split(",")) {
			final String trimmedTag = tag.trim();
			if (!trimmedTag.isEmpty()) {
				applicationTags.add(trimmedTag);
			}
		}

		reflector.setApplicationTags(appContext, applicationTags);
	}

	/**
	 * Singleton object which uses reflection to determine whether the {@link ApplicationSubmissionContext}
	 * supports various methods which, depending on the Hadoop version, may or may not be supported.
	 *
	 * If an unsupported method is invoked, nothing happens.
	 *
	 * Currently three methods are proxied:
	 * - setApplicationTags (>= 2.4.0)
	 * - setAttemptFailuresValidityInterval (>= 2.6.0)
	 * - setKeepContainersAcrossApplicationAttempts (>= 2.4.0)
	 */
	private static class ApplicationSubmissionContextReflector {
		private static final Logger LOG = LoggerFactory.getLogger(ApplicationSubmissionContextReflector.class);

		private static final ApplicationSubmissionContextReflector instance =
			new ApplicationSubmissionContextReflector(ApplicationSubmissionContext.class);

		public static ApplicationSubmissionContextReflector getInstance() {
			return instance;
		}

		private static final String APPLICATION_TAGS_METHOD_NAME = "setApplicationTags";
		private static final String ATTEMPT_FAILURES_METHOD_NAME = "setAttemptFailuresValidityInterval";
		private static final String KEEP_CONTAINERS_METHOD_NAME = "setKeepContainersAcrossApplicationAttempts";

		private final Method applicationTagsMethod;
		private final Method attemptFailuresValidityIntervalMethod;
		private final Method keepContainersMethod;

		private ApplicationSubmissionContextReflector(Class<ApplicationSubmissionContext> clazz) {
			Method applicationTagsMethod;
			Method attemptFailuresValidityIntervalMethod;
			Method keepContainersMethod;

			try {
				// this method is only supported by Hadoop 2.4.0 onwards
				applicationTagsMethod = clazz.getMethod(APPLICATION_TAGS_METHOD_NAME, Set.class);
				LOG.debug("{} supports method {}.", clazz.getCanonicalName(), APPLICATION_TAGS_METHOD_NAME);
			} catch (NoSuchMethodException e) {
				LOG.debug("{} does not support method {}.", clazz.getCanonicalName(), APPLICATION_TAGS_METHOD_NAME);
				// assign null because the Hadoop version apparently does not support this call.
				applicationTagsMethod = null;
			}

			this.applicationTagsMethod = applicationTagsMethod;

			try {
				// this method is only supported by Hadoop 2.6.0 onwards
				attemptFailuresValidityIntervalMethod = clazz.getMethod(ATTEMPT_FAILURES_METHOD_NAME, long.class);
				LOG.debug("{} supports method {}.", clazz.getCanonicalName(), ATTEMPT_FAILURES_METHOD_NAME);
			} catch (NoSuchMethodException e) {
				LOG.debug("{} does not support method {}.", clazz.getCanonicalName(), ATTEMPT_FAILURES_METHOD_NAME);
				// assign null because the Hadoop version apparently does not support this call.
				attemptFailuresValidityIntervalMethod = null;
			}

			this.attemptFailuresValidityIntervalMethod = attemptFailuresValidityIntervalMethod;

			try {
				// this method is only supported by Hadoop 2.4.0 onwards
				keepContainersMethod = clazz.getMethod(KEEP_CONTAINERS_METHOD_NAME, boolean.class);
				LOG.debug("{} supports method {}.", clazz.getCanonicalName(), KEEP_CONTAINERS_METHOD_NAME);
			} catch (NoSuchMethodException e) {
				LOG.debug("{} does not support method {}.", clazz.getCanonicalName(), KEEP_CONTAINERS_METHOD_NAME);
				// assign null because the Hadoop version apparently does not support this call.
				keepContainersMethod = null;
			}

			this.keepContainersMethod = keepContainersMethod;
		}

		public void setApplicationTags(
			ApplicationSubmissionContext appContext,
			Set<String> applicationTags) throws InvocationTargetException, IllegalAccessException {
			if (applicationTagsMethod != null) {
				LOG.debug("Calling method {} of {}.",
					applicationTagsMethod.getName(),
					appContext.getClass().getCanonicalName());
				applicationTagsMethod.invoke(appContext, applicationTags);
			} else {
				LOG.debug("{} does not support method {}. Doing nothing.",
					appContext.getClass().getCanonicalName(),
					APPLICATION_TAGS_METHOD_NAME);
			}
		}

		public void setAttemptFailuresValidityInterval(
				ApplicationSubmissionContext appContext,
				long validityInterval) throws InvocationTargetException, IllegalAccessException {
			if (attemptFailuresValidityIntervalMethod != null) {
				LOG.debug("Calling method {} of {}.",
					attemptFailuresValidityIntervalMethod.getName(),
					appContext.getClass().getCanonicalName());
				attemptFailuresValidityIntervalMethod.invoke(appContext, validityInterval);
			} else {
				LOG.debug("{} does not support method {}. Doing nothing.",
					appContext.getClass().getCanonicalName(),
					ATTEMPT_FAILURES_METHOD_NAME);
			}
		}

		public void setKeepContainersAcrossApplicationAttempts(
			ApplicationSubmissionContext appContext,
			boolean keepContainers) throws InvocationTargetException, IllegalAccessException {

			if (keepContainersMethod != null) {
				LOG.debug("Calling method {} of {}.", keepContainersMethod.getName(),
					appContext.getClass().getCanonicalName());
				keepContainersMethod.invoke(appContext, keepContainers);
			} else {
				LOG.debug("{} does not support method {}. Doing nothing.",
					appContext.getClass().getCanonicalName(), KEEP_CONTAINERS_METHOD_NAME);
			}
		}
	}

	private static class YarnDeploymentException extends RuntimeException {
		private static final long serialVersionUID = -812040641215388943L;

		public YarnDeploymentException(String message) {
			super(message);
		}

		public YarnDeploymentException(String message, Throwable cause) {
			super(message, cause);
		}
	}

	private class DeploymentFailureHook extends Thread {

		DeploymentFailureHook(YarnClient yarnClient, YarnClientApplication yarnApplication) {
			this.yarnClient = yarnClient;
			this.yarnApplication = yarnApplication;
		}

		private YarnClient yarnClient;
		private YarnClientApplication yarnApplication;

		@Override
		public void run() {
			LOG.info("Cancelling deployment from Deployment Failure Hook");
			failSessionDuringDeployment(yarnClient, yarnApplication);
			LOG.info("Deleting files in " + sessionFilesDir);
			try {
				FileSystem fs = FileSystem.get(conf);
				fs.delete(sessionFilesDir, true);
				fs.close();
			} catch (IOException e) {
				LOG.error("Failed to delete Flink Jar and conf files in HDFS", e);
			}
		}
	}

	protected void addLibFolderToShipFiles(Set<File> effectiveShipFiles) {
		// Add lib folder to the ship files if the environment variable is set.
		// This is for convenience when running from the command-line.
		// (for other files users explicitly set the ship files)
		String libDir = System.getenv().get(ENV_FLINK_LIB_DIR);
		if (libDir != null) {
			File libDirFile = new File(libDir);
			if (libDirFile.isDirectory()) {
				effectiveShipFiles.add(libDirFile);
			} else {
				throw new YarnDeploymentException("The environment variable '" + ENV_FLINK_LIB_DIR +
					"' is set to '" + libDir + "' but the directory doesn't exist.");
			}
		} else if (this.shipFiles.isEmpty()) {
			LOG.warn("Environment variable '{}' not set and ship files have not been provided manually. " +
				"Not shipping any library files.", ENV_FLINK_LIB_DIR);
		}
	}

	protected ContainerLaunchContext setupApplicationMasterContainer(boolean hasLogback,
																	boolean hasLog4j,
																	boolean hasKrb5) {
		// ------------------ Prepare Application Master Container  ------------------------------

		// respect custom JVM options in the YAML file
		String javaOpts = flinkConfiguration.getString(CoreOptions.FLINK_JVM_OPTIONS);
		if (flinkConfiguration.getString(CoreOptions.FLINK_JM_JVM_OPTIONS).length() > 0) {
			javaOpts += " " + flinkConfiguration.getString(CoreOptions.FLINK_JM_JVM_OPTIONS);
		}
		//applicable only for YarnMiniCluster secure test run
		//krb5.conf file will be available as local resource in JM/TM container
		if (hasKrb5) {
			javaOpts += " -Djava.security.krb5.conf=krb5.conf";
		}

		// Set up the container launch context for the application master
		ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

		final  Map<String, String> startCommandValues = new HashMap<>();
		startCommandValues.put("java", "$JAVA_HOME/bin/java");
		startCommandValues.put("jvmmem", "-Xmx" +
			Utils.calculateHeapSize(jobManagerMemoryMb, flinkConfiguration) +
			"m");
		startCommandValues.put("jvmopts", javaOpts);
		String logging = "";

		if (hasLogback || hasLog4j) {
			logging = "-Dlog.file=\"" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/jobmanager.log\"";

			if(hasLogback) {
				logging += " -Dlogback.configurationFile=file:" + CONFIG_FILE_LOGBACK_NAME;
			}

			if(hasLog4j) {
				logging += " -Dlog4j.configuration=file:" + CONFIG_FILE_LOG4J_NAME;
			}
		}

		startCommandValues.put("logging", logging);
		startCommandValues.put("class", getApplicationMasterClass().getName());
		startCommandValues.put("redirects",
			"1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/jobmanager.out " +
			"2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/jobmanager.err");
		startCommandValues.put("args", "");

		final String commandTemplate = flinkConfiguration
			.getString(ConfigConstants.YARN_CONTAINER_START_COMMAND_TEMPLATE,
				ConfigConstants.DEFAULT_YARN_CONTAINER_START_COMMAND_TEMPLATE);
		final String amCommand =
			BootstrapTools.getStartCommand(commandTemplate, startCommandValues);

		amContainer.setCommands(Collections.singletonList(amCommand));

		LOG.debug("Application Master start command: " + amCommand);

		return amContainer;
	}

	/**
	 * Creates a YarnClusterClient; may be overriden in tests
	 */
	protected YarnClusterClient createYarnClusterClient(
			AbstractYarnClusterDescriptor descriptor,
			YarnClient yarnClient,
			ApplicationReport report,
			org.apache.flink.configuration.Configuration flinkConfiguration,
			Path sessionFilesDir,
			boolean perJobCluster) throws IOException, YarnException {
		return new YarnClusterClient(
			descriptor,
			yarnClient,
			report,
			flinkConfiguration,
			sessionFilesDir,
			perJobCluster);
	}
}

