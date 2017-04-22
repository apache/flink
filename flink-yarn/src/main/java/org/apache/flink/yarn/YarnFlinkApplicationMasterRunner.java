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

import akka.actor.ActorSystem;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServices;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

/**
 * This class is the executable entry point for the YARN Application Master that
 * executes a single Flink job and then shuts the YARN application down.
 * 
 * <p>The lifetime of the YARN application bound to that of the Flink job. Other
 * YARN Application Master implementations are for example the YARN session.
 * 
 * It starts actor system and the actors for {@link JobManagerRunner}
 * and {@link YarnResourceManager}.
 *
 * The JobManagerRunner start a {@link org.apache.flink.runtime.jobmaster.JobMaster}
 * JobMaster handles Flink job execution, while the YarnResourceManager handles container
 * allocation and failure detection.
 */
public class YarnFlinkApplicationMasterRunner implements OnCompletionActions, FatalErrorHandler {

	/** Logger */
	protected static final Logger LOG = LoggerFactory.getLogger(YarnFlinkApplicationMasterRunner.class);

	/** The job graph file path */
	private static final String JOB_GRAPH_FILE_PATH = "flink.jobgraph.path";

	/** The process environment variables */
	protected static final Map<String, String> ENV = System.getenv();

	/** The exit code returned if the initialization of the application master failed */
	protected static final int INIT_ERROR_EXIT_CODE = 31;

	/** The host name passed by env */
	protected String appMasterHostname;

	// ------------------------------------------------------------------------

	/** The lock to guard startup / shutdown / manipulation methods */
	private final Object lock = new Object();

	private MetricRegistry metricRegistry;

	private HighAvailabilityServices haServices;

	private HeartbeatServices heartbeatServices;

	private RpcService commonRpcService;

	private ResourceManager resourceManager;

	private JobManagerRunner jobManagerRunner;

	@GuardedBy("lock")
	private JobGraph jobGraph;

	public YarnFlinkApplicationMasterRunner() {}

	public YarnFlinkApplicationMasterRunner(
		MetricRegistry metricRegistry,
		HighAvailabilityServices haServices,
		HeartbeatServices heartbeatServices,
		RpcService commonRpcService,
		ResourceManager resourceManager,
		JobManagerRunner jobManagerRunner) {

		this.metricRegistry = metricRegistry;
		this.haServices = haServices;
		this.heartbeatServices = heartbeatServices;
		this.commonRpcService = commonRpcService;
		this.resourceManager = resourceManager;
		this.jobManagerRunner = jobManagerRunner;
	}

	// ------------------------------------------------------------------------
	//  Program entry point
	// ------------------------------------------------------------------------

	/**
	 * The entry point for the YARN application master.
	 *
	 * @param args The command line arguments.
	 */
	public static void main(String[] args) {
		EnvironmentInformation.logEnvironmentInfo(LOG, "YARN ApplicationMaster / ResourceManager / JobManager", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		// run and exit with the proper return code
		int returnCode = new YarnFlinkApplicationMasterRunner().initServiceAndComponents().run(args);
		System.exit(returnCode);
	}

	private YarnFlinkApplicationMasterRunner initServiceAndComponents() {
		LOG.info("Starting High Availability Services");
		final String currDir = ENV.get(Environment.PWD.key());
		final String remoteKeytabPath = ENV.get(YarnConfigKeys.KEYTAB_PATH);
		final String remoteKeytabPrincipal = ENV.get(YarnConfigKeys.KEYTAB_PRINCIPAL);

		// Flink configuration
		final Map<String, String> dynamicProperties =
			FlinkYarnSessionCli.getDynamicProperties(ENV.get(YarnConfigKeys.ENV_DYNAMIC_PROPERTIES));
		final Configuration config = createConfiguration(currDir, dynamicProperties);
		String keytabPath = null;
		if(remoteKeytabPath != null) {
			File f = new File(currDir, Utils.KEYTAB_FILE_NAME);
			keytabPath = f.getAbsolutePath();
			LOG.debug("Keytab path: {}", keytabPath);
		}
		if (keytabPath != null && remoteKeytabPrincipal != null) {
			config.setString(SecurityOptions.KERBEROS_LOGIN_KEYTAB, keytabPath);
			config.setString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, remoteKeytabPrincipal);
		}

		// try to start the rpc service
		// using the port range definition from the config.
		final String amPortRange = config.getString(
			ConfigConstants.YARN_APPLICATION_MASTER_PORT,
			ConfigConstants.DEFAULT_YARN_JOB_MANAGER_PORT);

		try {
			// ---- (1) create common services
			HighAvailabilityServices haServices = HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(config);
			HeartbeatServices heartbeatServices = HeartbeatServices.fromConfiguration(config);
			MetricRegistry metricRegistry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(config));
			RpcService commonRpcService = createRpcService(config, appMasterHostname, amPortRange);

			// ---- (2) init resource manager -------
			ResourceManager resourceManager = createResourceManager(config);

			// ---- (3) init job master parameters
			JobManagerRunner jobManagerRunner = createJobManagerRunner(config);

			return new YarnFlinkApplicationMasterRunner(
					metricRegistry,
					haServices,
					heartbeatServices,
					commonRpcService,
					resourceManager,
					jobManagerRunner);
		} catch (Exception e) {
			LOG.error("YARN Application Master initialization failed", e);
			shutdown(ApplicationStatus.FAILED, e.getMessage());
			return null;
		}
	}

	/**
	 * The instance entry point for the YARN application master. Obtains user group
	 * information and calls the main work method {@link #runApplicationMaster(org.apache.flink.configuration.Configuration)} as a
	 * privileged action.
	 *
	 * @param args The command line arguments.
	 * @return The process exit code.
	 */
	protected int run(String[] args) {
		try {
			LOG.debug("All environment variables: {}", ENV);

			final String yarnClientUsername = ENV.get(YarnConfigKeys.ENV_HADOOP_USER_NAME);
			Preconditions.checkArgument(yarnClientUsername != null, "YARN client user name environment variable {} not set",
				YarnConfigKeys.ENV_HADOOP_USER_NAME);

			final String currDir = ENV.get(Environment.PWD.key());
			Preconditions.checkArgument(currDir != null, "Current working directory variable (%s) not set", Environment.PWD.key());
			LOG.debug("Current working directory: {}", currDir);

			final String remoteKeytabPath = ENV.get(YarnConfigKeys.KEYTAB_PATH);
			LOG.debug("Remote keytab path obtained {}", remoteKeytabPath);

			final String remoteKeytabPrincipal = ENV.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
			LOG.info("Remote keytab principal obtained {}", remoteKeytabPrincipal);

			String keytabPath = null;
			if(remoteKeytabPath != null) {
				File f = new File(currDir, Utils.KEYTAB_FILE_NAME);
				keytabPath = f.getAbsolutePath();
				LOG.debug("Keytab path: {}", keytabPath);
			}

			UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

			LOG.info("YARN daemon is running as: {} Yarn client user obtainer: {}",
				currentUser.getShortUserName(), yarnClientUsername );

			// Flink configuration
			final Map<String, String> dynamicProperties =
				FlinkYarnSessionCli.getDynamicProperties(ENV.get(YarnConfigKeys.ENV_DYNAMIC_PROPERTIES));
			LOG.debug("YARN dynamic properties: {}", dynamicProperties);

			final Configuration flinkConfig = createConfiguration(currDir, dynamicProperties);
			if (keytabPath != null && remoteKeytabPrincipal != null) {
				flinkConfig.setString(SecurityOptions.KERBEROS_LOGIN_KEYTAB, keytabPath);
				flinkConfig.setString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, remoteKeytabPrincipal);
			}

			org.apache.hadoop.conf.Configuration hadoopConfiguration = null;

			//To support Yarn Secure Integration Test Scenario
			File krb5Conf = new File(currDir, Utils.KRB5_FILE_NAME);
			if (krb5Conf.exists() && krb5Conf.canRead()) {
				String krb5Path = krb5Conf.getAbsolutePath();
				LOG.info("KRB5 Conf: {}", krb5Path);
				hadoopConfiguration = new org.apache.hadoop.conf.Configuration();
				hadoopConfiguration.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
				hadoopConfiguration.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, "true");
			}

			SecurityUtils.SecurityConfiguration sc;
			if(hadoopConfiguration != null) {
				sc = new SecurityUtils.SecurityConfiguration(flinkConfig, hadoopConfiguration);
			} else {
				sc = new SecurityUtils.SecurityConfiguration(flinkConfig);
			}

			SecurityUtils.install(sc);

			// Note that we use the "appMasterHostname" given by YARN here, to make sure
			// we use the hostnames given by YARN consistently throughout akka.
			// for akka "localhost" and "localhost.localdomain" are different actors.
			this.appMasterHostname = ENV.get(Environment.NM_HOST.key());
			Preconditions.checkArgument(appMasterHostname != null,
				"ApplicationMaster hostname variable %s not set", Environment.NM_HOST.key());
			LOG.info("YARN assigned hostname for application master: {}", appMasterHostname);

			return SecurityUtils.getInstalledContext().runSecured(new Callable<Integer>() {
				@Override
				public Integer call() throws Exception {
					return runApplicationMaster(flinkConfig);
				}
			});

		}
		catch (Throwable t) {
			// make sure that everything whatever ends up in the log
			LOG.error("YARN Application Master initialization failed", t);
			return INIT_ERROR_EXIT_CODE;
		}
	}

	/**
	 * The main work method, must run as a privileged action.
	 *
	 * @return The return code for the Java process.
	 */
	protected int runApplicationMaster(Configuration config) {

		try {
			synchronized (lock) {
				// ---- (1) start the resource manager  and job manager runner:
				resourceManager.start();
				LOG.debug("YARN Flink Resource Manager started");

				jobManagerRunner.start();
				LOG.debug("Job Manager Runner started");

				// ---- (2) start the web monitor
				// TODO: add web monitor
			}
			// wait for resource manager to finish
			resourceManager.getTerminationFuture().get();
			// everything started, we can wait until all is done or the process is killed
			LOG.info("YARN Application Master finished");
		}
		catch (Throwable t) {
			// make sure that everything whatever ends up in the log
			LOG.error("Run application master failed", t);
			return INIT_ERROR_EXIT_CODE;
		}

		return 0;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * @param baseDirectory  The working directory
	 * @param additional Additional parameters
	 *
	 * @return The configuration to be used by the TaskExecutors.
	 */
	private static Configuration createConfiguration(String baseDirectory, Map<String, String> additional) {
		LOG.info("Loading config from directory {}.", baseDirectory);

		Configuration configuration = GlobalConfiguration.loadConfiguration(baseDirectory);

		// add dynamic properties to JobManager configuration.
		for (Map.Entry<String, String> property : additional.entrySet()) {
			configuration.setString(property.getKey(), property.getValue());
		}

		// override zookeeper namespace with user cli argument (if provided)
		String cliZKNamespace = ENV.get(YarnConfigKeys.ENV_ZOOKEEPER_NAMESPACE);
		if (cliZKNamespace != null && !cliZKNamespace.isEmpty()) {
			configuration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, cliZKNamespace);
		}

		// if a web monitor shall be started, set the port to random binding
		if (configuration.getInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, 0) >= 0) {
			configuration.setInteger(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, 0);
		}

		// if the user has set the deprecated YARN-specific config keys, we add the
		// corresponding generic config keys instead. that way, later code needs not
		// deal with deprecated config keys

		BootstrapTools.substituteDeprecatedConfigKey(configuration,
			ConfigConstants.YARN_HEAP_CUTOFF_RATIO,
			ConfigConstants.CONTAINERIZED_HEAP_CUTOFF_RATIO);

		BootstrapTools.substituteDeprecatedConfigKey(configuration,
			ConfigConstants.YARN_HEAP_CUTOFF_MIN,
			ConfigConstants.CONTAINERIZED_HEAP_CUTOFF_MIN);

		BootstrapTools.substituteDeprecatedConfigPrefix(configuration,
			ConfigConstants.YARN_APPLICATION_MASTER_ENV_PREFIX,
			ConfigConstants.CONTAINERIZED_MASTER_ENV_PREFIX);

		BootstrapTools.substituteDeprecatedConfigPrefix(configuration,
			ConfigConstants.YARN_TASK_MANAGER_ENV_PREFIX,
			ConfigConstants.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX);

		return configuration;
	}

	protected RpcService createRpcService(
			Configuration configuration,
			String bindAddress,
			String portRange) throws Exception{
		ActorSystem actorSystem = BootstrapTools.startActorSystem(configuration, bindAddress, portRange, LOG);
		FiniteDuration duration = AkkaUtils.getTimeout(configuration);
		return new AkkaRpcService(actorSystem, Time.of(duration.length(), duration.unit()));
	}

	private ResourceManager<?> createResourceManager(Configuration config) throws Exception {
		final ResourceManagerConfiguration resourceManagerConfiguration = ResourceManagerConfiguration.fromConfiguration(config);
		final ResourceManagerRuntimeServicesConfiguration resourceManagerRuntimeServicesConfiguration = ResourceManagerRuntimeServicesConfiguration.fromConfiguration(config);
		final ResourceManagerRuntimeServices resourceManagerRuntimeServices = ResourceManagerRuntimeServices.fromConfiguration(
			resourceManagerRuntimeServicesConfiguration,
			haServices,
			commonRpcService.getScheduledExecutor());

		return new YarnResourceManager(
			commonRpcService, ResourceID.generate(),
			config,
			ENV,
			resourceManagerConfiguration,
			haServices,
			heartbeatServices,
			resourceManagerRuntimeServices.getSlotManagerFactory(),
			metricRegistry,
			resourceManagerRuntimeServices.getJobLeaderIdService(),
			this);
	}

	private JobManagerRunner createJobManagerRunner(Configuration config) throws Exception{
		// first get JobGraph from local resources
		//TODO: generate the job graph from user's jar
		jobGraph = loadJobGraph(config);

		// now the JobManagerRunner
		return new JobManagerRunner(
			ResourceID.generate(),
			jobGraph,
			config,
			commonRpcService,
			haServices,
			heartbeatServices,
			this,
			this);
	}

	protected void shutdown(ApplicationStatus status, String msg) {
		// Need to clear the job state in the HA services before shutdown
		try {
			haServices.getRunningJobsRegistry().clearJob(jobGraph.getJobID());
		}
		catch (Throwable t) {
			LOG.warn("Could not clear the job at the high-availability services", t);
		}

		synchronized (lock) {
			if (jobManagerRunner != null) {
				try {
					jobManagerRunner.shutdown();
				} catch (Throwable tt) {
					LOG.warn("Failed to stop the JobManagerRunner", tt);
				}
			}
			if (resourceManager != null) {
				try {
					resourceManager.shutDownCluster(status, msg);
					resourceManager.shutDown();
				} catch (Throwable tt) {
					LOG.warn("Failed to stop the ResourceManager", tt);
				}
			}
			if (commonRpcService != null) {
				try {
					commonRpcService.stopService();
				} catch (Throwable tt) {
					LOG.error("Error shutting down resource manager rpc service", tt);
				}
			}
			if (haServices != null) {
				try {
					haServices.close();
				} catch (Throwable tt) {
					LOG.warn("Failed to stop the HA service", tt);
				}
			}
			if (metricRegistry != null) {
				try {
					metricRegistry.shutdown();
				} catch (Throwable tt) {
					LOG.warn("Failed to stop the metrics registry", tt);
				}
			}
		}
	}

	private static JobGraph loadJobGraph(Configuration config) throws Exception {
		JobGraph jg = null;
		String jobGraphFile = config.getString(JOB_GRAPH_FILE_PATH, "job.graph");
		if (jobGraphFile != null) {
			File fp = new File(jobGraphFile);
			if (fp.isFile()) {
				try (FileInputStream input = new FileInputStream(fp);
					ObjectInputStream obInput = new ObjectInputStream(input);) {
					jg = (JobGraph) obInput.readObject();
				} catch (IOException e) {
					LOG.warn("Failed to read job graph file", e);
				}
			}
		}
		if (jg == null) {
			throw new Exception("Fail to load job graph " + jobGraphFile);
		}
		return jg;
	}

	//-------------------------------------------------------------------------------------
	// Fatal error handler
	//-------------------------------------------------------------------------------------

	@Override
	public void onFatalError(Throwable exception) {
		LOG.error("Encountered fatal error.", exception);

		shutdown(ApplicationStatus.FAILED, exception.getMessage());
	}

	//----------------------------------------------------------------------------------------------
	// Result and error handling methods
	//----------------------------------------------------------------------------------------------

	/**
	 * Job completion notification triggered by JobManager
	 */
	@Override
	public void jobFinished(JobExecutionResult result) {
		shutdown(ApplicationStatus.SUCCEEDED, null);
	}

	/**
	 * Job completion notification triggered by JobManager
	 */
	@Override
	public void jobFailed(Throwable cause) {
		shutdown(ApplicationStatus.FAILED, cause.getMessage());
	}

	/**
	 * Job completion notification triggered by self
	 */
	@Override
	public void jobFinishedByOther() {
		shutdown(ApplicationStatus.UNKNOWN, null);
	}
}
