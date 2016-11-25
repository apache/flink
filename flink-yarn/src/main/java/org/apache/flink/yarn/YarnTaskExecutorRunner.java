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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.security.SecurityContext;
import org.apache.flink.runtime.taskexecutor.TaskExecutor;
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration;
import org.apache.flink.runtime.taskexecutor.TaskManagerServices;
import org.apache.flink.runtime.taskexecutor.TaskManagerServicesConfiguration;
import org.apache.flink.runtime.taskexecutor.utils.TaskExecutorMetricsInitializer;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import javax.annotation.concurrent.GuardedBy;
import java.io.File;
import java.net.InetAddress;
import java.util.Map;

/**
 * This class is the executable entry point for running a TaskExecutor in a YARN container.
 */
public class YarnTaskExecutorRunner implements FatalErrorHandler {

	/** Logger */
	protected static final Logger LOG = LoggerFactory.getLogger(YarnTaskExecutorRunner.class);

	/** The process environment variables */
	private static final Map<String, String> ENV = System.getenv();

	/** The exit code returned if the initialization of the application master failed */
	private static final int INIT_ERROR_EXIT_CODE = 31;

	/** The lock to guard startup / shutdown / manipulation methods */
	private final Object lock = new Object();

	@GuardedBy("lock")
	private MetricRegistry metricRegistry;

	@GuardedBy("lock")
	private HighAvailabilityServices haServices;

	@GuardedBy("lock")
	private RpcService taskExecutorRpcService;

	@GuardedBy("lock")
	private TaskExecutor taskExecutor;

	// ------------------------------------------------------------------------
	//  Program entry point
	// ------------------------------------------------------------------------

	/**
	 * The entry point for the YARN task executor.
	 *
	 * @param args The command line arguments.
	 */
	public static void main(String[] args) {
		EnvironmentInformation.logEnvironmentInfo(LOG, "YARN TaskExecutor runner", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		// run and exit with the proper return code
		int returnCode = new YarnTaskExecutorRunner().run(args);
		System.exit(returnCode);
	}

	/**
	 * The instance entry point for the YARN task executor. Obtains user group
	 * information and calls the main work method {@link #runTaskExecutor(org.apache.flink.configuration.Configuration)} as a
	 * privileged action.
	 *
	 * @param args The command line arguments.
	 * @return The process exit code.
	 */
	protected int run(String[] args) {
		try {
			LOG.debug("All environment variables: {}", ENV);

			final String yarnClientUsername = ENV.get(YarnConfigKeys.ENV_HADOOP_USER_NAME);
			final String localDirs = ENV.get(Environment.LOCAL_DIRS.key());
			LOG.info("Current working/local Directory: {}", localDirs);

			final String currDir = ENV.get(Environment.PWD.key());
			LOG.info("Current working Directory: {}", currDir);

			final String remoteKeytabPath = ENV.get(YarnConfigKeys.KEYTAB_PATH);
			LOG.info("TM: remote keytab path obtained {}", remoteKeytabPath);

			final String remoteKeytabPrincipal = ENV.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
			LOG.info("TM: remote keytab principal obtained {}", remoteKeytabPrincipal);

			final Configuration configuration = GlobalConfiguration.loadConfiguration(currDir);
			FileSystem.setDefaultScheme(configuration);

			// configure local directory
			String flinkTempDirs = configuration.getString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, null);
			if (flinkTempDirs == null) {
				LOG.info("Setting directories for temporary file " + localDirs);
				configuration.setString(ConfigConstants.TASK_MANAGER_TMP_DIR_KEY, localDirs);
			}
			else {
				LOG.info("Overriding YARN's temporary file directories with those " +
						"specified in the Flink config: " + flinkTempDirs);
			}

			// tell akka to die in case of an error
			configuration.setBoolean(ConfigConstants.AKKA_JVM_EXIT_ON_FATAL_ERROR, true);

			String keytabPath = null;
			if(remoteKeytabPath != null) {
				File f = new File(currDir, Utils.KEYTAB_FILE_NAME);
				keytabPath = f.getAbsolutePath();
				LOG.info("keytab path: {}", keytabPath);
			}

			UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

			LOG.info("YARN daemon is running as: {} Yarn client user obtainer: {}",
					currentUser.getShortUserName(), yarnClientUsername);

			SecurityContext.SecurityConfiguration sc = new SecurityContext.SecurityConfiguration();

			//To support Yarn Secure Integration Test Scenario
			File krb5Conf = new File(currDir, Utils.KRB5_FILE_NAME);
			if(krb5Conf.exists() && krb5Conf.canRead()) {
				String krb5Path = krb5Conf.getAbsolutePath();
				LOG.info("KRB5 Conf: {}", krb5Path);
				org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
				conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
				conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, "true");
				sc.setHadoopConfiguration(conf);
			}

			if(keytabPath != null && remoteKeytabPrincipal != null) {
				configuration.setString(ConfigConstants.SECURITY_KEYTAB_KEY, keytabPath);
				configuration.setString(ConfigConstants.SECURITY_PRINCIPAL_KEY, remoteKeytabPrincipal);
			}

			SecurityContext.install(sc.setFlinkConfiguration(configuration));

			return SecurityContext.getInstalled().runSecured(new SecurityContext.FlinkSecuredRunner<Integer>() {
				@Override
				public Integer run() {
					return runTaskExecutor(configuration);
				}
			});

		}
		catch (Throwable t) {
			// make sure that everything whatever ends up in the log
			LOG.error("YARN Application Master initialization failed", t);
			return INIT_ERROR_EXIT_CODE;
		}
	}

	// ------------------------------------------------------------------------
	//  Core work method
	// ------------------------------------------------------------------------

	/**
	 * The main work method, must run as a privileged action.
	 *
	 * @return The return code for the Java process.
	 */
	protected int runTaskExecutor(Configuration config) {

		try {
			// ---- (1) create common services
			// first get the ResouceId, resource id is the container id for yarn.
			final String containerId = ENV.get(YarnFlinkResourceManager.ENV_FLINK_CONTAINER_ID);
			Preconditions.checkArgument(containerId != null,
					"ContainerId variable %s not set", YarnFlinkResourceManager.ENV_FLINK_CONTAINER_ID);

			ResourceID resourceID = new ResourceID(containerId);
			LOG.info("YARN assigned resource id {} for the task executor.", resourceID.toString());

			synchronized (lock) {
				haServices = HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(config);
				metricRegistry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(config));

				// ---- (2) init resource manager -------
				taskExecutorRpcService = createRpcService(config);
				taskExecutor = createTaskExecutor(config, resourceID);

				// ---- (3) start the task executor
				taskExecutor.start();
				LOG.debug("YARN task executor started");
			}

			taskExecutor.getTerminationFuture().get();
			// everything started, we can wait until all is done or the process is killed
			LOG.info("YARN task executor runner finished");
		}
		catch (Throwable t) {
			// make sure that everything whatever ends up in the log
			LOG.error("YARN task executor initialization failed", t);
			shutdown();
			return INIT_ERROR_EXIT_CODE;
		}

		return 0;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private RpcService createRpcService(
			Configuration configuration) throws Exception{
		String taskExecutorHostname = ENV.get(YarnResourceManager.ENV_FLINK_NODE_ID);//TODO
		Preconditions.checkArgument(taskExecutorHostname != null,
				"taskExecutorHostname variable %s not set", YarnResourceManager.ENV_FLINK_NODE_ID);

		// if no task manager port has been configured, use 0 (system will pick any free port)
		int taskExecutorPort = configuration.getInteger(ConfigConstants.TASK_MANAGER_IPC_PORT_KEY, 0);
		if (taskExecutorPort < 0 || taskExecutorPort > 65535) {
			throw new IllegalConfigurationException("Invalid value for '" +
					ConfigConstants.TASK_MANAGER_IPC_PORT_KEY +
					"' (port for the TaskManager actor system) : " + taskExecutorPort +
					" - Leave config parameter empty or use 0 to let the system choose a port automatically.");
		}

		return RpcServiceUtils.createRpcService(taskExecutorHostname, taskExecutorPort, configuration);
	}

	private TaskExecutor createTaskExecutor(Configuration config, ResourceID resourceID) throws Exception {

		InetAddress taskExecutorAddress = InetAddress.getByName(taskExecutorRpcService.getAddress());

		TaskManagerServicesConfiguration taskManagerServicesConfiguration = TaskManagerServicesConfiguration.fromConfiguration(
				config,
				taskExecutorAddress,
				false);

		TaskManagerServices taskManagerServices = TaskManagerServices.fromConfiguration(
				taskManagerServicesConfiguration,
				resourceID);

		TaskManagerConfiguration taskManagerConfiguration = TaskManagerConfiguration.fromConfiguration(config);

		TaskManagerMetricGroup taskManagerMetricGroup = new TaskManagerMetricGroup(
				metricRegistry,
				taskManagerServices.getTaskManagerLocation().getHostname(),
				resourceID.toString());

		// Initialize the TM metrics
		TaskExecutorMetricsInitializer.instantiateStatusMetrics(taskManagerMetricGroup, taskManagerServices.getNetworkEnvironment());

		return new TaskExecutor(
				taskManagerConfiguration,
				taskManagerServices.getTaskManagerLocation(),
				taskExecutorRpcService,
				taskManagerServices.getMemoryManager(),
				taskManagerServices.getIOManager(),
				taskManagerServices.getNetworkEnvironment(),
				haServices,
				metricRegistry,
				taskManagerMetricGroup,
				taskManagerServices.getBroadcastVariableManager(),
				taskManagerServices.getFileCache(),
				taskManagerServices.getTaskSlotTable(),
				taskManagerServices.getJobManagerTable(),
				taskManagerServices.getJobLeaderService(),
				this);
	}

	protected void shutdown() {
		synchronized (lock) {
			if (taskExecutor != null) {
				try {
					taskExecutor.shutDown();
				} catch (Throwable tt) {
					LOG.warn("Failed to stop the JobMaster", tt);
				}
			}
			if (taskExecutorRpcService != null) {
				try {
					taskExecutorRpcService.stopService();
				} catch (Throwable tt) {
					LOG.error("Error shutting down job master rpc service", tt);
				}
			}
			if (haServices != null) {
				try {
					haServices.shutdown();
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

	//-------------------------------------------------------------------------------------
	// Fatal error handler
	//-------------------------------------------------------------------------------------

	@Override
	public void onFatalError(Throwable exception) {
		LOG.error("Encountered fatal error.", exception);

		shutdown();
	}

}
