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
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.JobManagerServices;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.net.SSLUtils;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.exceptions.ConfigurationException;
import org.apache.flink.runtime.resourcemanager.slotmanager.DefaultSlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerFactory;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcService;
import org.apache.flink.runtime.security.SecurityContext;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.runtime.webmonitor.WebMonitor;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.duration.FiniteDuration;

import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;

/**
 * This class is the executable entry point for the YARN application master.
 * It starts actor system and the actors for {@link org.apache.flink.runtime.jobmaster.JobMaster}
 * and {@link org.apache.flink.yarn.YarnResourceManager}.
 *
 * The JobMasters handles Flink job execution, while the YarnResourceManager handles container
 * allocation and failure detection.
 */
public class YarnFlinkApplicationMasterRunner implements LeaderContender, OnCompletionActions, FatalErrorHandler {

	/** Logger */
	protected static final Logger LOG = LoggerFactory.getLogger(YarnFlinkApplicationMasterRunner.class);

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
    private RpcService jobMasterRpcService;

    @GuardedBy("lock")
    private RpcService resourceManagerRpcService;

    @GuardedBy("lock")
    private ResourceManager resourceManager;

    @GuardedBy("lock")
    private JobMaster jobMaster;

    @GuardedBy("lock")
    JobManagerServices jobManagerServices;

    @GuardedBy("lock")
    JobManagerMetricGroup jobManagerMetrics;

    @GuardedBy("lock")
    private JobGraph jobGraph;

    @GuardedBy("lock")
    WebMonitor webMonitor;

    /** Flag marking the app master runner as started/running */
    @GuardedBy("lock")
    private boolean running;
	// ------------------------------------------------------------------------
	//  Program entry point
	// ------------------------------------------------------------------------

	/**
	 * The entry point for the YARN application master.
	 *
	 * @param args The command line arguments.
	 */
	public static void main(String[] args) {
		EnvironmentInformation.logEnvironmentInfo(LOG, "YARN ApplicationMaster runner", args);
		SignalHandler.register(LOG);
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		// run and exit with the proper return code
		int returnCode = new YarnFlinkApplicationMasterRunner().run(args);
        System.exit(returnCode);
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
			require(yarnClientUsername != null, "YARN client user name environment variable {} not set",
				YarnConfigKeys.ENV_HADOOP_USER_NAME);

			final String currDir = ENV.get(Environment.PWD.key());
			require(currDir != null, "Current working directory variable (%s) not set", Environment.PWD.key());
			LOG.debug("Current working Directory: {}", currDir);

			final String remoteKeytabPath = ENV.get(YarnConfigKeys.KEYTAB_PATH);
			LOG.debug("remoteKeytabPath obtained {}", remoteKeytabPath);

			final String remoteKeytabPrincipal = ENV.get(YarnConfigKeys.KEYTAB_PRINCIPAL);
			LOG.info("remoteKeytabPrincipal obtained {}", remoteKeytabPrincipal);

			String keytabPath = null;
			if(remoteKeytabPath != null) {
				File f = new File(currDir, Utils.KEYTAB_FILE_NAME);
				keytabPath = f.getAbsolutePath();
				LOG.debug("keytabPath: {}", keytabPath);
			}

			UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

			LOG.info("YARN daemon is running as: {} Yarn client user obtainer: {}",
					currentUser.getShortUserName(), yarnClientUsername );

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

			// Flink configuration
			final Map<String, String> dynamicProperties =
					FlinkYarnSessionCli.getDynamicProperties(ENV.get(YarnConfigKeys.ENV_DYNAMIC_PROPERTIES));
			LOG.debug("YARN dynamic properties: {}", dynamicProperties);

			final Configuration flinkConfig = createConfiguration(currDir, dynamicProperties);
			if(keytabPath != null && remoteKeytabPrincipal != null) {
				flinkConfig.setString(ConfigConstants.SECURITY_KEYTAB_KEY, keytabPath);
				flinkConfig.setString(ConfigConstants.SECURITY_PRINCIPAL_KEY, remoteKeytabPrincipal);
			}

			SecurityContext.install(sc.setFlinkConfiguration(flinkConfig));

			return SecurityContext.getInstalled().runSecured(new SecurityContext.FlinkSecuredRunner<Integer>() {
				@Override
				public Integer run() {
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

	// ------------------------------------------------------------------------
	//  Core work method
	// ------------------------------------------------------------------------

	/**
	 * The main work method, must run as a privileged action.
	 *
	 * @return The return code for the Java process.
	 */
	protected int runApplicationMaster(Configuration config) {

		try {
            // ---- (1) create common services
            // Note that we use the "appMasterHostname" given by YARN here, to make sure
            // we use the hostnames given by YARN consistently throughout akka.
            // for akka "localhost" and "localhost.localdomain" are different actors.
            final String appMasterHostname = ENV.get(Environment.NM_HOST.key());
            require(appMasterHostname != null,
                    "ApplicationMaster hostname variable %s not set", Environment.NM_HOST.key());
            LOG.info("YARN assigned hostname for application master: {}", appMasterHostname);

            // try to start the rpc service
            // using the port range definition from the config.
            final String amPortRange = config.getString(
                    ConfigConstants.YARN_APPLICATION_MASTER_PORT,
                    ConfigConstants.DEFAULT_YARN_JOB_MANAGER_PORT);

            synchronized (lock) {
                haServices = HighAvailabilityServicesUtils.createAvailableOrEmbeddedServices(config);
                metricRegistry = new MetricRegistry(MetricRegistryConfiguration.fromConfiguration(config));

                // ---- (2) init resource manager -------
                resourceManagerRpcService = createRpcService(config, appMasterHostname, amPortRange);
                resourceManager = createResourceManager(config);

                // ---- (3) init job master parameters
                jobMasterRpcService = createRpcService(config, appMasterHostname, amPortRange);
                jobManagerServices = JobManagerServices.fromConfiguration(config, haServices);
                jobManagerMetrics = new JobManagerMetricGroup(metricRegistry, jobMasterRpcService.getAddress());
                jobMaster = createJobMaster(config);

                // ---- (4) start the resource manager  and job master:
                resourceManager.start();
                LOG.debug("YARN Flink Resource Manager started");

                // mark the job as running in the HA services
                try {
                    haServices.getRunningJobsRegistry().setJobRunning(jobGraph.getJobID());
                }
                catch (Throwable t) {
                    throw new JobExecutionException(jobGraph.getJobID(),
                            "Could not register the job at the high-availability services", t);
                }
                jobMaster.start(UUID.randomUUID());
                LOG.debug("JobMaster started");

                // ---- (5) start the web monitor
                /**
                LOG.debug("Starting Web Frontend");

                webMonitor = BootstrapTools.startWebMonitorIfConfigured(config, actorSystem, jobManager, LOG);

                String protocol = "http://";
                if (config.getBoolean(ConfigConstants.JOB_MANAGER_WEB_SSL_ENABLED,
                    ConfigConstants.DEFAULT_JOB_MANAGER_WEB_SSL_ENABLED) && SSLUtils.getSSLEnabled(config)) {
                    protocol = "https://";
                }
                final String webMonitorURL = webMonitor == null ? null :
                    protocol + appMasterHostname + ":" + webMonitor.getServerPort();

                */
                running = true;
            }
            while (running) {
                Thread.sleep(100);
            }
            // everything started, we can wait until all is done or the process is killed
            LOG.info("YARN Application Master finished");
		}
		catch (Throwable t) {
			// make sure that everything whatever ends up in the log
			LOG.error("YARN Application Master initialization failed", t);
            shutdown();
			return INIT_ERROR_EXIT_CODE;
		}

		return 0;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Validates a condition, throwing a RuntimeException if the condition is violated.
	 * 
	 * @param condition The condition.
	 * @param message The message for the runtime exception, with format variables as defined by
	 *                {@link String#format(String, Object...)}.
	 * @param values The format arguments.
	 */
	private static void require(boolean condition, String message, Object... values) {
		if (!condition) {
			throw new RuntimeException(String.format(message, values));
		}
	}
    protected RpcService createRpcService(
            Configuration configuration,
            String bindAddress,
            String portRange) throws Exception{
        ActorSystem actorSystem = BootstrapTools.startActorSystem(configuration, bindAddress, portRange, LOG);
        FiniteDuration duration = AkkaUtils.getTimeout(configuration);
        return new AkkaRpcService(actorSystem, Time.of(duration.length(), duration.unit()));
    }

    private ResourceManager createResourceManager(Configuration config) throws ConfigurationException {
        final ResourceManagerConfiguration resourceManagerConfiguration = ResourceManagerConfiguration.fromConfiguration(config);
        final SlotManagerFactory slotManagerFactory = new DefaultSlotManager.Factory();
        final JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(haServices);

        return new YarnResourceManager(config,
                ENV,
                resourceManagerRpcService,
                resourceManagerConfiguration,
                haServices,
                slotManagerFactory,
                metricRegistry,
                jobLeaderIdService,
                this);
    }

    private JobMaster createJobMaster(Configuration config) throws Exception{
        // get JobGraph from local resources
        jobGraph = loadJobGraph();

        // libraries and class loader
        final BlobLibraryCacheManager libraryCacheManager = jobManagerServices.libraryCacheManager;
        try {
            libraryCacheManager.registerJob(
                    jobGraph.getJobID(), jobGraph.getUserJarBlobKeys(), jobGraph.getClasspaths());
        } catch (IOException e) {
            throw new Exception("Cannot set up the user code libraries: " + e.getMessage(), e);
        }

        final ClassLoader userCodeLoader = libraryCacheManager.getClassLoader(jobGraph.getJobID());
        if (userCodeLoader == null) {
            throw new Exception("The user code class loader could not be initialized.");
        }

        // now the JobManager
        return new JobMaster(
                jobGraph, config,
                jobMasterRpcService,
                haServices,
                jobManagerServices.executorService,
                jobManagerServices.libraryCacheManager,
                jobManagerServices.restartStrategyFactory,
                jobManagerServices.rpcAskTimeout,
                jobManagerMetrics,
                this,
                this,
                userCodeLoader);
    }

    protected void shutdown() {
        synchronized (lock) {
            try {
                haServices.getRunningJobsRegistry().setJobFinished(jobGraph.getJobID());
            }
            catch (Throwable t) {
                LOG.error("Could not un-register from high-availability services job {} ({}).",
                        jobGraph.getName(), jobGraph.getJobID(), t);
            }
            if (webMonitor != null) {
                try {
                    webMonitor.stop();
                } catch (Throwable ignored) {
                    LOG.warn("Failed to stop the web frontend", ignored);
                }
            }
            try {
                jobManagerServices.shutdown();
            } catch (Throwable tt) {
                LOG.error("Error while shutting down JobManager services", tt);
            }
            if (jobManagerMetrics != null) {
                jobManagerMetrics.close();
            }
            if (jobMaster != null) {
                try {
                    jobMaster.shutDown();
                } catch (Throwable tt) {
                    LOG.warn("Failed to stop the JobMaster", tt);
                }
            }
            if (resourceManager != null) {
                try {
                    resourceManager.shutDown();
                } catch (Throwable tt) {
                    LOG.warn("Failed to stop the ResourceManager", tt);
                }
            }
            if (jobMasterRpcService != null) {
                try {
                    jobMasterRpcService.stopService();
                } catch (Throwable tt) {
                    LOG.error("Error shutting down job master rpc service", tt);
                }
            }
            if (resourceManagerRpcService != null) {
                try {
                    resourceManagerRpcService.stopService();
                } catch (Throwable tt) {
                    LOG.error("Error shutting down resource manager rpc service", tt);
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
            running = false;
        }
    }

    private JobGraph loadJobGraph() {
        return null;
    }

	/**
	 * 
	 * @param baseDirectory
	 * @param additional
	 * 
	 * @return The configuration to be used by the TaskExecutors.
	 */
	@SuppressWarnings("deprecation")
	private static Configuration createConfiguration(String baseDirectory, Map<String, String> additional) {
		LOG.info("Loading config from directory " + baseDirectory);

		Configuration configuration = GlobalConfiguration.loadConfiguration(baseDirectory);

		configuration.setString(ConfigConstants.FLINK_BASE_DIR_PATH_KEY, baseDirectory);

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


    //-------------------------------------------------------------------------------------
    // Fatal error handler
    //-------------------------------------------------------------------------------------

    @Override
    public void onFatalError(Throwable exception) {
        LOG.error("Encountered fatal error.", exception);

        shutdown();
    }

    //----------------------------------------------------------------------------------------------
    // Leadership methods
    //----------------------------------------------------------------------------------------------

    @Override
    public void grantLeadership(final UUID leaderSessionID) {
        throw new UnsupportedOperationException("Yarn app master does not need leader select");
    }

    @Override
    public void revokeLeadership() {
        throw new UnsupportedOperationException("Yarn app master does not need leader select");
    }

    @Override
    public String getAddress() {
        throw new UnsupportedOperationException("Yarn app master does not need leader select");
    }

    @Override
    public void handleError(Exception exception) {
        throw new UnsupportedOperationException("Yarn app master does not need leader select");
    }
    //----------------------------------------------------------------------------------------------
    // Result and error handling methods
    //----------------------------------------------------------------------------------------------

    /**
     * Job completion notification triggered by JobManager
     */
    @Override
    public void jobFinished(JobExecutionResult result) {
        shutdown();
    }

    /**
     * Job completion notification triggered by JobManager
     */
    @Override
    public void jobFailed(Throwable cause) {
        shutdown();
    }

    /**
     * Job completion notification triggered by self
     */
    @Override
    public void jobFinishedByOther() {
        shutdown();
    }
}
