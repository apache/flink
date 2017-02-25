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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.cli.FlinkYarnSessionCli;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * This class is the executable entry point for the YARN application master.
 * It starts actor system and the actors for {@link org.apache.flink.runtime.jobmaster.JobMaster}
 * and {@link YarnResourceManager}.
 *
 * The JobMasters handles Flink job execution, while the YarnResourceManager handles container
 * allocation and failure detection.
 */
public abstract class AbstractYarnFlinkApplicationMasterRunner {

	/** Logger */
	protected static final Logger LOG = LoggerFactory.getLogger(AbstractYarnFlinkApplicationMasterRunner.class);

	/** The process environment variables */
	protected static final Map<String, String> ENV = System.getenv();

	/** The exit code returned if the initialization of the application master failed */
	protected static final int INIT_ERROR_EXIT_CODE = 31;

	/** The host name passed by env */
	protected String appMasterHostname;

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

	// ------------------------------------------------------------------------
	//  Core work method
	// ------------------------------------------------------------------------

	/**
	 * The main work method, must run as a privileged action.
	 *
	 * @return The return code for the Java process.
	 */
	protected abstract int runApplicationMaster(Configuration config);

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

		// if the user doesn't specify port, set the port to random binding
		if (!configuration.containsKey(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY)) {
			configuration.setString(ConfigConstants.JOB_MANAGER_WEB_PORT_KEY, "0");
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

}
