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

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.Utils;
import org.apache.flink.yarn.YarnConfigKeys;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.util.ClusterEntrypointUtils.tryFindUserLibDirectory;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class contains utility methods for the {@link YarnSessionClusterEntrypoint} and
 * {@link YarnJobClusterEntrypoint}.
 */
public class YarnEntrypointUtils {

	public static Configuration loadConfiguration(String workingDirectory, Map<String, String> env) {
		Configuration configuration = GlobalConfiguration.loadConfiguration(workingDirectory);

		final String keytabPrincipal = env.get(YarnConfigKeys.KEYTAB_PRINCIPAL);

		final String zooKeeperNamespace = env.get(YarnConfigKeys.ENV_ZOOKEEPER_NAMESPACE);

		final String hostname = env.get(ApplicationConstants.Environment.NM_HOST.key());
		Preconditions.checkState(
			hostname != null,
			"ApplicationMaster hostname variable %s not set",
			ApplicationConstants.Environment.NM_HOST.key());

		configuration.setString(JobManagerOptions.ADDRESS, hostname);
		configuration.setString(RestOptions.ADDRESS, hostname);

		if (zooKeeperNamespace != null) {
			configuration.setString(HighAvailabilityOptions.HA_CLUSTER_ID, zooKeeperNamespace);
		}

		// if a web monitor shall be started, set the port to random binding
		if (configuration.getInteger(WebOptions.PORT, 0) >= 0) {
			configuration.setInteger(WebOptions.PORT, 0);
		}

		if (!configuration.contains(RestOptions.BIND_PORT)) {
			// set the REST port to 0 to select it randomly
			configuration.setString(RestOptions.BIND_PORT, "0");
		}

		// if the user has set the deprecated YARN-specific config keys, we add the
		// corresponding generic config keys instead. that way, later code needs not
		// deal with deprecated config keys

		BootstrapTools.substituteDeprecatedConfigPrefix(configuration,
			ConfigConstants.YARN_APPLICATION_MASTER_ENV_PREFIX,
			ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX);

		BootstrapTools.substituteDeprecatedConfigPrefix(configuration,
			ConfigConstants.YARN_TASK_MANAGER_ENV_PREFIX,
			ResourceManagerOptions.CONTAINERIZED_TASK_MANAGER_ENV_PREFIX);

		final String keytabPath = Utils.resolveKeytabPath(workingDirectory, env.get(YarnConfigKeys.LOCAL_KEYTAB_PATH));

		if (keytabPath != null && keytabPrincipal != null) {
			configuration.setString(SecurityOptions.KERBEROS_LOGIN_KEYTAB, keytabPath);
			configuration.setString(SecurityOptions.KERBEROS_LOGIN_PRINCIPAL, keytabPrincipal);
		}

		final String localDirs = env.get(ApplicationConstants.Environment.LOCAL_DIRS.key());
		BootstrapTools.updateTmpDirectoriesInConfiguration(configuration, localDirs);

		return configuration;
	}

	public static void logYarnEnvironmentInformation(Map<String, String> env, Logger log) throws IOException {
		final String yarnClientUsername = env.get(YarnConfigKeys.ENV_HADOOP_USER_NAME);
		Preconditions.checkArgument(
			yarnClientUsername != null,
			"YARN client user name environment variable %s not set",
			YarnConfigKeys.ENV_HADOOP_USER_NAME);

		UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

		log.info("YARN daemon is running as: {} Yarn client user obtainer: {}",
			currentUser.getShortUserName(), yarnClientUsername);
	}

	public static Optional<File> getUsrLibDir(final Configuration configuration) {
		final YarnConfigOptions.UserJarInclusion userJarInclusion = configuration
				.getEnum(YarnConfigOptions.UserJarInclusion.class, YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR);
		final Optional<File> userLibDir = tryFindUserLibDirectory();

		checkState(
				userJarInclusion != YarnConfigOptions.UserJarInclusion.DISABLED || userLibDir.isPresent(),
				"The %s is set to %s. But the usrlib directory does not exist.",
				YarnConfigOptions.CLASSPATH_INCLUDE_USER_JAR.key(),
				YarnConfigOptions.UserJarInclusion.DISABLED);

		return userJarInclusion == YarnConfigOptions.UserJarInclusion.DISABLED ? userLibDir : Optional.empty();
	}
}
