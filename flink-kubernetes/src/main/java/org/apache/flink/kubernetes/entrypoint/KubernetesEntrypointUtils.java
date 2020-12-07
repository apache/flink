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

package org.apache.flink.kubernetes.entrypoint;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.KubernetesClusterDescriptor;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class contains utility methods for the {@link KubernetesSessionClusterEntrypoint}.
 */
class KubernetesEntrypointUtils {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesEntrypointUtils.class);

	/**
	 * For non-HA cluster, {@link JobManagerOptions#ADDRESS} has be set to Kubernetes service name on client side. See
	 * {@link KubernetesClusterDescriptor#deployClusterInternal}. So the TaskManager will use service address to contact
	 * with JobManager.
	 * For HA cluster, {@link JobManagerOptions#ADDRESS} will be set to the pod ip address. The TaskManager use Zookeeper
	 * or other high-availability service to find the address of JobManager.
	 *
	 * @return Updated configuration
	 */
	static Configuration loadConfiguration(Configuration dynamicParameters) {
		final String configDir = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
		Preconditions.checkNotNull(
			configDir,
			"Flink configuration directory (%s) in environment should not be null!",
			ConfigConstants.ENV_FLINK_CONF_DIR);

		final Configuration configuration = GlobalConfiguration.loadConfiguration(
			configDir,
			dynamicParameters);

		if (HighAvailabilityMode.isHighAvailabilityModeActivated(configuration)) {
			final String ipAddress = System.getenv().get(Constants.ENV_FLINK_POD_IP_ADDRESS);
			Preconditions.checkState(
				ipAddress != null,
				"JobManager ip address environment variable %s not set",
				Constants.ENV_FLINK_POD_IP_ADDRESS);
			configuration.setString(JobManagerOptions.ADDRESS, ipAddress);
			configuration.setString(RestOptions.ADDRESS, ipAddress);
		}

		return configuration;
	}

	private KubernetesEntrypointUtils() {}
}
