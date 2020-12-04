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

package org.apache.flink.kubernetes.kubeclient.parameters;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptionsInternal;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.LocalObjectReference;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.kubernetes.configuration.KubernetesConfigOptions.CONTAINER_IMAGE_PULL_SECRETS;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract class for the {@link KubernetesParameters}.
 */
public abstract class AbstractKubernetesParameters implements KubernetesParameters {

	protected final Configuration flinkConfig;

	public AbstractKubernetesParameters(Configuration flinkConfig) {
		this.flinkConfig = checkNotNull(flinkConfig);
	}

	public Configuration getFlinkConfiguration() {
		return flinkConfig;
	}

	@Override
	public String getConfigDirectory() {
		final String configDir = flinkConfig.getOptional(DeploymentOptionsInternal.CONF_DIR).orElse(
			flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR));

		checkNotNull(configDir);
		return configDir;
	}

	@Override
	public String getClusterId() {
		final String clusterId = flinkConfig.getString(KubernetesConfigOptions.CLUSTER_ID);

		if (StringUtils.isBlank(clusterId)) {
			throw new IllegalArgumentException(KubernetesConfigOptions.CLUSTER_ID.key() + " must not be blank.");
		} else if (clusterId.length() > Constants.MAXIMUM_CHARACTERS_OF_CLUSTER_ID) {
			throw new IllegalArgumentException(KubernetesConfigOptions.CLUSTER_ID.key() + " must be no more than " +
				Constants.MAXIMUM_CHARACTERS_OF_CLUSTER_ID + " characters.");
		}

		return clusterId;
	}

	@Override
	public String getNamespace() {
		final String namespace = flinkConfig.getString(KubernetesConfigOptions.NAMESPACE);
		checkArgument(!namespace.trim().isEmpty(), "Invalid " + KubernetesConfigOptions.NAMESPACE + ".");

		return namespace;
	}

	@Override
	public String getImage() {
		final String containerImage = flinkConfig.getString(KubernetesConfigOptions.CONTAINER_IMAGE);
		checkArgument(!containerImage.trim().isEmpty(),
			"Invalid " + KubernetesConfigOptions.CONTAINER_IMAGE + ".");
		return containerImage;
	}

	@Override
	public KubernetesConfigOptions.ImagePullPolicy getImagePullPolicy() {
		return flinkConfig.get(KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY);
	}

	@Override
	public LocalObjectReference[] getImagePullSecrets() {
		final List<String> imagePullSecrets = flinkConfig.get(CONTAINER_IMAGE_PULL_SECRETS);

		if (imagePullSecrets == null) {
			return new LocalObjectReference[0];
		} else {
			return imagePullSecrets.stream()
				.map(String::trim)
				.filter(secret -> !secret.isEmpty())
				.map(LocalObjectReference::new)
				.toArray(LocalObjectReference[]::new);
		}
	}

	@Override
	public Map<String, String> getCommonLabels() {
		return Collections.unmodifiableMap(KubernetesUtils.getCommonLabels(getClusterId()));
	}

	@Override
	public String getFlinkConfDirInPod() {
		return flinkConfig.getString(KubernetesConfigOptions.FLINK_CONF_DIR);
	}

	@Override
	public String getFlinkLogDirInPod() {
		return flinkConfig.getString(KubernetesConfigOptions.FLINK_LOG_DIR);
	}

	@Override
	public String getContainerEntrypoint() {
		return flinkConfig.getString(KubernetesConfigOptions.KUBERNETES_ENTRY_PATH);
	}

	@Override
	public boolean hasLogback() {
		final String confDir = getConfigDirectory();
		final File logbackFile = new File(confDir, CONFIG_FILE_LOGBACK_NAME);
		return logbackFile.exists();
	}

	@Override
	public boolean hasLog4j() {
		final String confDir = getConfigDirectory();
		final File log4jFile = new File(confDir, CONFIG_FILE_LOG4J_NAME);
		return log4jFile.exists();
	}

	@Override
	public Optional<String> getExistingHadoopConfigurationConfigMap() {
		final String existingHadoopConfigMap = flinkConfig.getString(KubernetesConfigOptions.HADOOP_CONF_CONFIG_MAP);
		if (StringUtils.isBlank(existingHadoopConfigMap)) {
			return Optional.empty();
		} else {
			return Optional.of(existingHadoopConfigMap.trim());
		}
	}

	@Override
	public Optional<String> getLocalHadoopConfigurationDirectory() {
		final String hadoopConfDirEnv = System.getenv(Constants.ENV_HADOOP_CONF_DIR);
		if (StringUtils.isNotBlank(hadoopConfDirEnv)) {
			return Optional.of(hadoopConfDirEnv);
		}

		final String hadoopHomeEnv = System.getenv(Constants.ENV_HADOOP_HOME);
		if (StringUtils.isNotBlank(hadoopHomeEnv)) {
			// Hadoop 2.2+
			final File hadoop2ConfDir = new File(hadoopHomeEnv, "/etc/hadoop");
			if (hadoop2ConfDir.exists()) {
				return Optional.of(hadoop2ConfDir.getAbsolutePath());
			}

			// Hadoop 1.x
			final File hadoop1ConfDir = new File(hadoopHomeEnv, "/conf");
			if (hadoop1ConfDir.exists()) {
				return Optional.of(hadoop1ConfDir.getAbsolutePath());
			}
		}

		return Optional.empty();
	}

	public Map<String, String> getSecretNamesToMountPaths() {
		return flinkConfig.getOptional(KubernetesConfigOptions.KUBERNETES_SECRETS).orElse(Collections.emptyMap());
	}

	@Override
	public List<Map<String, String>> getEnvironmentsFromSecrets() {
		return flinkConfig.getOptional(KubernetesConfigOptions.KUBERNETES_ENV_SECRET_KEY_REF).orElse(Collections.emptyList());
	}
}
