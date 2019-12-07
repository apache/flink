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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.ConfigMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;

/**
 * Decorate config map for flink configuration and log files.
 */
public class ConfigMapDecorator extends Decorator<ConfigMap, KubernetesConfigMap> {

	private static final Logger LOG = LoggerFactory.getLogger(ConfigMapDecorator.class);

	@Override
	protected ConfigMap decorateInternalResource(ConfigMap resource, Configuration flinkConfig) {

		final String confDir = CliFrontend.getConfigurationDirectoryFromEnv();
		Preconditions.checkArgument(confDir != null);

		final StringBuilder flinkConfContent = new StringBuilder();
		final Map<String, String> flinkConfigMap = new HashMap<>(flinkConfig.toMap());
		// Remove some keys should not be taken to jobmanager and taskmanager.
		flinkConfigMap.remove(KubernetesConfigOptions.KUBE_CONFIG_FILE.key());
		flinkConfigMap.forEach(
			(k, v) -> flinkConfContent.append(k).append(": ").append(v).append(System.lineSeparator())
		);

		final Map<String, String> configMap = resource.getData() == null ? new HashMap<>() : resource.getData();
		configMap.put(FLINK_CONF_FILENAME, flinkConfContent.toString());

		final String log4jFile = confDir + File.separator + CONFIG_FILE_LOG4J_NAME;
		try {
			final String log4jContent = KubernetesUtils.getContentFromFile(log4jFile);
			configMap.put(CONFIG_FILE_LOG4J_NAME, log4jContent);
		} catch (FileNotFoundException e) {
			LOG.info("File {} will not be added to configMap, {}", log4jFile, e.getMessage());
		}

		final String logbackFile = confDir + File.separator + CONFIG_FILE_LOGBACK_NAME;
		try {
			final String logbackContent = KubernetesUtils.getContentFromFile(logbackFile);
			configMap.put(CONFIG_FILE_LOGBACK_NAME, logbackContent);
		} catch (FileNotFoundException e) {
			LOG.info("File {} will not be added to configMap, {}", logbackFile, e.getMessage());
		}
		resource.setData(configMap);
		return resource;
	}
}
