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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Factory class to create {@link FlinkKubeClient}.
 */
public class KubeClientFactory {

	private static final Logger LOG = LoggerFactory.getLogger(KubeClientFactory.class);

	public static FlinkKubeClient fromConfiguration(Configuration flinkConfig) {

		final Config config;

		final String kubeConfigFile = flinkConfig.getString(KubernetesConfigOptions.KUBE_CONFIG_FILE);
		if (kubeConfigFile != null) {
			LOG.debug("Trying to load kubernetes config from file: {}.", kubeConfigFile);
			try {
				config = Config.fromKubeconfig(KubernetesUtils.getContentFromFile(kubeConfigFile));
			} catch (IOException e) {
				throw new KubernetesClientException("Load kubernetes config failed.", e);
			}
		} else {
			LOG.debug("Trying to load default kubernetes config.");
			// Null means load from default context
			config = Config.autoConfigure(null);
		}

		final KubernetesClient client = new DefaultKubernetesClient(config);

		return new Fabric8FlinkKubeClient(flinkConfig, client);
	}
}
