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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.kubernetes.FlinkKubernetesOptions;
import org.apache.flink.kubernetes.kubeclient.fabric8.Fabric8FlinkKubeClient;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Factory class to create {@link KubeClient}.
 * */
public class KubeClientFactory {

	private static final Logger LOG = LoggerFactory.getLogger(KubeClientFactory.class);

	private static KubernetesClient kubernetesClient = null;

	@VisibleForTesting
	public static void setkubernetesClient(KubernetesClient client) {
		kubernetesClient = client;
	}

	public static synchronized KubeClient fromConfiguration(FlinkKubernetesOptions options) {

		//For test Purpose
		if (kubernetesClient != null) {
			return new Fabric8FlinkKubeClient(options, kubernetesClient);
		}

		Config config = null;

		if (options.getKubeConfigFilePath() != null) {
			LOG.info("Load kubernetes config from file: {}.", options.getKubeConfigFilePath());
			try {
				config = Config.fromKubeconfig(options.getKubeConfigFilePath());
			} catch (IOException e) {
				LOG.error("Load kubernetes config failed.", e);
			}
		}
		if (config == null) {
			LOG.info("Load default kubernetes config.");

			//null means load from default context
			config = Config.autoConfigure(null);
		}

		KubernetesClient client = new DefaultKubernetesClient(config);

		return new Fabric8FlinkKubeClient(options, client);
	}
}
