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

package org.apache.flink.kubernetes.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.utils.HttpClientUtils;
import okhttp3.Dispatcher;
import okhttp3.OkHttpClient;

import static org.apache.flink.kubernetes.configuration.Constants.SERVICE_NAME_SUFFIX;

/**
 * Builder for Kubernetes clients.
 */

public class KubernetesClientFactory {

	public static KubernetesClient create(Configuration flinkConf) {
		String master = flinkConf.getString(KubernetesConfigOptions.MASTER_URL);
		String namespace = flinkConf.getString(KubernetesConfigOptions.NAME_SPACE);

		ConfigBuilder clientConfig = new ConfigBuilder()
			.withApiVersion("v1")
			.withMasterUrl(master)
			.withWebsocketPingInterval(0)
			.withNamespace(namespace);

		Dispatcher dispatcher = new Dispatcher();
		OkHttpClient httpClient = HttpClientUtils.createHttpClient(clientConfig.build()).newBuilder()
			.dispatcher(dispatcher).build();
		return new DefaultKubernetesClient(httpClient, clientConfig.build());
	}

	public static void destroyCluster(Configuration flinkConf) {
		KubernetesClient kubernetesClient = create(flinkConf);
		String cluserId = flinkConf.getString(KubernetesConfigOptions.CLUSTER_ID);
		Preconditions.checkNotNull(cluserId);
		kubernetesClient.services().withName(cluserId + SERVICE_NAME_SUFFIX).delete();
	}
}
