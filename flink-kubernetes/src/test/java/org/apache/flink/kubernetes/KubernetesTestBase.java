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

package org.apache.flink.kubernetes;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.clusterframework.BootstrapTools;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.util.TestLogger;

import io.fabric8.kubernetes.client.KubernetesClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Base test class for Kubernetes.
 */
public class KubernetesTestBase extends TestLogger {
	@Rule
	public MixedKubernetesServer server = new MixedKubernetesServer(true, true);

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private File flinkConfDir;

	protected static final String NAMESPACE = "test";

	protected static final Configuration FLINK_CONFIG = new Configuration();

	protected static final String CLUSTER_ID = "my-flink-cluster1";

	protected static final String CONTAINER_IMAGE = "flink-k8s-test:latest";

	protected static final String MOCK_SERVICE_ID = "mock-uuid-of-service";

	protected static final String MOCK_SERVICE_IP = "192.168.0.1";

	protected static final String FLINK_MASTER_ENV_KEY = "LD_LIBRARY_PATH";

	protected static final String FLINK_MASTER_ENV_VALUE = "/usr/lib/native";

	@Before
	public void setUp() throws IOException {
		FLINK_CONFIG.setString(KubernetesConfigOptions.NAMESPACE, NAMESPACE);
		FLINK_CONFIG.setString(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
		FLINK_CONFIG.setString(KubernetesConfigOptions.CONTAINER_IMAGE, CONTAINER_IMAGE);
		FLINK_CONFIG.setString(KubernetesConfigOptionsInternal.SERVICE_ID, MOCK_SERVICE_ID);
		FLINK_CONFIG.setString(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, "main-class");
		FLINK_CONFIG.setString(BlobServerOptions.PORT, String.valueOf(Constants.BLOB_SERVER_PORT));
		FLINK_CONFIG.setString(TaskManagerOptions.RPC_PORT, String.valueOf(Constants.TASK_MANAGER_RPC_PORT));
		FLINK_CONFIG.setString(
			ResourceManagerOptions.CONTAINERIZED_MASTER_ENV_PREFIX + FLINK_MASTER_ENV_KEY,
			FLINK_MASTER_ENV_VALUE);

		flinkConfDir = temporaryFolder.newFolder().getAbsoluteFile();
		BootstrapTools.writeConfiguration(new Configuration(), new File(flinkConfDir, "flink-conf.yaml"));
		Map<String, String> map = new HashMap<>();
		map.put(ConfigConstants.ENV_FLINK_CONF_DIR, flinkConfDir.toString());
		TestBaseUtils.setEnv(map);
	}

	protected FlinkKubeClient getFabric8FlinkKubeClient(){
		return getFabric8FlinkKubeClient(FLINK_CONFIG);
	}

	protected FlinkKubeClient getFabric8FlinkKubeClient(Configuration flinkConfig){
		return new Fabric8FlinkKubeClient(flinkConfig, server.getClient().inNamespace(NAMESPACE));
	}

	protected KubernetesClient getKubeClient() {
		return server.getClient().inNamespace(NAMESPACE);
	}

	protected Map<String, String> getCommonLabels() {
		Map<String, String> labels = new HashMap<>();
		labels.put(Constants.LABEL_TYPE_KEY, Constants.LABEL_TYPE_NATIVE_TYPE);
		labels.put(Constants.LABEL_APP_KEY, CLUSTER_ID);
		return labels;
	}
}
