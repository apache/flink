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

package org.apache.flink.kubernetes.deploy;

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.ReplicationController;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesServer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static org.apache.flink.configuration.GlobalConfiguration.FLINK_CONF_FILENAME;
import static org.apache.flink.kubernetes.configuration.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.configuration.Constants.FLINK_CONF_VOLUME;
import static org.apache.flink.kubernetes.configuration.Constants.JOBMANAGER_RC_NAME_SUFFIX;
import static org.apache.flink.kubernetes.configuration.Constants.JOBMANAGER_RPC_PORT;
import static org.apache.flink.kubernetes.configuration.Constants.JOB_MANAGER_CONFIG_MAP_SUFFIX;
import static org.apache.flink.kubernetes.configuration.Constants.SERVICE_NAME_SUFFIX;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for kubernetes cluster descriptor.
 */
public class KubernetesClusterDescriptorTest {

	private Configuration flinkConf;

	private static final int JOB_MANAGER_MEMORY = 1024;
	private static final String JAVA_OPT = "-Xmn512";
	private static final String CONF_DIR_IN_IMAGE = "/flink/conf";
	private static final String JM_CONTAINER_NAME = "flink-jm";
	private static final String CLUSTER_ID = "flink-cluster-1";
	private static final String SERVICE_ACCOUNT = "flink";
	private KubernetesClient kubernetesClient;


	@Rule
	public KubernetesServer server = new KubernetesServer(true, true);

	private KubernetesClusterDescriptor kubernetesClusterDescriptor;

	@Before
	public void setUp() {
		flinkConf = new Configuration();
		flinkConf.setString(KubernetesConfigOptions.CLUSTER_ID, CLUSTER_ID);
		flinkConf.setString(KubernetesConfigOptions.JOB_MANAGER_CONTAINER_NAME, JM_CONTAINER_NAME);
		flinkConf.setString(CoreOptions.FLINK_JM_JVM_OPTIONS, JAVA_OPT);
		flinkConf.setString(KubernetesConfigOptions.CONF_DIR, CONF_DIR_IN_IMAGE);
		flinkConf.setString(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT, SERVICE_ACCOUNT);

		kubernetesClient = server.getClient();
		kubernetesClusterDescriptor = new KubernetesClusterDescriptor(flinkConf, "", kubernetesClient);
	}

	@Test
	public void testCreateJobManagerContainer() {
		String clusterEntryPoint = KubernetesSessionClusterEntrypoint.class.getName();
		Container container = kubernetesClusterDescriptor.createJobManagerContainer(clusterEntryPoint, JOB_MANAGER_MEMORY);

		String[] commands = new String[] {
			"/bin/bash", "-c",
			"$JAVA_HOME/bin/java " +
			"-classpath " + CONF_DIR_IN_IMAGE + ":$FLINK_CLASSPATH " + "-Xmx" + JOB_MANAGER_MEMORY + "m " +
			JAVA_OPT + " -Dlog4j.configuration=" + CONFIG_FILE_LOG4J_NAME + " " + clusterEntryPoint
		};
		assertTrue(container.getCommand().isEmpty());
		assertArrayEquals(commands, container.getArgs().toArray());
		assertEquals(JM_CONTAINER_NAME, container.getName());
		ContainerPort rpcPort = new ContainerPortBuilder()
			.withContainerPort(JobManagerOptions.PORT.defaultValue())
			.withName(JOBMANAGER_RPC_PORT)
			.withProtocol("TCP").build();
		assertTrue(container.getPorts().contains(rpcPort));
		assertEquals(JOB_MANAGER_MEMORY << 20,
			Integer.parseInt(container.getResources().getLimits().get("memory").getAmount()));
	}

	@Test
	public void testStartJobManager() {
		ClusterSpecification clusterSpecification = ClusterSpecification.fromConfiguration(flinkConf);
		String clusterEntryPoint = KubernetesSessionClusterEntrypoint.class.getName();
		Map<String, String> labels = new HashMap<>();
		labels.put("app", CLUSTER_ID);
		labels.put("component", "jobmanager");
		kubernetesClusterDescriptor.startJobManager(clusterEntryPoint, clusterSpecification,
			KubernetesClusterId.fromString(CLUSTER_ID), labels);

		// check config mapS
		ConfigMap configMap = kubernetesClient.configMaps().withName(CLUSTER_ID + JOB_MANAGER_CONFIG_MAP_SUFFIX).get();
		assertNotNull(configMap);
		assertEquals(1, configMap.getData().size());
		assertTrue(configMap.getData().containsKey(FLINK_CONF_FILENAME));

		// check replication controller
		ReplicationController rc = kubernetesClient.replicationControllers().withName(
			CLUSTER_ID + JOBMANAGER_RC_NAME_SUFFIX).get();
		assertNotNull(rc);
		assertEquals(1, rc.getSpec().getReplicas().intValue());
		assertEquals(2, rc.getSpec().getSelector().size());
		assertEquals(JM_CONTAINER_NAME, rc.getSpec().getTemplate().getSpec().getContainers().get(0).getName());
		assertEquals(FLINK_CONF_VOLUME, rc.getSpec().getTemplate().getSpec().getVolumes().get(0).getName());
		assertEquals(SERVICE_ACCOUNT, rc.getSpec().getTemplate().getSpec().getServiceAccountName());

		// check service
		Service service = kubernetesClient.services().withName(CLUSTER_ID + SERVICE_NAME_SUFFIX).get();
		assertNotNull(service);
		assertEquals(labels, service.getSpec().getSelector());
		assertEquals(3, service.getSpec().getPorts().size());

		// check owner reference
		String serviceName = service.getMetadata().getName();
		assertNotNull(configMap.getMetadata().getOwnerReferences());
		assertEquals(serviceName, configMap.getMetadata().getOwnerReferences().get(0).getName());
		assertNotNull(rc.getMetadata().getOwnerReferences().get(0));
		assertEquals(serviceName, rc.getMetadata().getOwnerReferences().get(0).getName());
	}

	@Test(timeout = 30000)
	public void testKillCluster() {
		ClusterSpecification clusterSpecification = ClusterSpecification.fromConfiguration(flinkConf);
		String clusterEntryPoint = KubernetesSessionClusterEntrypoint.class.getName();
		Map<String, String> labels = new HashMap<>();
		labels.put("app", CLUSTER_ID);
		labels.put("component", "jobmanager");
		kubernetesClusterDescriptor.startJobManager(clusterEntryPoint, clusterSpecification,
			KubernetesClusterId.fromString(CLUSTER_ID), labels);

		Service service = kubernetesClient.services().withName(CLUSTER_ID + SERVICE_NAME_SUFFIX).get();
		assertNotNull(service);

		ConfigMap configMap = kubernetesClient.configMaps().withName(CLUSTER_ID + JOB_MANAGER_CONFIG_MAP_SUFFIX).get();
		assertNotNull(configMap);

		ReplicationController rc = kubernetesClient.replicationControllers().withName(
			CLUSTER_ID + JOBMANAGER_RC_NAME_SUFFIX).get();
		assertNotNull(rc);

		kubernetesClusterDescriptor.killCluster(KubernetesClusterId.fromString(CLUSTER_ID));

		// Service should be deleted
		while (service != null) {
			service = kubernetesClient.services().withName(CLUSTER_ID + SERVICE_NAME_SUFFIX).get();
		}
	}

}
