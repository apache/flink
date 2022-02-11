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

import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.kubeclient.decorators.InternalServiceDecorator;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;
import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_POD_IP_ADDRESS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link KubernetesClusterDescriptor}. */
public class KubernetesClusterDescriptorTest extends KubernetesClientTestBase {
    private static final String MOCK_SERVICE_HOST_NAME = "mock-host-name-of-service";
    private static final String MOCK_SERVICE_IP = "192.168.0.1";

    private final ClusterSpecification clusterSpecification =
            new ClusterSpecification.ClusterSpecificationBuilder().createClusterSpecification();

    private final Service loadBalancerSvc =
            buildExternalServiceWithLoadBalancer(MOCK_SERVICE_HOST_NAME, MOCK_SERVICE_IP);

    private final ApplicationConfiguration appConfig =
            new ApplicationConfiguration(new String[0], null);

    private KubernetesClusterDescriptor descriptor;

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        descriptor = new KubernetesClusterDescriptor(flinkConfig, flinkKubeClient);
    }

    @Test
    public void testDeploySessionCluster() throws Exception {
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        final ClusterClient<String> clusterClient = deploySessionCluster().getClusterClient();
        checkClusterClient(clusterClient);
        checkUpdatedConfigAndResourceSetting();
        clusterClient.close();
    }

    @Test
    public void testDeployHighAvailabilitySessionCluster() throws ClusterDeploymentException {
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        flinkConfig.setString(
                HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.toString());
        final ClusterClient<String> clusterClient = deploySessionCluster().getClusterClient();
        checkClusterClient(clusterClient);

        final Container jmContainer =
                kubeClient
                        .apps()
                        .deployments()
                        .list()
                        .getItems()
                        .get(0)
                        .getSpec()
                        .getTemplate()
                        .getSpec()
                        .getContainers()
                        .get(0);
        assertTrue(
                "Environment " + ENV_FLINK_POD_IP_ADDRESS + " should be set.",
                jmContainer.getEnv().stream()
                        .map(EnvVar::getName)
                        .collect(Collectors.toList())
                        .contains(ENV_FLINK_POD_IP_ADDRESS));

        clusterClient.close();
    }

    @Test
    public void testKillCluster() throws Exception {
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        deploySessionCluster();

        assertEquals(2, kubeClient.services().list().getItems().size());

        descriptor.killCluster(CLUSTER_ID);

        // Mock kubernetes server do not delete the accompanying resources by gc.
        assertTrue(kubeClient.apps().deployments().list().getItems().isEmpty());
        assertEquals(2, kubeClient.services().list().getItems().size());
        assertEquals(1, kubeClient.configMaps().list().getItems().size());
    }

    @Test
    public void testDeployApplicationCluster() {
        flinkConfig.set(
                PipelineOptions.JARS, Collections.singletonList("local:///path/of/user.jar"));
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());
        try {
            descriptor.deployApplicationCluster(clusterSpecification, appConfig, false);
        } catch (Exception ignored) {
        }

        mockExpectedServiceFromServerSide(loadBalancerSvc);
        final ClusterClient<String> clusterClient =
                descriptor.retrieve(CLUSTER_ID).getClusterClient();
        checkClusterClient(clusterClient);
        checkUpdatedConfigAndResourceSetting();
    }

    @Test
    public void testDeployApplicationClusterWithNonLocalSchema() {
        flinkConfig.set(
                PipelineOptions.JARS, Collections.singletonList("file:///path/of/user.jar"));
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());
        assertThrows(
                "Only \"local\" is supported as schema for application mode.",
                IllegalArgumentException.class,
                () -> descriptor.deployApplicationCluster(clusterSpecification, appConfig, false));
    }

    @Test
    public void testDeployApplicationClusterWithClusterAlreadyExists() {
        flinkConfig.set(
                PipelineOptions.JARS, Collections.singletonList("local:///path/of/user.jar"));
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());
        mockExpectedServiceFromServerSide(loadBalancerSvc);
        assertThrows(
                "The Flink cluster " + CLUSTER_ID + " already exists.",
                ClusterDeploymentException.class,
                () -> descriptor.deployApplicationCluster(clusterSpecification, appConfig, false));
    }

    @Test
    public void testDeployApplicationClusterWithDeploymentTargetNotCorrectlySet() {
        flinkConfig.set(
                PipelineOptions.JARS, Collections.singletonList("local:///path/of/user.jar"));
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        assertThrows(
                "Expected deployment.target=kubernetes-application",
                ClusterDeploymentException.class,
                () -> descriptor.deployApplicationCluster(clusterSpecification, appConfig, false));
    }

    @Test
    public void testDeployApplicationClusterWithMultipleJarsSet() {
        flinkConfig.set(
                PipelineOptions.JARS,
                Arrays.asList("local:///path/of/user.jar", "local:///user2.jar"));
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());
        assertThrows(
                "Should only have one jar",
                IllegalArgumentException.class,
                () -> descriptor.deployApplicationCluster(clusterSpecification, appConfig, false));
    }

    @Test
    public void testDeployApplicationClusterWithClusterIP() throws Exception {
        flinkConfig.set(
                PipelineOptions.JARS, Collections.singletonList("local:///path/of/user.jar"));
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());
        flinkConfig.set(
                KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE,
                KubernetesConfigOptions.ServiceExposedType.ClusterIP);

        final ClusterClient<String> clusterClient =
                descriptor
                        .deployApplicationCluster(clusterSpecification, appConfig, false)
                        .getClusterClient();

        final String address = CLUSTER_ID + Constants.FLINK_REST_SERVICE_SUFFIX + "." + NAMESPACE;
        final int port = flinkConfig.get(RestOptions.PORT);
        assertThat(
                clusterClient.getWebInterfaceURL(),
                is(String.format("http://%s:%d", address, port)));
    }

    private ClusterClientProvider<String> deploySessionCluster() throws ClusterDeploymentException {
        mockExpectedServiceFromServerSide(loadBalancerSvc);
        return descriptor.deploySessionCluster(clusterSpecification);
    }

    private void checkClusterClient(ClusterClient<String> clusterClient) {
        assertEquals(CLUSTER_ID, clusterClient.getClusterId());
        // Both HA and non-HA mode, the web interface should always be the Kubernetes exposed
        // service address.
        assertEquals(
                String.format("http://%s:%d", MOCK_SERVICE_IP, REST_PORT),
                clusterClient.getWebInterfaceURL());
    }

    private void checkUpdatedConfigAndResourceSetting() {
        // Check updated flink config options
        assertEquals(
                String.valueOf(Constants.BLOB_SERVER_PORT),
                flinkConfig.getString(BlobServerOptions.PORT));
        assertEquals(
                String.valueOf(Constants.TASK_MANAGER_RPC_PORT),
                flinkConfig.getString(TaskManagerOptions.RPC_PORT));
        assertEquals(
                InternalServiceDecorator.getNamespacedInternalServiceName(CLUSTER_ID, NAMESPACE),
                flinkConfig.getString(JobManagerOptions.ADDRESS));

        final Deployment jmDeployment = kubeClient.apps().deployments().list().getItems().get(0);

        final Container jmContainer =
                jmDeployment.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertEquals(
                String.valueOf(clusterSpecification.getMasterMemoryMB()),
                jmContainer
                        .getResources()
                        .getRequests()
                        .get(Constants.RESOURCE_NAME_MEMORY)
                        .getAmount());
        assertEquals(
                String.valueOf(clusterSpecification.getMasterMemoryMB()),
                jmContainer
                        .getResources()
                        .getLimits()
                        .get(Constants.RESOURCE_NAME_MEMORY)
                        .getAmount());
    }
}
