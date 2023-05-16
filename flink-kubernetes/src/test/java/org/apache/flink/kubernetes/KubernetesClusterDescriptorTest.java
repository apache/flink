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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClientFactory;
import org.apache.flink.kubernetes.kubeclient.decorators.InternalServiceDecorator;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.util.concurrent.Executors;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.kubernetes.utils.Constants.ENV_FLINK_POD_IP_ADDRESS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link KubernetesClusterDescriptor}. */
class KubernetesClusterDescriptorTest extends KubernetesClientTestBase {
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

        descriptor =
                new KubernetesClusterDescriptor(
                        flinkConfig,
                        new FlinkKubeClientFactory() {
                            @Override
                            public FlinkKubeClient fromConfiguration(
                                    Configuration flinkConfig, String useCase) {
                                return new Fabric8FlinkKubeClient(
                                        flinkConfig,
                                        server.createClient().inNamespace(NAMESPACE),
                                        Executors.newDirectExecutorService());
                            }
                        });
    }

    @Test
    void testDeploySessionCluster() throws Exception {
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        final ClusterClient<String> clusterClient = deploySessionCluster().getClusterClient();
        checkClusterClient(clusterClient);
        checkUpdatedConfigAndResourceSetting();
        clusterClient.close();
    }

    @Test
    void testDeployHighAvailabilitySessionCluster() throws ClusterDeploymentException {
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
        assertThat(jmContainer.getEnv().stream().map(EnvVar::getName))
                .withFailMessage("Environment " + ENV_FLINK_POD_IP_ADDRESS + " should be set.")
                .contains(ENV_FLINK_POD_IP_ADDRESS);

        clusterClient.close();
    }

    @Test
    void testKillCluster() throws Exception {
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        deploySessionCluster();

        assertThat(kubeClient.services().list().getItems()).hasSize(2);

        descriptor.killCluster(CLUSTER_ID);

        // Mock kubernetes server do not delete the accompanying resources by gc.
        assertThat(kubeClient.apps().deployments().list().getItems()).isEmpty();
        assertThat(kubeClient.services().list().getItems()).hasSize(2);
        assertThat(kubeClient.configMaps().list().getItems()).hasSize(1);
    }

    @Test
    void testDeployApplicationCluster() {
        flinkConfig.set(
                PipelineOptions.JARS, Collections.singletonList("local:///path/of/user.jar"));
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());
        try {
            descriptor.deployApplicationCluster(clusterSpecification, appConfig);
        } catch (Exception ignored) {
        }

        mockExpectedServiceFromServerSide(loadBalancerSvc);
        final ClusterClient<String> clusterClient =
                descriptor.retrieve(CLUSTER_ID).getClusterClient();
        checkClusterClient(clusterClient);
        checkUpdatedConfigAndResourceSetting();
    }

    @Test
    void testDeployApplicationClusterWithNonLocalSchema() {
        flinkConfig.set(
                PipelineOptions.JARS, Collections.singletonList("file:///path/of/user.jar"));
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());
        assertThatThrownBy(
                        () -> descriptor.deployApplicationCluster(clusterSpecification, appConfig))
                .satisfies(
                        cause ->
                                assertThat(cause)
                                        .isInstanceOf(IllegalArgumentException.class)
                                        .hasMessageContaining(
                                                "Only \"local\" is supported as schema for application mode."));
    }

    @Test
    void testDeployApplicationClusterWithClusterAlreadyExists() {
        flinkConfig.set(
                PipelineOptions.JARS, Collections.singletonList("local:///path/of/user.jar"));
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());
        mockExpectedServiceFromServerSide(loadBalancerSvc);
        assertThatThrownBy(
                        () -> descriptor.deployApplicationCluster(clusterSpecification, appConfig))
                .satisfies(
                        cause ->
                                assertThat(cause)
                                        .isInstanceOf(ClusterDeploymentException.class)
                                        .hasMessageContaining(
                                                "The Flink cluster "
                                                        + CLUSTER_ID
                                                        + " already exists."));
    }

    @Test
    void testDeployApplicationClusterWithDeploymentTargetNotCorrectlySet() {
        flinkConfig.set(
                PipelineOptions.JARS, Collections.singletonList("local:///path/of/user.jar"));
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        assertThatThrownBy(
                        () -> descriptor.deployApplicationCluster(clusterSpecification, appConfig))
                .satisfies(
                        cause ->
                                assertThat(cause)
                                        .isInstanceOf(ClusterDeploymentException.class)
                                        .hasMessageContaining(
                                                "Expected deployment.target=kubernetes-application"));
    }

    @Test
    void testDeployApplicationClusterWithMultipleJarsSet() {
        flinkConfig.set(
                PipelineOptions.JARS,
                Arrays.asList("local:///path/of/user.jar", "local:///user2.jar"));
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());
        assertThatThrownBy(
                        () -> descriptor.deployApplicationCluster(clusterSpecification, appConfig))
                .satisfies(
                        cause ->
                                assertThat(cause)
                                        .isInstanceOf(IllegalArgumentException.class)
                                        .hasMessageContaining("Should only have one jar"));
    }

    @Test
    void testDeployApplicationClusterWithClusterIP() throws Exception {
        flinkConfig.set(
                PipelineOptions.JARS, Collections.singletonList("local:///path/of/user.jar"));
        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.APPLICATION.getName());
        flinkConfig.set(
                KubernetesConfigOptions.REST_SERVICE_EXPOSED_TYPE,
                KubernetesConfigOptions.ServiceExposedType.ClusterIP);

        final ClusterClient<String> clusterClient =
                descriptor
                        .deployApplicationCluster(clusterSpecification, appConfig)
                        .getClusterClient();

        final String address = CLUSTER_ID + Constants.FLINK_REST_SERVICE_SUFFIX + "." + NAMESPACE;
        final int port = flinkConfig.get(RestOptions.PORT);
        assertThat(clusterClient.getWebInterfaceURL())
                .isEqualTo(String.format("http://%s:%d", address, port));
    }

    private ClusterClientProvider<String> deploySessionCluster() throws ClusterDeploymentException {
        mockExpectedServiceFromServerSide(loadBalancerSvc);
        return descriptor.deploySessionCluster(clusterSpecification);
    }

    private void checkClusterClient(ClusterClient<String> clusterClient) {
        assertThat(clusterClient.getClusterId()).isEqualTo(CLUSTER_ID);
        // Both HA and non-HA mode, the web interface should always be the Kubernetes exposed
        // service address.
        assertThat(clusterClient.getWebInterfaceURL())
                .isEqualTo(String.format("http://%s:%d", MOCK_SERVICE_IP, REST_PORT));
    }

    private void checkUpdatedConfigAndResourceSetting() {
        // Check updated flink config options
        assertThat(flinkConfig.getString(BlobServerOptions.PORT))
                .isEqualTo(String.valueOf(Constants.BLOB_SERVER_PORT));
        assertThat(flinkConfig.getString(TaskManagerOptions.RPC_PORT))
                .isEqualTo(String.valueOf(Constants.TASK_MANAGER_RPC_PORT));
        assertThat(flinkConfig.getString(JobManagerOptions.ADDRESS))
                .isEqualTo(
                        InternalServiceDecorator.getNamespacedInternalServiceName(
                                CLUSTER_ID, NAMESPACE));

        final Deployment jmDeployment = kubeClient.apps().deployments().list().getItems().get(0);

        final Container jmContainer =
                jmDeployment.getSpec().getTemplate().getSpec().getContainers().get(0);

        assertThat(
                        jmContainer
                                .getResources()
                                .getRequests()
                                .get(Constants.RESOURCE_NAME_MEMORY)
                                .getAmount())
                .isEqualTo(String.valueOf(clusterSpecification.getMasterMemoryMB()));
        assertThat(
                        jmContainer
                                .getResources()
                                .getLimits()
                                .get(Constants.RESOURCE_NAME_MEMORY)
                                .getAmount())
                .isEqualTo(String.valueOf(clusterSpecification.getMasterMemoryMB()));
    }
}
