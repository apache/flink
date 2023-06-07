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

import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.kubernetes.KubernetesClientTestBase;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.decorators.InternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.factory.KubernetesJobManagerFactory;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.utils.Constants;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.client.Watcher.Action;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.core.testutils.FlinkAssertions.assertThatChainOfCauses;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.apache.flink.kubernetes.utils.Constants.KUBERNETES_ZERO_RESOURCE_VERSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/** Tests for Fabric implementation of {@link FlinkKubeClient}. */
public class Fabric8FlinkKubeClientTest extends KubernetesClientTestBase {
    private static final int RPC_PORT = 7123;
    private static final int BLOB_SERVER_PORT = 8346;

    private static final double JOB_MANAGER_CPU = 2.0;
    private static final int JOB_MANAGER_MEMORY = 768;

    private static final String SERVICE_ACCOUNT_NAME = "service-test";

    private static final String TASKMANAGER_POD_NAME = "mock-task-manager-pod";

    private static final String TESTING_CONFIG_MAP_NAME = "test-config-map";
    private static final String TESTING_CONFIG_MAP_KEY = "test-config-map-key";
    private static final String TESTING_CONFIG_MAP_VALUE = "test-config-map-value";
    private static final String TESTING_CONFIG_MAP_NEW_VALUE = "test-config-map-new-value";

    private static final Map<String, String> TESTING_LABELS =
            new HashMap<String, String>() {
                {
                    put("label1", "value1");
                    put("label2", "value2");
                }
            };

    private static final String ENTRY_POINT_CLASS =
            KubernetesSessionClusterEntrypoint.class.getCanonicalName();

    private KubernetesJobManagerSpecification kubernetesJobManagerSpecification;

    @Override
    protected void setupFlinkConfig() {
        super.setupFlinkConfig();

        flinkConfig.set(DeploymentOptions.TARGET, KubernetesDeploymentTarget.SESSION.getName());
        flinkConfig.set(
                KubernetesConfigOptions.CONTAINER_IMAGE_PULL_POLICY, CONTAINER_IMAGE_PULL_POLICY);
        flinkConfig.set(KubernetesConfigOptionsInternal.ENTRY_POINT_CLASS, ENTRY_POINT_CLASS);
        flinkConfig.set(RestOptions.PORT, REST_PORT);
        flinkConfig.set(JobManagerOptions.PORT, RPC_PORT);
        flinkConfig.set(BlobServerOptions.PORT, Integer.toString(BLOB_SERVER_PORT));
        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_CPU, JOB_MANAGER_CPU);
        flinkConfig.set(KubernetesConfigOptions.JOB_MANAGER_SERVICE_ACCOUNT, SERVICE_ACCOUNT_NAME);
    }

    @Override
    protected void onSetup() throws Exception {
        super.onSetup();

        KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOGBACK_NAME);
        KubernetesTestUtils.createTemporyFile("some data", flinkConfDir, CONFIG_FILE_LOG4J_NAME);

        final ClusterSpecification clusterSpecification =
                new ClusterSpecification.ClusterSpecificationBuilder()
                        .setMasterMemoryMB(JOB_MANAGER_MEMORY)
                        .setTaskManagerMemoryMB(1000)
                        .setSlotsPerTaskManager(3)
                        .createClusterSpecification();

        final KubernetesJobManagerParameters kubernetesJobManagerParameters =
                new KubernetesJobManagerParameters(flinkConfig, clusterSpecification);
        this.kubernetesJobManagerSpecification =
                KubernetesJobManagerFactory.buildKubernetesJobManagerSpecification(
                        new FlinkPod.Builder().build(), kubernetesJobManagerParameters);
    }

    @Test
    void testCreateFlinkMasterComponent() throws Exception {
        flinkKubeClient.createJobManagerComponent(this.kubernetesJobManagerSpecification);

        final List<Deployment> resultedDeployments =
                kubeClient.apps().deployments().inNamespace(NAMESPACE).list().getItems();
        assertThat(resultedDeployments).hasSize(1);

        final List<ConfigMap> resultedConfigMaps =
                kubeClient.configMaps().inNamespace(NAMESPACE).list().getItems();
        assertThat(resultedConfigMaps).hasSize(1);

        final List<Service> resultedServices =
                kubeClient.services().inNamespace(NAMESPACE).list().getItems();
        assertThat(resultedServices).hasSize(2);

        testOwnerReferenceSetting(resultedDeployments.get(0), resultedConfigMaps);
        testOwnerReferenceSetting(resultedDeployments.get(0), resultedServices);
    }

    private <T extends HasMetadata> void testOwnerReferenceSetting(
            HasMetadata ownerReference, List<T> resources) {
        resources.forEach(
                resource -> {
                    List<OwnerReference> ownerReferences =
                            resource.getMetadata().getOwnerReferences();
                    assertThat(ownerReferences).hasSize(1);
                    assertThat(ownerReferences.get(0).getUid())
                            .isEqualTo(ownerReference.getMetadata().getUid());
                });
    }

    @Test
    void testUpdateRestServicePort() throws Exception {
        this.flinkKubeClient.createJobManagerComponent(this.kubernetesJobManagerSpecification);

        final int expectedRestPort = 9081;
        final String restServiceName = ExternalServiceDecorator.getExternalServiceName(CLUSTER_ID);
        flinkKubeClient
                .updateServiceTargetPort(
                        restServiceName, Constants.REST_PORT_NAME, expectedRestPort)
                .get();
        final int updatedRestPort =
                getServiceTargetPort(
                        CLUSTER_ID + Constants.FLINK_REST_SERVICE_SUFFIX, Constants.REST_PORT_NAME);
        assertThat(updatedRestPort).isEqualTo(expectedRestPort);
    }

    @Test
    void testUpdateInternalServicePort() throws Exception {
        this.flinkKubeClient.createJobManagerComponent(this.kubernetesJobManagerSpecification);

        final int expectedBlobPort = 9082;
        final String internalServiceName =
                InternalServiceDecorator.getInternalServiceName(CLUSTER_ID);
        flinkKubeClient
                .updateServiceTargetPort(
                        internalServiceName, Constants.BLOB_SERVER_PORT_NAME, expectedBlobPort)
                .get();
        final int updatedBlobPort =
                getServiceTargetPort(CLUSTER_ID, Constants.BLOB_SERVER_PORT_NAME);
        assertThat(updatedBlobPort).isEqualTo(expectedBlobPort);
    }

    private int getServiceTargetPort(String serviceName, String portName) {
        final List<Integer> ports =
                kubeClient.services().withName(serviceName).get().getSpec().getPorts().stream()
                        .filter(servicePort -> servicePort.getName().equalsIgnoreCase(portName))
                        .map(servicePort -> servicePort.getTargetPort().getIntVal())
                        .collect(Collectors.toList());
        assertThat(ports).hasSize(1);
        return ports.get(0);
    }

    @Test
    void testCreateFlinkTaskManagerPod() throws Exception {
        this.flinkKubeClient.createJobManagerComponent(this.kubernetesJobManagerSpecification);

        final KubernetesPod kubernetesPod = buildKubernetesPod("mock-task-manager-pod");
        this.flinkKubeClient.createTaskManagerPod(kubernetesPod).get();

        final Pod resultTaskManagerPod =
                this.kubeClient.pods().inNamespace(NAMESPACE).withName(TASKMANAGER_POD_NAME).get();

        assertThat(resultTaskManagerPod.getMetadata().getOwnerReferences().get(0).getUid())
                .isEqualTo(
                        this.kubeClient
                                .apps()
                                .deployments()
                                .inNamespace(NAMESPACE)
                                .list()
                                .getItems()
                                .get(0)
                                .getMetadata()
                                .getUid());
    }

    @Test
    void testCreateTwoTaskManagerPods() throws Exception {
        flinkKubeClient.createJobManagerComponent(this.kubernetesJobManagerSpecification);
        flinkKubeClient.createTaskManagerPod(buildKubernetesPod("mock-task-manager-pod1")).get();
        mockGetDeploymentWithError();
        try {
            flinkKubeClient
                    .createTaskManagerPod(buildKubernetesPod("mock-task-manager-pod2"))
                    .get();
        } catch (Exception e) {
            fail("should only get the master deployment once");
        }
    }

    private KubernetesPod buildKubernetesPod(String name) {
        return new KubernetesPod(
                new PodBuilder()
                        .editOrNewMetadata()
                        .withName(name)
                        .endMetadata()
                        .editOrNewSpec()
                        .endSpec()
                        .build());
    }

    @Test
    void testStopPod() throws ExecutionException, InterruptedException {
        final String podName = "pod-for-delete";
        final Pod pod =
                new PodBuilder()
                        .editOrNewMetadata()
                        .withName(podName)
                        .endMetadata()
                        .editOrNewSpec()
                        .endSpec()
                        .build();

        this.kubeClient.pods().inNamespace(NAMESPACE).create(pod);
        assertThat(this.kubeClient.pods().inNamespace(NAMESPACE).withName(podName).get())
                .isNotNull();

        this.flinkKubeClient.stopPod(podName).get();
        assertThat(this.kubeClient.pods().inNamespace(NAMESPACE).withName(podName).get()).isNull();
    }

    @Test
    void testGetPodsWithLabels() {
        final String podName = "pod-with-labels";
        final Pod pod =
                new PodBuilder()
                        .editOrNewMetadata()
                        .withName(podName)
                        .withLabels(TESTING_LABELS)
                        .endMetadata()
                        .editOrNewSpec()
                        .endSpec()
                        .build();
        this.kubeClient.pods().inNamespace(NAMESPACE).create(pod);
        List<KubernetesPod> kubernetesPods = this.flinkKubeClient.getPodsWithLabels(TESTING_LABELS);
        assertThat(kubernetesPods)
                .satisfiesExactly(
                        kubernetesPod -> assertThat(kubernetesPod.getName()).isEqualTo(podName));
    }

    @Test
    void testServiceLoadBalancerWithNoIP() {
        final String hostName = "test-host-name";
        mockExpectedServiceFromServerSide(buildExternalServiceWithLoadBalancer(hostName, ""));

        final Optional<Endpoint> resultEndpoint = flinkKubeClient.getRestEndpoint(CLUSTER_ID);

        assertThat(resultEndpoint).isPresent();
        assertThat(resultEndpoint.get().getAddress()).isEqualTo(hostName);
        assertThat(resultEndpoint.get().getPort()).isEqualTo(REST_PORT);
    }

    @Test
    void testServiceLoadBalancerEmptyHostAndIP() {
        mockExpectedServiceFromServerSide(buildExternalServiceWithLoadBalancer("", ""));

        final Optional<Endpoint> resultEndpoint = flinkKubeClient.getRestEndpoint(CLUSTER_ID);
        assertThat(resultEndpoint).isNotPresent();
    }

    @Test
    void testServiceLoadBalancerNullHostAndIP() {
        mockExpectedServiceFromServerSide(buildExternalServiceWithLoadBalancer(null, null));

        final Optional<Endpoint> resultEndpoint = flinkKubeClient.getRestEndpoint(CLUSTER_ID);
        assertThat(resultEndpoint).isNotPresent();
    }

    @Test
    void testNodePortServiceWithInternalIP() {
        testNodePortService(KubernetesConfigOptions.NodePortAddressType.InternalIP);
    }

    @Test
    void testNodePortServiceWithExternalIP() {
        testNodePortService(KubernetesConfigOptions.NodePortAddressType.ExternalIP);
    }

    private void testNodePortService(KubernetesConfigOptions.NodePortAddressType addressType) {
        flinkConfig.set(
                KubernetesConfigOptions.REST_SERVICE_EXPOSED_NODE_PORT_ADDRESS_TYPE, addressType);
        final List<String> internalAddresses =
                Arrays.asList(
                        "InternalIP:10.0.0.1:true",
                        "InternalIP:10.0.0.2:false",
                        "InternalIP:10.0.0.3: ");
        final List<String> externalAddresses =
                Arrays.asList(
                        "ExternalIP:7.7.7.7:true",
                        "ExternalIP:8.8.8.8:false",
                        "ExternalIP:9.9.9.9: ");
        final List<String> addresses = new ArrayList<>();
        addresses.addAll(internalAddresses);
        addresses.addAll(externalAddresses);
        mockExpectedServiceFromServerSide(buildExternalServiceWithNodePort());
        mockExpectedNodesFromServerSide(addresses);
        try (final Fabric8FlinkKubeClient localClient =
                new Fabric8FlinkKubeClient(
                        flinkConfig,
                        kubeClient,
                        org.apache.flink.util.concurrent.Executors.newDirectExecutorService())) {
            final Optional<Endpoint> resultEndpoint = localClient.getRestEndpoint(CLUSTER_ID);
            assertThat(resultEndpoint).isPresent();
            final List<String> expectedIps;
            switch (addressType) {
                case InternalIP:
                    expectedIps =
                            internalAddresses.stream()
                                    .filter(s -> !"true".equals(s.split(":")[2]))
                                    .map(s -> s.split(":")[1])
                                    .collect(Collectors.toList());
                    break;
                case ExternalIP:
                    expectedIps =
                            externalAddresses.stream()
                                    .filter(s -> !"true".equals(s.split(":")[2]))
                                    .map(s -> s.split(":")[1])
                                    .collect(Collectors.toList());
                    break;
                default:
                    throw new IllegalArgumentException(
                            String.format("Unexpected address type %s.", addressType));
            }
            assertThat(expectedIps.size()).isEqualTo(2);
            assertThat(resultEndpoint.get().getAddress()).isIn(expectedIps);
            assertThat(resultEndpoint.get().getPort()).isEqualTo(NODE_PORT);
        }
    }

    @Test
    void testNodePortServiceWithNoMatchingIP() {
        mockExpectedServiceFromServerSide(buildExternalServiceWithNodePort());
        assertThat(flinkKubeClient.getRestEndpoint(CLUSTER_ID)).isNotPresent();
    }

    @Test
    void testClusterIPService() {
        mockExpectedServiceFromServerSide(buildExternalServiceWithClusterIP());

        final Optional<Endpoint> resultEndpoint = flinkKubeClient.getRestEndpoint(CLUSTER_ID);
        assertThat(resultEndpoint).isPresent();
        assertThat(resultEndpoint.get().getAddress())
                .isEqualTo(
                        ExternalServiceDecorator.getNamespacedExternalServiceName(
                                CLUSTER_ID, NAMESPACE));
        assertThat(resultEndpoint.get().getPort()).isEqualTo(REST_PORT);
    }

    @Test
    void testStopAndCleanupCluster() throws Exception {
        this.flinkKubeClient.createJobManagerComponent(this.kubernetesJobManagerSpecification);

        final KubernetesPod kubernetesPod = buildKubernetesPod(TASKMANAGER_POD_NAME);
        this.flinkKubeClient.createTaskManagerPod(kubernetesPod).get();

        assertThat(
                        this.kubeClient
                                .apps()
                                .deployments()
                                .inNamespace(NAMESPACE)
                                .list()
                                .getItems()
                                .size())
                .isEqualTo(1);
        assertThat(this.kubeClient.configMaps().inNamespace(NAMESPACE).list().getItems().size())
                .isEqualTo(1);
        assertThat(this.kubeClient.services().inNamespace(NAMESPACE).list().getItems()).hasSize(2);
        assertThat(this.kubeClient.pods().inNamespace(NAMESPACE).list().getItems()).hasSize(1);

        this.flinkKubeClient.stopAndCleanupCluster(CLUSTER_ID);
        assertThat(this.kubeClient.apps().deployments().inNamespace(NAMESPACE).list().getItems())
                .isEmpty();
    }

    @Test
    void testWatchPodsAndDoCallback() throws Exception {
        mockPodEventWithLabels(
                NAMESPACE, TASKMANAGER_POD_NAME, KUBERNETES_ZERO_RESOURCE_VERSION, TESTING_LABELS);
        // the count latch for events.
        CompletableFuture<Action> podAddedAction = new CompletableFuture();
        CompletableFuture<Action> podDeletedAction = new CompletableFuture();
        CompletableFuture<Action> podModifiedAction = new CompletableFuture();
        TestingWatchCallbackHandler<KubernetesPod> watchCallbackHandler =
                TestingWatchCallbackHandler.<KubernetesPod>builder()
                        .setOnAddedConsumer((ignore) -> podAddedAction.complete(Action.ADDED))
                        .setOnDeletedConsumer((ignore) -> podDeletedAction.complete(Action.DELETED))
                        .setOnModifiedConsumer(
                                (ignore) -> podModifiedAction.complete(Action.MODIFIED))
                        .build();
        this.flinkKubeClient.watchPodsAndDoCallback(TESTING_LABELS, watchCallbackHandler);
        assertThat(podAddedAction.get()).isEqualTo(Action.ADDED);
        assertThat(podDeletedAction.get()).isEqualTo(Action.DELETED);
        assertThat(podModifiedAction.get()).isEqualTo(Action.MODIFIED);
    }

    @Test
    void testCreateConfigMap() throws Exception {
        final KubernetesConfigMap configMap = buildTestingConfigMap();
        this.flinkKubeClient.createConfigMap(configMap).get();
        final Optional<KubernetesConfigMap> currentOpt =
                this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME);
        assertThat(currentOpt)
                .hasValueSatisfying(
                        map ->
                                assertThat(map.getData())
                                        .containsEntry(
                                                TESTING_CONFIG_MAP_KEY, TESTING_CONFIG_MAP_VALUE));
    }

    @Test
    void testCreateConfigMapAlreadyExisting() throws Exception {
        final KubernetesConfigMap configMap = buildTestingConfigMap();
        this.flinkKubeClient.createConfigMap(configMap).get();

        mockCreateConfigMapAlreadyExisting(configMap.getInternalResource());
        configMap.getData().put(TESTING_CONFIG_MAP_KEY, TESTING_CONFIG_MAP_NEW_VALUE);

        final String errorMsg = "Failed to create ConfigMap " + TESTING_CONFIG_MAP_NAME;
        assertThatThrownBy(
                        () -> this.flinkKubeClient.createConfigMap(configMap).get(),
                        "Overwrite an already existing config map should fail with an exception.")
                .satisfies(anyCauseMatches(errorMsg));

        // Create failed we should still get the old value
        final Optional<KubernetesConfigMap> currentOpt =
                this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME);
        assertThat(currentOpt).isPresent();
        assertThat(currentOpt.get().getData())
                .containsEntry(TESTING_CONFIG_MAP_KEY, TESTING_CONFIG_MAP_VALUE);
    }

    @Test
    void testDeleteConfigMapByLabels() throws Exception {
        this.flinkKubeClient.createConfigMap(buildTestingConfigMap()).get();
        assertThat(this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME)).isPresent();
        this.flinkKubeClient.deleteConfigMapsByLabels(TESTING_LABELS).get();
        assertThat(this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME)).isNotPresent();
    }

    @Test
    void testDeleteNotExistingConfigMapByLabels() throws Exception {
        assertThat(this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME)).isNotPresent();
        this.flinkKubeClient.deleteConfigMapsByLabels(TESTING_LABELS).get();
        assertThat(this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME)).isNotPresent();
    }

    @Test
    void testDeleteConfigMapByName() throws Exception {
        this.flinkKubeClient.createConfigMap(buildTestingConfigMap()).get();
        assertThat(this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME)).isPresent();
        this.flinkKubeClient.deleteConfigMap(TESTING_CONFIG_MAP_NAME).get();
        assertThat(this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME)).isNotPresent();
    }

    @Test
    void testDeleteNotExistingConfigMapByName() throws Exception {
        assertThat(this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME)).isNotPresent();
        this.flinkKubeClient.deleteConfigMap(TESTING_CONFIG_MAP_NAME).get();
        assertThat(this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME)).isNotPresent();
    }

    @Test
    void testCheckAndUpdateConfigMap() throws Exception {
        this.flinkKubeClient.createConfigMap(buildTestingConfigMap());

        // Checker pass
        final boolean updated =
                this.flinkKubeClient
                        .checkAndUpdateConfigMap(
                                TESTING_CONFIG_MAP_NAME,
                                c -> {
                                    c.getData()
                                            .put(
                                                    TESTING_CONFIG_MAP_KEY,
                                                    TESTING_CONFIG_MAP_NEW_VALUE);
                                    return Optional.of(c);
                                })
                        .get();
        assertThat(updated).isTrue();

        final Optional<KubernetesConfigMap> configMapOpt =
                this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME);
        assertThat(configMapOpt).isPresent();
        assertThat(configMapOpt.get().getData())
                .containsEntry(TESTING_CONFIG_MAP_KEY, TESTING_CONFIG_MAP_NEW_VALUE);
    }

    @Test
    void testCheckAndUpdateConfigMapWithEmptyResult() throws Exception {
        this.flinkKubeClient.createConfigMap(buildTestingConfigMap());

        // Checker not pass and return empty result
        final boolean updated =
                this.flinkKubeClient
                        .checkAndUpdateConfigMap(TESTING_CONFIG_MAP_NAME, c -> Optional.empty())
                        .get();
        assertThat(updated).isFalse();

        final Optional<KubernetesConfigMap> configMapOpt =
                this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME);
        assertThat(configMapOpt).isPresent();
        assertThat(configMapOpt.get().getData())
                .containsEntry(TESTING_CONFIG_MAP_KEY, TESTING_CONFIG_MAP_VALUE);
    }

    @Test
    void testCheckAndUpdateConfigMapWhenConfigMapNotExist() {
        final String errMsg =
                "Cannot retry checkAndUpdateConfigMap with configMap "
                        + TESTING_CONFIG_MAP_NAME
                        + " because it does not exist.";
        assertThatThrownBy(
                        () ->
                                this.flinkKubeClient
                                        .checkAndUpdateConfigMap(
                                                TESTING_CONFIG_MAP_NAME, c -> Optional.empty())
                                        .get(),
                        "CheckAndUpdateConfigMap should fail with an exception when the ConfigMap does not exist.")
                .satisfies(anyCauseMatches(errMsg))
                .satisfies(
                        // Should not retry when ConfigMap does not exist.
                        anyCauseMatches(
                                "Stopped retrying the operation because the error is not retryable."));
    }

    @Test
    void testCheckAndUpdateConfigMapWhenGetConfigMapFailed() throws Exception {
        final int configuredRetries =
                flinkConfig.getInteger(
                        KubernetesConfigOptions.KUBERNETES_TRANSACTIONAL_OPERATION_MAX_RETRIES);
        final KubernetesConfigMap configMap = buildTestingConfigMap();
        this.flinkKubeClient.createConfigMap(configMap).get();
        kubeClient.getConfiguration().setRequestRetryBackoffLimit(0);

        mockGetConfigMapFailed(configMap.getInternalResource());

        final int initialRequestCount = server.getRequestCount();
        assertThatThrownBy(
                        () ->
                                this.flinkKubeClient
                                        .checkAndUpdateConfigMap(
                                                TESTING_CONFIG_MAP_NAME,
                                                c -> {
                                                    throw new AssertionError(
                                                            "The replace operation should have never been triggered.");
                                                })
                                        .get(),
                        "checkAndUpdateConfigMap should fail without a PossibleInconsistentStateException being the cause when number of retries has been exhausted.")
                .satisfies(
                        anyCauseMatches(
                                "Could not complete the operation. Number of retries has been exhausted."))
                .satisfies(
                        cause ->
                                assertThatChainOfCauses(cause)
                                        .withFailMessage(
                                                "An error while retrieving the ConfigMap should not cause a PossibleInconsistentStateException.")
                                        .isNotOfAnyClassIn(
                                                PossibleInconsistentStateException.class));
        final int actualRetryCount = server.getRequestCount() - initialRequestCount;
        assertThat(actualRetryCount).isEqualTo(configuredRetries + 1);
    }

    @Test
    void testCheckAndUpdateConfigMapWhenReplaceConfigMapFailed() throws Exception {
        final int configuredRetries =
                flinkConfig.getInteger(
                        KubernetesConfigOptions.KUBERNETES_TRANSACTIONAL_OPERATION_MAX_RETRIES);
        final KubernetesConfigMap configMap = buildTestingConfigMap();
        this.flinkKubeClient.createConfigMap(configMap).get();

        mockReplaceConfigMapFailed(configMap.getInternalResource());

        final AtomicInteger retries = new AtomicInteger(0);
        assertThatThrownBy(
                        () ->
                                this.flinkKubeClient
                                        .checkAndUpdateConfigMap(
                                                TESTING_CONFIG_MAP_NAME,
                                                c -> {
                                                    retries.incrementAndGet();
                                                    return Optional.of(configMap);
                                                })
                                        .get(),
                        "checkAndUpdateConfigMap should fail due to a PossibleInconsistentStateException when number of retries has been exhausted.")
                .satisfies(
                        anyCauseMatches(
                                "Could not complete the "
                                        + "operation. Number of retries has been exhausted."))
                .satisfies(anyCauseMatches(PossibleInconsistentStateException.class));
        assertThat(retries.get()).isEqualTo(configuredRetries + 1);
    }

    @Test
    void testIOExecutorShouldBeShutDownWhenFlinkKubeClientClosed() {
        final ExecutorService executorService =
                Executors.newFixedThreadPool(2, new ExecutorThreadFactory("Testing-IO"));
        final FlinkKubeClient flinkKubeClient =
                new Fabric8FlinkKubeClient(flinkConfig, kubeClient, executorService);
        flinkKubeClient.close();
        assertThat(executorService.isShutdown()).isTrue();
    }

    private KubernetesConfigMap buildTestingConfigMap() {
        final Map<String, String> data = new HashMap<>();
        data.put(TESTING_CONFIG_MAP_KEY, TESTING_CONFIG_MAP_VALUE);
        return new KubernetesConfigMap(
                new ConfigMapBuilder()
                        .withNewMetadata()
                        .withName(TESTING_CONFIG_MAP_NAME)
                        .withLabels(TESTING_LABELS)
                        .withNamespace(NAMESPACE)
                        .endMetadata()
                        .withData(data)
                        .build());
    }
}
