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
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.kubernetes.KubernetesClientTestBase;
import org.apache.flink.kubernetes.KubernetesTestUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptionsInternal;
import org.apache.flink.kubernetes.configuration.KubernetesDeploymentTarget;
import org.apache.flink.kubernetes.entrypoint.KubernetesSessionClusterEntrypoint;
import org.apache.flink.kubernetes.kubeclient.decorators.ExternalServiceDecorator;
import org.apache.flink.kubernetes.kubeclient.factory.KubernetesJobManagerFactory;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.NoOpWatchCallbackHandler;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.ExceptionUtils;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.apps.Deployment;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOG4J_NAME;
import static org.apache.flink.kubernetes.utils.Constants.CONFIG_FILE_LOGBACK_NAME;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for Fabric implementation of {@link FlinkKubeClient}. */
public class Fabric8FlinkKubeClientTest extends KubernetesClientTestBase {
    private static final long TIMEOUT = 10 * 1000;
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
    public void testCreateFlinkMasterComponent() throws Exception {
        flinkKubeClient.createJobManagerComponent(this.kubernetesJobManagerSpecification);

        final List<Deployment> resultedDeployments =
                kubeClient.apps().deployments().inNamespace(NAMESPACE).list().getItems();
        assertEquals(1, resultedDeployments.size());

        final List<ConfigMap> resultedConfigMaps =
                kubeClient.configMaps().inNamespace(NAMESPACE).list().getItems();
        assertEquals(1, resultedConfigMaps.size());

        final List<Service> resultedServices =
                kubeClient.services().inNamespace(NAMESPACE).list().getItems();
        assertEquals(2, resultedServices.size());

        testOwnerReferenceSetting(resultedDeployments.get(0), resultedConfigMaps);
        testOwnerReferenceSetting(resultedDeployments.get(0), resultedServices);
    }

    private <T extends HasMetadata> void testOwnerReferenceSetting(
            HasMetadata ownerReference, List<T> resources) {
        resources.forEach(
                resource -> {
                    List<OwnerReference> ownerReferences =
                            resource.getMetadata().getOwnerReferences();
                    assertEquals(1, ownerReferences.size());
                    assertEquals(
                            ownerReference.getMetadata().getUid(), ownerReferences.get(0).getUid());
                });
    }

    @Test
    public void testCreateFlinkTaskManagerPod() throws Exception {
        this.flinkKubeClient.createJobManagerComponent(this.kubernetesJobManagerSpecification);

        final KubernetesPod kubernetesPod =
                new KubernetesPod(
                        new PodBuilder()
                                .editOrNewMetadata()
                                .withName("mock-task-manager-pod")
                                .endMetadata()
                                .editOrNewSpec()
                                .endSpec()
                                .build());
        this.flinkKubeClient.createTaskManagerPod(kubernetesPod).get();

        final Pod resultTaskManagerPod =
                this.kubeClient.pods().inNamespace(NAMESPACE).withName(TASKMANAGER_POD_NAME).get();

        assertEquals(
                this.kubeClient
                        .apps()
                        .deployments()
                        .inNamespace(NAMESPACE)
                        .list()
                        .getItems()
                        .get(0)
                        .getMetadata()
                        .getUid(),
                resultTaskManagerPod.getMetadata().getOwnerReferences().get(0).getUid());
    }

    @Test
    public void testStopPod() throws ExecutionException, InterruptedException {
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
        assertNotNull(this.kubeClient.pods().inNamespace(NAMESPACE).withName(podName).get());

        this.flinkKubeClient.stopPod(podName).get();
        assertNull(this.kubeClient.pods().inNamespace(NAMESPACE).withName(podName).get());
    }

    @Test
    public void testServiceLoadBalancerWithNoIP() {
        final String hostName = "test-host-name";
        mockExpectedServiceFromServerSide(buildExternalServiceWithLoadBalancer(hostName, ""));

        final Optional<Endpoint> resultEndpoint = flinkKubeClient.getRestEndpoint(CLUSTER_ID);

        assertThat(resultEndpoint.isPresent(), is(true));
        assertThat(resultEndpoint.get().getAddress(), is(hostName));
        assertThat(resultEndpoint.get().getPort(), is(REST_PORT));
    }

    @Test
    public void testServiceLoadBalancerEmptyHostAndIP() {
        mockExpectedServiceFromServerSide(buildExternalServiceWithLoadBalancer("", ""));

        final Optional<Endpoint> resultEndpoint = flinkKubeClient.getRestEndpoint(CLUSTER_ID);
        assertThat(resultEndpoint.isPresent(), is(false));
    }

    @Test
    public void testServiceLoadBalancerNullHostAndIP() {
        mockExpectedServiceFromServerSide(buildExternalServiceWithLoadBalancer(null, null));

        final Optional<Endpoint> resultEndpoint = flinkKubeClient.getRestEndpoint(CLUSTER_ID);
        assertThat(resultEndpoint.isPresent(), is(false));
    }

    @Test
    public void testNodePortService() {
        mockExpectedServiceFromServerSide(buildExternalServiceWithNodePort());

        final Optional<Endpoint> resultEndpoint = flinkKubeClient.getRestEndpoint(CLUSTER_ID);
        assertThat(resultEndpoint.isPresent(), is(true));
        assertThat(resultEndpoint.get().getPort(), is(NODE_PORT));
    }

    @Test
    public void testClusterIPService() {
        mockExpectedServiceFromServerSide(buildExternalServiceWithClusterIP());

        final Optional<Endpoint> resultEndpoint = flinkKubeClient.getRestEndpoint(CLUSTER_ID);
        assertThat(resultEndpoint.isPresent(), is(true));
        assertThat(
                resultEndpoint.get().getAddress(),
                is(
                        ExternalServiceDecorator.getNamespacedExternalServiceName(
                                CLUSTER_ID, NAMESPACE)));
        assertThat(resultEndpoint.get().getPort(), is(REST_PORT));
    }

    @Test
    public void testStopAndCleanupCluster() throws Exception {
        this.flinkKubeClient.createJobManagerComponent(this.kubernetesJobManagerSpecification);

        final KubernetesPod kubernetesPod =
                new KubernetesPod(
                        new PodBuilder()
                                .editOrNewMetadata()
                                .withName(TASKMANAGER_POD_NAME)
                                .endMetadata()
                                .editOrNewSpec()
                                .endSpec()
                                .build());
        this.flinkKubeClient.createTaskManagerPod(kubernetesPod).get();

        assertEquals(
                1,
                this.kubeClient
                        .apps()
                        .deployments()
                        .inNamespace(NAMESPACE)
                        .list()
                        .getItems()
                        .size());
        assertEquals(
                1, this.kubeClient.configMaps().inNamespace(NAMESPACE).list().getItems().size());
        assertEquals(2, this.kubeClient.services().inNamespace(NAMESPACE).list().getItems().size());
        assertEquals(1, this.kubeClient.pods().inNamespace(NAMESPACE).list().getItems().size());

        this.flinkKubeClient.stopAndCleanupCluster(CLUSTER_ID);
        assertTrue(
                this.kubeClient
                        .apps()
                        .deployments()
                        .inNamespace(NAMESPACE)
                        .list()
                        .getItems()
                        .isEmpty());
    }

    @Test
    public void testCreateConfigMap() throws Exception {
        final KubernetesConfigMap configMap = buildTestingConfigMap();
        this.flinkKubeClient.createConfigMap(configMap).get();
        final Optional<KubernetesConfigMap> currentOpt =
                this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME);
        assertThat(currentOpt.isPresent(), is(true));
        assertThat(
                currentOpt.get().getData().get(TESTING_CONFIG_MAP_KEY),
                is(TESTING_CONFIG_MAP_VALUE));
    }

    @Test
    public void testCreateConfigMapAlreadyExisting() throws Exception {
        final KubernetesConfigMap configMap = buildTestingConfigMap();
        this.flinkKubeClient.createConfigMap(configMap).get();

        mockCreateConfigMapAlreadyExisting(configMap.getInternalResource());
        configMap.getData().put(TESTING_CONFIG_MAP_KEY, TESTING_CONFIG_MAP_NEW_VALUE);
        try {
            this.flinkKubeClient.createConfigMap(configMap).get();
            fail("Overwrite an already existing config map should fail with an exception.");
        } catch (Exception ex) {
            final String errorMsg = "Failed to create ConfigMap " + TESTING_CONFIG_MAP_NAME;
            assertThat(ex, FlinkMatchers.containsMessage(errorMsg));
        }
        // Create failed we should still get the old value
        final Optional<KubernetesConfigMap> currentOpt =
                this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME);
        assertThat(currentOpt.isPresent(), is(true));
        assertThat(
                currentOpt.get().getData().get(TESTING_CONFIG_MAP_KEY),
                is(TESTING_CONFIG_MAP_VALUE));
    }

    @Test
    public void testDeleteConfigMapByLabels() throws Exception {
        this.flinkKubeClient.createConfigMap(buildTestingConfigMap()).get();
        assertThat(
                this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME).isPresent(), is(true));
        this.flinkKubeClient.deleteConfigMapsByLabels(TESTING_LABELS).get();
        assertThat(
                this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME).isPresent(), is(false));
    }

    @Test
    public void testDeleteConfigMapByName() throws Exception {
        this.flinkKubeClient.createConfigMap(buildTestingConfigMap()).get();
        assertThat(
                this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME).isPresent(), is(true));
        this.flinkKubeClient.deleteConfigMap(TESTING_CONFIG_MAP_NAME).get();
        assertThat(
                this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME).isPresent(), is(false));
    }

    @Test
    public void testCheckAndUpdateConfigMap() throws Exception {
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
        assertThat(updated, is(true));

        final Optional<KubernetesConfigMap> configMapOpt =
                this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME);
        assertThat(configMapOpt.isPresent(), is(true));
        assertThat(
                configMapOpt.get().getData().get(TESTING_CONFIG_MAP_KEY),
                is(TESTING_CONFIG_MAP_NEW_VALUE));
    }

    @Test
    public void testCheckAndUpdateConfigMapWithEmptyResult() throws Exception {
        this.flinkKubeClient.createConfigMap(buildTestingConfigMap());

        // Checker not pass and return empty result
        final boolean updated =
                this.flinkKubeClient
                        .checkAndUpdateConfigMap(TESTING_CONFIG_MAP_NAME, c -> Optional.empty())
                        .get();
        assertThat(updated, is(false));

        final Optional<KubernetesConfigMap> configMapOpt =
                this.flinkKubeClient.getConfigMap(TESTING_CONFIG_MAP_NAME);
        assertThat(configMapOpt.isPresent(), is(true));
        assertThat(
                configMapOpt.get().getData().get(TESTING_CONFIG_MAP_KEY),
                is(TESTING_CONFIG_MAP_VALUE));
    }

    @Test
    public void testCheckAndUpdateConfigMapWhenConfigMapNotExist() {
        try {
            final CompletableFuture<Boolean> future =
                    this.flinkKubeClient.checkAndUpdateConfigMap(
                            TESTING_CONFIG_MAP_NAME, c -> Optional.empty());
            future.get();
            fail(
                    "CheckAndUpdateConfigMap should fail with an exception when the ConfigMap does not exist.");
        } catch (Exception ex) {
            final String errMsg =
                    "Cannot retry checkAndUpdateConfigMap with configMap "
                            + TESTING_CONFIG_MAP_NAME
                            + " because it does not exist.";
            assertThat(ex, FlinkMatchers.containsMessage(errMsg));
            // Should not retry when ConfigMap does not exist.
            assertThat(
                    ex,
                    FlinkMatchers.containsMessage(
                            "Stopped retrying the operation because the error is not retryable."));
        }
    }

    @Test
    public void testCheckAndUpdateConfigMapWhenGetConfigMapFailed() throws Exception {
        final int configuredRetries =
                flinkConfig.getInteger(
                        KubernetesConfigOptions.KUBERNETES_TRANSACTIONAL_OPERATION_MAX_RETRIES);
        final KubernetesConfigMap configMap = buildTestingConfigMap();
        this.flinkKubeClient.createConfigMap(configMap).get();

        mockGetConfigMapFailed(configMap.getInternalResource());

        final int initialRequestCount = server.getRequestCount();
        try {
            this.flinkKubeClient
                    .checkAndUpdateConfigMap(
                            TESTING_CONFIG_MAP_NAME,
                            c -> {
                                throw new AssertionError(
                                        "The replace operation should have never been triggered.");
                            })
                    .get();
            fail(
                    "checkAndUpdateConfigMap should fail without a PossibleInconsistentStateException being the cause when number of retries has been exhausted.");
        } catch (Exception ex) {
            assertThat(
                    ex,
                    FlinkMatchers.containsMessage(
                            "Could not complete the "
                                    + "operation. Number of retries has been exhausted."));
            final int actualRetryCount = server.getRequestCount() - initialRequestCount;
            assertThat(actualRetryCount, is(configuredRetries + 1));
            assertThat(
                    "An error while retrieving the ConfigMap should not cause a PossibleInconsistentStateException.",
                    ExceptionUtils.findThrowable(ex, PossibleInconsistentStateException.class)
                            .isPresent(),
                    is(false));
        }
    }

    @Test
    public void testCheckAndUpdateConfigMapWhenReplaceConfigMapFailed() throws Exception {
        final int configuredRetries =
                flinkConfig.getInteger(
                        KubernetesConfigOptions.KUBERNETES_TRANSACTIONAL_OPERATION_MAX_RETRIES);
        final KubernetesConfigMap configMap = buildTestingConfigMap();
        this.flinkKubeClient.createConfigMap(configMap).get();

        mockReplaceConfigMapFailed(configMap.getInternalResource());

        final AtomicInteger retries = new AtomicInteger(0);
        try {
            this.flinkKubeClient
                    .checkAndUpdateConfigMap(
                            TESTING_CONFIG_MAP_NAME,
                            c -> {
                                retries.incrementAndGet();
                                return Optional.of(configMap);
                            })
                    .get();
            fail(
                    "checkAndUpdateConfigMap should fail due to a PossibleInconsistentStateException when number of retries has been exhausted.");
        } catch (Exception ex) {
            assertThat(
                    ex,
                    FlinkMatchers.containsMessage(
                            "Could not complete the "
                                    + "operation. Number of retries has been exhausted."));
            assertThat(retries.get(), is(configuredRetries + 1));

            assertThat(
                    "An error while replacing the ConfigMap should cause an PossibleInconsistentStateException.",
                    ExceptionUtils.findThrowable(ex, PossibleInconsistentStateException.class)
                            .isPresent(),
                    is(true));
        }
    }

    @Test
    public void testWatchConfigMaps() throws Exception {
        final String kubeConfigFile = writeKubeConfigForMockKubernetesServer();
        flinkConfig.set(KubernetesConfigOptions.KUBE_CONFIG_FILE, kubeConfigFile);

        final FlinkKubeClient realFlinkKubeClient =
                FlinkKubeClientFactory.getInstance().fromConfiguration(flinkConfig, "testing");
        realFlinkKubeClient.watchConfigMaps(CLUSTER_ID, new NoOpWatchCallbackHandler<>());
        final String path =
                "/api/v1/namespaces/"
                        + NAMESPACE
                        + "/configmaps?fieldSelector=metadata.name%3D"
                        + CLUSTER_ID
                        + "&watch=true";
        final RecordedRequest watchRequest = server.takeRequest(TIMEOUT, TimeUnit.MILLISECONDS);
        assertThat(watchRequest.getPath(), is(path));
        assertThat(watchRequest.getMethod(), is(HttpMethodWrapper.GET.toString()));
    }

    @Test
    public void testIOExecutorShouldBeShutDownWhenFlinkKubeClientClosed() {
        final ExecutorService executorService =
                Executors.newFixedThreadPool(2, new ExecutorThreadFactory("Testing-IO"));
        final FlinkKubeClient flinkKubeClient =
                new Fabric8FlinkKubeClient(flinkConfig, kubeClient, executorService);
        flinkKubeClient.close();
        assertThat(executorService.isShutdown(), is(true));
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
