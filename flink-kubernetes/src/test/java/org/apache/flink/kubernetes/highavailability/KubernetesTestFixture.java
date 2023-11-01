/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesConfigMapSharedWatcher;
import org.apache.flink.kubernetes.kubeclient.TestingFlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesException;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.concurrent.FutureUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector.LEADER_ANNOTATION_KEY;
import static org.apache.flink.kubernetes.utils.Constants.LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY;
import static org.assertj.core.api.Assertions.assertThat;

/** Test fixture for Kubernetes tests that sets up a mock {@link FlinkKubeClient}. */
class KubernetesTestFixture {
    private static final long TIMEOUT = 30L * 1000L;

    private final String leaderConfigmapName;
    private final String lockIdentity;

    private final Configuration configuration;

    private final Map<String, KubernetesConfigMap> configMapStore = new HashMap<>();

    private final List<CompletableFuture<FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap>>>
            configMapCallbackFutures = new ArrayList<>();

    private final List<TestingFlinkKubeClient.MockKubernetesWatch> configMapWatches =
            new ArrayList<>();

    private final CompletableFuture<Map<String, String>> deleteConfigMapByLabelsFuture =
            new CompletableFuture<>();
    private final CompletableFuture<Void> closeKubeClientFuture = new CompletableFuture<>();

    private final CompletableFuture<KubernetesLeaderElector.LeaderCallbackHandler>
            leaderCallbackHandlerFuture = new CompletableFuture<>();

    private final FlinkKubeClient flinkKubeClient;

    private final KubernetesConfigMapSharedWatcher configMapSharedWatcher;

    KubernetesTestFixture(String clusterId, String leaderConfigmapName, String lockIdentity) {
        this.leaderConfigmapName = leaderConfigmapName;
        this.lockIdentity = lockIdentity;
        configuration = new Configuration();
        configuration.setString(KubernetesConfigOptions.CLUSTER_ID, clusterId);

        flinkKubeClient = createFlinkKubeClient();
        configMapSharedWatcher =
                flinkKubeClient.createConfigMapSharedWatcher(
                        KubernetesUtils.getConfigMapLabels(
                                clusterId, LABEL_CONFIGMAP_TYPE_HIGH_AVAILABILITY));
    }

    void close() {
        configMapSharedWatcher.close();
    }

    FlinkKubeClient getFlinkKubeClient() {
        return flinkKubeClient;
    }

    CompletableFuture<Void> getCloseKubeClientFuture() {
        return closeKubeClientFuture;
    }

    CompletableFuture<Map<String, String>> getDeleteConfigMapByLabelsFuture() {
        return deleteConfigMapByLabelsFuture;
    }

    KubernetesConfigMapSharedWatcher getConfigMapSharedWatcher() {
        return configMapSharedWatcher;
    }

    Configuration getConfiguration() {
        return configuration;
    }

    KubernetesConfigMap getLeaderConfigMap() {
        final Optional<KubernetesConfigMap> configMapOpt =
                flinkKubeClient.getConfigMap(leaderConfigmapName);
        assertThat(configMapOpt).isPresent();
        return configMapOpt.get();
    }

    // Use the leader callback to manually grant leadership
    void leaderCallbackGrantLeadership() throws Exception {
        createLeaderConfigMap();
        getLeaderCallback().isLeader();
    }

    FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> getLeaderElectionConfigMapCallback()
            throws Exception {
        assertThat(configMapCallbackFutures).hasSizeGreaterThanOrEqualTo(1);
        return configMapCallbackFutures.get(0).get(TIMEOUT, TimeUnit.MILLISECONDS);
    }

    FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> getLeaderRetrievalConfigMapCallback()
            throws Exception {
        assertThat(configMapCallbackFutures).hasSize(2);
        return configMapCallbackFutures.get(1).get(TIMEOUT, TimeUnit.MILLISECONDS);
    }

    KubernetesLeaderElector.LeaderCallbackHandler getLeaderCallback() throws Exception {
        return leaderCallbackHandlerFuture.get(TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private FlinkKubeClient createFlinkKubeClient() {
        return createFlinkKubeClientBuilder().build();
    }

    TestingFlinkKubeClient.Builder createFlinkKubeClientBuilder() {
        return TestingFlinkKubeClient.builder()
                .setCreateConfigMapFunction(
                        configMap -> {
                            configMapStore.put(configMap.getName(), configMap);
                            return CompletableFuture.completedFuture(null);
                        })
                .setGetConfigMapFunction(
                        configMapName -> Optional.ofNullable(configMapStore.get(configMapName)))
                .setCheckAndUpdateConfigMapFunction(
                        (configMapName, updateFunction) -> {
                            final KubernetesConfigMap configMap = configMapStore.get(configMapName);
                            if (configMap != null) {
                                try {
                                    final boolean updated =
                                            updateFunction
                                                    .apply(configMap)
                                                    .map(
                                                            updateConfigMap -> {
                                                                configMapStore.put(
                                                                        configMap.getName(),
                                                                        updateConfigMap);
                                                                return true;
                                                            })
                                                    .orElse(false);
                                    return CompletableFuture.completedFuture(updated);
                                } catch (Throwable throwable) {
                                    return FutureUtils.completedExceptionally(throwable);
                                }
                            }
                            return FutureUtils.completedExceptionally(
                                    new KubernetesException(
                                            "ConfigMap " + configMapName + " does not exist."));
                        })
                .setDeleteConfigMapFunction(
                        name -> {
                            configMapStore.remove(name);
                            return FutureUtils.completedVoidFuture();
                        })
                .setDeleteConfigMapByLabelFunction(
                        labels -> {
                            if (deleteConfigMapByLabelsFuture.isDone()) {
                                return FutureUtils.completedExceptionally(
                                        new KubernetesException(
                                                "ConfigMap with labels "
                                                        + labels
                                                        + " has already be deleted."));
                            } else {
                                deleteConfigMapByLabelsFuture.complete(labels);
                                return FutureUtils.completedVoidFuture();
                            }
                        })
                .setCloseConsumer(closeKubeClientFuture::complete)
                .setCreateLeaderElectorFunction(
                        (leaderConfig, callbackHandler) -> {
                            leaderCallbackHandlerFuture.complete(callbackHandler);
                            return new TestingFlinkKubeClient.TestingKubernetesLeaderElector(
                                    leaderConfig, callbackHandler);
                        })
                .setCreateConfigMapSharedWatcherFunction(
                        (labels) -> {
                            final TestingFlinkKubeClient.TestingKubernetesConfigMapSharedWatcher
                                    watcher =
                                            new TestingFlinkKubeClient
                                                    .TestingKubernetesConfigMapSharedWatcher(
                                                    labels);
                            watcher.setWatchFunction(
                                    (ignore, handler) -> {
                                        final CompletableFuture<
                                                        FlinkKubeClient.WatchCallbackHandler<
                                                                KubernetesConfigMap>>
                                                future = CompletableFuture.completedFuture(handler);
                                        configMapCallbackFutures.add(future);
                                        final TestingFlinkKubeClient.MockKubernetesWatch watch =
                                                new TestingFlinkKubeClient.MockKubernetesWatch();
                                        configMapWatches.add(watch);
                                        return watch;
                                    });
                            return watcher;
                        });
    }

    // This method need be called when before the leader is granted. Since the
    // TestingKubernetesLeaderElector
    // will not create the leader ConfigMap automatically.
    private void createLeaderConfigMap() {
        final KubernetesConfigMap configMap =
                new TestingFlinkKubeClient.MockKubernetesConfigMap(leaderConfigmapName);
        configMap.getAnnotations().put(LEADER_ANNOTATION_KEY, lockIdentity);
        flinkKubeClient.createConfigMap(configMap);
    }
}
