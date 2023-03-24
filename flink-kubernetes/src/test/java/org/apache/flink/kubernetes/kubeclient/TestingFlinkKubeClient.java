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

import org.apache.flink.kubernetes.configuration.KubernetesLeaderElectionConfiguration;
import org.apache.flink.kubernetes.kubeclient.KubernetesSharedWatcher.Watch;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesService;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;

import javax.annotation.Nullable;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/** Testing implementation of {@link FlinkKubeClient}. */
public class TestingFlinkKubeClient implements FlinkKubeClient {

    private final Function<KubernetesPod, CompletableFuture<Void>> createTaskManagerPodFunction;
    private final Function<String, CompletableFuture<Void>> stopPodFunction;
    private final Consumer<String> stopAndCleanupClusterConsumer;
    private final Function<Map<String, String>, List<KubernetesPod>> getPodsWithLabelsFunction;
    private final BiFunction<
                    Map<String, String>, WatchCallbackHandler<KubernetesPod>, KubernetesWatch>
            watchPodsAndDoCallbackFunction;
    private final Function<KubernetesConfigMap, CompletableFuture<Void>> createConfigMapFunction;
    private final Function<String, Optional<KubernetesConfigMap>> getConfigMapFunction;
    private final BiFunction<
                    String,
                    Function<KubernetesConfigMap, Optional<KubernetesConfigMap>>,
                    CompletableFuture<Boolean>>
            checkAndUpdateConfigMapFunction;
    private final Function<Map<String, String>, CompletableFuture<Void>>
            deleteConfigMapByLabelFunction;
    private final Function<String, CompletableFuture<Void>> deleteConfigMapFunction;
    private final Consumer<Void> closeConsumer;
    private final BiFunction<
                    KubernetesLeaderElectionConfiguration,
                    KubernetesLeaderElector.LeaderCallbackHandler,
                    KubernetesLeaderElector>
            createLeaderElectorFunction;

    private final Function<Map<String, String>, KubernetesConfigMapSharedWatcher>
            createConfigMapSharedWatcherFunction;

    private TestingFlinkKubeClient(
            Function<KubernetesPod, CompletableFuture<Void>> createTaskManagerPodFunction,
            Function<String, CompletableFuture<Void>> stopPodFunction,
            Consumer<String> stopAndCleanupClusterConsumer,
            Function<Map<String, String>, List<KubernetesPod>> getPodsWithLabelsFunction,
            BiFunction<Map<String, String>, WatchCallbackHandler<KubernetesPod>, KubernetesWatch>
                    watchPodsAndDoCallbackFunction,
            Function<KubernetesConfigMap, CompletableFuture<Void>> createConfigMapFunction,
            Function<String, Optional<KubernetesConfigMap>> getConfigMapFunction,
            BiFunction<
                            String,
                            Function<KubernetesConfigMap, Optional<KubernetesConfigMap>>,
                            CompletableFuture<Boolean>>
                    checkAndUpdateConfigMapFunction,
            Function<Map<String, String>, CompletableFuture<Void>> deleteConfigMapByLabelFunction,
            Function<String, CompletableFuture<Void>> deleteConfigMapFunction,
            Consumer<Void> closeConsumer,
            BiFunction<
                            KubernetesLeaderElectionConfiguration,
                            KubernetesLeaderElector.LeaderCallbackHandler,
                            KubernetesLeaderElector>
                    createLeaderElectorFunction,
            Function<Map<String, String>, KubernetesConfigMapSharedWatcher>
                    createConfigMapSharedWatcherFunction) {

        this.createTaskManagerPodFunction = createTaskManagerPodFunction;
        this.stopPodFunction = stopPodFunction;
        this.stopAndCleanupClusterConsumer = stopAndCleanupClusterConsumer;
        this.getPodsWithLabelsFunction = getPodsWithLabelsFunction;
        this.watchPodsAndDoCallbackFunction = watchPodsAndDoCallbackFunction;

        this.createConfigMapFunction = createConfigMapFunction;
        this.getConfigMapFunction = getConfigMapFunction;
        this.checkAndUpdateConfigMapFunction = checkAndUpdateConfigMapFunction;
        this.deleteConfigMapByLabelFunction = deleteConfigMapByLabelFunction;
        this.deleteConfigMapFunction = deleteConfigMapFunction;

        this.closeConsumer = closeConsumer;

        this.createLeaderElectorFunction = createLeaderElectorFunction;
        this.createConfigMapSharedWatcherFunction = createConfigMapSharedWatcherFunction;
    }

    @Override
    public void createJobManagerComponent(KubernetesJobManagerSpecification kubernetesJMSpec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> createTaskManagerPod(KubernetesPod kubernetesPod) {
        return createTaskManagerPodFunction.apply(kubernetesPod);
    }

    @Override
    public CompletableFuture<Void> stopPod(String podName) {
        return stopPodFunction.apply(podName);
    }

    @Override
    public void stopAndCleanupCluster(String clusterId) {
        stopAndCleanupClusterConsumer.accept(clusterId);
    }

    @Override
    public Optional<KubernetesService> getService(String serviceName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<Endpoint> getRestEndpoint(String clusterId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<KubernetesPod> getPodsWithLabels(Map<String, String> labels) {
        return getPodsWithLabelsFunction.apply(labels);
    }

    @Override
    public KubernetesWatch watchPodsAndDoCallback(
            Map<String, String> labels, WatchCallbackHandler<KubernetesPod> podCallbackHandler) {
        return watchPodsAndDoCallbackFunction.apply(labels, podCallbackHandler);
    }

    @Override
    public KubernetesLeaderElector createLeaderElector(
            KubernetesLeaderElectionConfiguration leaderConfig,
            KubernetesLeaderElector.LeaderCallbackHandler callbackHandler) {
        return createLeaderElectorFunction.apply(leaderConfig, callbackHandler);
    }

    @Override
    public CompletableFuture<Void> createConfigMap(KubernetesConfigMap configMap) {
        return createConfigMapFunction.apply(configMap);
    }

    @Override
    public Optional<KubernetesConfigMap> getConfigMap(String name) {
        return getConfigMapFunction.apply(name);
    }

    @Override
    public CompletableFuture<Boolean> checkAndUpdateConfigMap(
            String configMapName,
            Function<KubernetesConfigMap, Optional<KubernetesConfigMap>> updateFunction) {
        return checkAndUpdateConfigMapFunction.apply(configMapName, updateFunction);
    }

    @Override
    public CompletableFuture<Void> deleteConfigMapsByLabels(Map<String, String> labels) {
        return deleteConfigMapByLabelFunction.apply(labels);
    }

    @Override
    public CompletableFuture<Void> deleteConfigMap(String configMapName) {
        return deleteConfigMapFunction.apply(configMapName);
    }

    @Override
    public KubernetesConfigMapSharedWatcher createConfigMapSharedWatcher(
            Map<String, String> labels) {
        return createConfigMapSharedWatcherFunction.apply(labels);
    }

    @Override
    public void close() {
        closeConsumer.accept(null);
    }

    @Override
    public KubernetesPod loadPodFromTemplateFile(File file) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<Void> updateServiceTargetPort(
            String serviceName, String portName, int targetPort) {
        throw new UnsupportedOperationException();
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Builder class for {@link TestingFlinkKubeClient}. */
    public static class Builder {
        private Function<KubernetesPod, CompletableFuture<Void>> createTaskManagerPodFunction =
                (ignore) -> FutureUtils.completedVoidFuture();
        private Function<String, CompletableFuture<Void>> stopPodFunction =
                (ignore) -> FutureUtils.completedVoidFuture();
        private Consumer<String> stopAndCleanupClusterConsumer = (ignore) -> {};
        private Function<Map<String, String>, List<KubernetesPod>> getPodsWithLabelsFunction =
                (ignore) -> Collections.emptyList();
        private BiFunction<
                        Map<String, String>, WatchCallbackHandler<KubernetesPod>, KubernetesWatch>
                watchPodsAndDoCallbackFunction = (ignore1, ignore2) -> new MockKubernetesWatch();

        private Function<KubernetesConfigMap, CompletableFuture<Void>> createConfigMapFunction =
                (ignore) -> FutureUtils.completedVoidFuture();
        private Function<String, Optional<KubernetesConfigMap>> getConfigMapFunction =
                (ignore) -> Optional.empty();
        private BiFunction<
                        String,
                        Function<KubernetesConfigMap, Optional<KubernetesConfigMap>>,
                        CompletableFuture<Boolean>>
                checkAndUpdateConfigMapFunction =
                        (ignore1, ignore2) -> CompletableFuture.completedFuture(true);
        private Function<Map<String, String>, CompletableFuture<Void>>
                deleteConfigMapByLabelFunction = (ignore) -> FutureUtils.completedVoidFuture();
        private Function<String, CompletableFuture<Void>> deleteConfigMapFunction =
                (ignore) -> FutureUtils.completedVoidFuture();

        private Consumer<Void> closeConsumer = (ignore) -> {};

        private BiFunction<
                        KubernetesLeaderElectionConfiguration,
                        KubernetesLeaderElector.LeaderCallbackHandler,
                        KubernetesLeaderElector>
                createLeaderElectorFunction = TestingKubernetesLeaderElector::new;

        private Function<Map<String, String>, KubernetesConfigMapSharedWatcher>
                createConfigMapSharedWatcherFunction = TestingKubernetesConfigMapSharedWatcher::new;

        private Builder() {}

        public Builder setCreateTaskManagerPodFunction(
                Function<KubernetesPod, CompletableFuture<Void>> createTaskManagerPodFunction) {
            this.createTaskManagerPodFunction =
                    Preconditions.checkNotNull(createTaskManagerPodFunction);
            return this;
        }

        public Builder setStopPodFunction(
                Function<String, CompletableFuture<Void>> stopPodFunction) {
            this.stopPodFunction = Preconditions.checkNotNull(stopPodFunction);
            return this;
        }

        public Builder setStopAndCleanupClusterConsumer(
                Consumer<String> stopAndCleanupClusterConsumer) {
            this.stopAndCleanupClusterConsumer =
                    Preconditions.checkNotNull(stopAndCleanupClusterConsumer);
            return this;
        }

        public Builder setGetPodsWithLabelsFunction(
                Function<Map<String, String>, List<KubernetesPod>> getPodsWithLabelsFunction) {
            this.getPodsWithLabelsFunction = Preconditions.checkNotNull(getPodsWithLabelsFunction);
            return this;
        }

        public Builder setWatchPodsAndDoCallbackFunction(
                BiFunction<
                                Map<String, String>,
                                WatchCallbackHandler<KubernetesPod>,
                                KubernetesWatch>
                        watchPodsAndDoCallbackFunction) {
            this.watchPodsAndDoCallbackFunction =
                    Preconditions.checkNotNull(watchPodsAndDoCallbackFunction);
            return this;
        }

        public Builder setCreateConfigMapFunction(
                Function<KubernetesConfigMap, CompletableFuture<Void>> createConfigMapFunction) {
            this.createConfigMapFunction = createConfigMapFunction;
            return this;
        }

        public Builder setGetConfigMapFunction(
                Function<String, Optional<KubernetesConfigMap>> getConfigMapFunction) {
            this.getConfigMapFunction = getConfigMapFunction;
            return this;
        }

        public Builder setCheckAndUpdateConfigMapFunction(
                BiFunction<
                                String,
                                Function<KubernetesConfigMap, Optional<KubernetesConfigMap>>,
                                CompletableFuture<Boolean>>
                        checkAndUpdateConfigMapFunction) {
            this.checkAndUpdateConfigMapFunction = checkAndUpdateConfigMapFunction;
            return this;
        }

        public Builder setDeleteConfigMapByLabelFunction(
                Function<Map<String, String>, CompletableFuture<Void>>
                        deleteConfigMapByLabelFunction) {
            this.deleteConfigMapByLabelFunction = deleteConfigMapByLabelFunction;
            return this;
        }

        public Builder setDeleteConfigMapFunction(
                Function<String, CompletableFuture<Void>> deleteConfigMapFunction) {
            this.deleteConfigMapFunction = deleteConfigMapFunction;
            return this;
        }

        public Builder setCloseConsumer(Consumer<Void> closeConsumer) {
            this.closeConsumer = closeConsumer;
            return this;
        }

        public Builder setCreateLeaderElectorFunction(
                BiFunction<
                                KubernetesLeaderElectionConfiguration,
                                KubernetesLeaderElector.LeaderCallbackHandler,
                                KubernetesLeaderElector>
                        createLeaderElectorFunction) {
            this.createLeaderElectorFunction = createLeaderElectorFunction;
            return this;
        }

        public Builder setCreateConfigMapSharedWatcherFunction(
                Function<Map<String, String>, KubernetesConfigMapSharedWatcher>
                        createConfigMapSharedWatcherFunction) {
            this.createConfigMapSharedWatcherFunction = createConfigMapSharedWatcherFunction;
            return this;
        }

        public TestingFlinkKubeClient build() {
            return new TestingFlinkKubeClient(
                    createTaskManagerPodFunction,
                    stopPodFunction,
                    stopAndCleanupClusterConsumer,
                    getPodsWithLabelsFunction,
                    watchPodsAndDoCallbackFunction,
                    createConfigMapFunction,
                    getConfigMapFunction,
                    checkAndUpdateConfigMapFunction,
                    deleteConfigMapByLabelFunction,
                    deleteConfigMapFunction,
                    closeConsumer,
                    createLeaderElectorFunction,
                    createConfigMapSharedWatcherFunction);
        }
    }

    /** Testing implementation of {@link KubernetesWatch} and {@link Watch}. */
    public static class MockKubernetesWatch extends KubernetesWatch implements Watch {
        private boolean isClosed;

        public MockKubernetesWatch() {
            super(null);
            this.isClosed = false;
        }

        @Override
        public void close() {
            this.isClosed = true;
        }

        public boolean isClosed() {
            return isClosed;
        }
    }

    /** Testing implementation of {@link KubernetesConfigMap}. */
    public static class MockKubernetesConfigMap extends KubernetesConfigMap {
        private final String name;
        private final Map<String, String> data;
        private final Map<String, String> labels;
        private final Map<String, String> annotations;
        private final String resourceVersion = "1";

        public MockKubernetesConfigMap(String name) {
            super(null);
            this.name = name;
            this.data = new HashMap<>();
            this.labels = new HashMap<>();
            this.annotations = new HashMap<>();
        }

        @Override
        public String getName() {
            return this.name;
        }

        @Override
        public Map<String, String> getData() {
            return this.data;
        }

        @Override
        public Map<String, String> getAnnotations() {
            return annotations;
        }

        @Override
        public Map<String, String> getLabels() {
            return this.labels;
        }

        @Override
        public String getResourceVersion() {
            return this.resourceVersion;
        }
    }

    /** Testing implementation of {@link KubernetesLeaderElector}. */
    public static class TestingKubernetesLeaderElector extends KubernetesLeaderElector {

        public TestingKubernetesLeaderElector(
                KubernetesLeaderElectionConfiguration leaderConfig,
                LeaderCallbackHandler leaderCallbackHandler) {
            super(new KubernetesMockServer().createClient(), leaderConfig, leaderCallbackHandler);
        }

        @Override
        public void run() {
            // noop
        }
    }

    /** Testing implementation of {@link KubernetesSharedWatcher} for ConfigMap. */
    public static class TestingKubernetesConfigMapSharedWatcher
            implements KubernetesConfigMapSharedWatcher {

        private BiFunction<String, WatchCallbackHandler<KubernetesConfigMap>, Watch> watchFunction =
                (ignore1, ignore2) -> new MockKubernetesWatch();

        public TestingKubernetesConfigMapSharedWatcher(Map<String, String> labels) {}

        public void setWatchFunction(
                BiFunction<String, WatchCallbackHandler<KubernetesConfigMap>, Watch>
                        watchFunction) {
            this.watchFunction = watchFunction;
        }

        @Override
        public void close() {
            // noop
        }

        @Override
        public Watch watch(
                String name,
                WatchCallbackHandler<KubernetesConfigMap> callbackHandler,
                @Nullable Executor executor) {
            return watchFunction.apply(name, callbackHandler);
        }
    }
}
