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

package org.apache.flink.kubernetes.kubeclient.resources;

import org.apache.flink.kubernetes.KubernetesExtension;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesConfigMapSharedWatcher;
import org.apache.flink.kubernetes.kubeclient.KubernetesSharedWatcher.Watch;
import org.apache.flink.kubernetes.kubeclient.TestingWatchCallbackHandler;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** IT Tests for the {@link KubernetesSharedInformer}. */
class KubernetesSharedInformerITCase {

    @RegisterExtension
    private static final KubernetesExtension kubernetesExtension = new KubernetesExtension();

    private final ImmutableMap<String, String> labels =
            ImmutableMap.of("app", "shared-informer-test-cluster");

    private FlinkKubeClient client;
    private ExecutorService watchCallbackExecutorService;

    private static volatile Long blockVal = 0L;

    @BeforeEach
    void setUp() throws Exception {
        client = kubernetesExtension.getFlinkKubeClient();
        watchCallbackExecutorService =
                Executors.newCachedThreadPool(new ExecutorThreadFactory("Watch-Callback"));
    }

    @AfterEach
    void tearDown() throws Exception {
        ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, watchCallbackExecutorService);
        client.deleteConfigMapsByLabels(labels).get();
    }

    @Test
    @Timeout(120000)
    public void testWatch() throws Exception {

        try (KubernetesConfigMapSharedWatcher watcher =
                client.createConfigMapSharedWatcher(labels)) {
            for (int i = 0; i < 10; i++) {
                List<TestingCallbackHandler> callbackHandlers = new ArrayList<>();
                List<Watch> watchers = new ArrayList<>();

                watchInRange(watcher, callbackHandlers, watchers, 0, 20);
                createConfigMapsInRange(0, 5);
                watchInRange(watcher, callbackHandlers, watchers, 20, 40);
                createConfigMapsInRange(5, 10);
                updateConfigMapInRange(0, 10, ImmutableMap.of("foo", "bar"));
                for (TestingCallbackHandler handler : callbackHandlers) {
                    handler.addFuture.get();
                    handler.addOrUpdateFuture.get();
                    assertThat(handler.assertFuture).isNotCompletedExceptionally();
                }
                watchers.forEach(Watch::close);
                callbackHandlers.clear();
                watchers.clear();

                watchInRange(watcher, callbackHandlers, watchers, 40, 60);
                for (TestingCallbackHandler handler : callbackHandlers) {
                    handler.addFuture.get();
                    handler.addOrUpdateFuture.get();
                }
                client.deleteConfigMapsByLabels(labels).get();
                for (TestingCallbackHandler handler : callbackHandlers) {
                    handler.deleteFuture.get();
                    if (handler.assertFuture.isCompletedExceptionally()) {
                        handler.assertFuture.get();
                    }
                }
                watchers.forEach(Watch::close);
            }
        }
    }

    @Test
    void testWatchWithBlockHandler() throws Exception {
        try (KubernetesConfigMapSharedWatcher watcher =
                client.createConfigMapSharedWatcher(labels)) {

            final String configMapName = getConfigMapName(System.currentTimeMillis());
            final long block = 500;
            final long maxUpdateVal = 30;

            final CompletableFuture<Void> expectedFuture = new CompletableFuture<>();
            final CompletableFuture<Void> assertFuture = new CompletableFuture<>();
            final TestingWatchCallbackHandler handler =
                    TestingWatchCallbackHandler.<KubernetesConfigMap>builder()
                            .setOnAddedConsumer(
                                    (resources) ->
                                            onAddedOrModified(
                                                    resources,
                                                    maxUpdateVal,
                                                    block,
                                                    expectedFuture,
                                                    assertFuture))
                            .setOnModifiedConsumer(
                                    (resources) ->
                                            onAddedOrModified(
                                                    resources,
                                                    maxUpdateVal,
                                                    block,
                                                    expectedFuture,
                                                    assertFuture))
                            .build();
            final Watch watch = watcher.watch(configMapName, handler, watchCallbackExecutorService);
            createConfigMap(configMapName);
            for (int i = 1; i <= maxUpdateVal; i++) {
                updateConfigMap(configMapName, ImmutableMap.of("val", String.valueOf(i)));
            }
            assertThatCode(() -> expectedFuture.get(120, TimeUnit.SECONDS))
                    .as("expected value: " + maxUpdateVal + ", actual: " + blockVal)
                    .doesNotThrowAnyException();
            try {
                assertFuture.get(2 * block, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                // expected
            }
            watch.close();
        }
    }

    private void createConfigMapsInRange(int start, int end) throws Exception {
        for (int i = start; i < end; i++) {
            createConfigMap(getConfigMapName(i));
        }
    }

    private void createConfigMap(String name) throws Exception {
        client.createConfigMap(
                        new KubernetesConfigMap(
                                new ConfigMapBuilder()
                                        .withNewMetadata()
                                        .withName(name)
                                        .withLabels(labels)
                                        .endMetadata()
                                        .build()))
                .get();
    }

    private void updateConfigMapInRange(int start, int end, Map<String, String> data)
            throws Exception {
        for (int i = start; i < end; i++) {
            final String configMapName = getConfigMapName(i);
            updateConfigMap(configMapName, data);
        }
    }

    private void updateConfigMap(String configMapName, Map<String, String> data) throws Exception {
        client.checkAndUpdateConfigMap(
                        configMapName,
                        configMap -> {
                            configMap.getData().putAll(data);
                            return Optional.of(configMap);
                        })
                .get();
    }

    private void watchInRange(
            KubernetesConfigMapSharedWatcher watcher,
            List<TestingCallbackHandler> callbackHandlers,
            List<Watch> watchers,
            int start,
            int end) {

        for (int i = start; i < end; i++) {
            final String name = getConfigMapName(i % 10);
            final TestingCallbackHandler handler = new TestingCallbackHandler(name);
            callbackHandlers.add(handler);
            final Watch watch = watcher.watch(name, handler.build(), watchCallbackExecutorService);
            watchers.add(watch);
        }
    }

    private String getConfigMapName(long id) {
        return "shared-informer-test-cluster-" + id;
    }

    private static final class TestingCallbackHandler {

        private final CompletableFuture<Void> addFuture = new CompletableFuture<>();
        private final CompletableFuture<Void> addOrUpdateFuture = new CompletableFuture<>();
        private final CompletableFuture<Void> deleteFuture = new CompletableFuture<>();
        private final CompletableFuture<Void> assertFuture = new CompletableFuture<>();

        private final String resourceName;

        private TestingCallbackHandler(String resourceName) {
            this.resourceName = resourceName;
        }

        private TestingWatchCallbackHandler build() {
            return TestingWatchCallbackHandler.<KubernetesConfigMap>builder()
                    .setOnAddedConsumer(
                            resources -> {
                                final KubernetesConfigMap kubernetesConfigMap = resources.get(0);
                                check(
                                        assertFuture,
                                        () -> {
                                            assertThat(kubernetesConfigMap.getName())
                                                    .isEqualTo(resourceName);
                                            assertThat(addFuture).isNotDone();
                                            assertThat(addOrUpdateFuture).isNotDone();
                                        });
                                addFuture.complete(null);
                                final String foo = kubernetesConfigMap.getData().get("foo");
                                if (foo != null) {
                                    check(assertFuture, () -> assertThat(foo).isEqualTo("bar"));
                                    addOrUpdateFuture.complete(null);
                                }
                            })
                    .setOnModifiedConsumer(
                            (resources) -> {
                                final KubernetesConfigMap kubernetesConfigMap = resources.get(0);
                                final String foo = kubernetesConfigMap.getData().get("foo");
                                check(
                                        assertFuture,
                                        () -> {
                                            assertThat(kubernetesConfigMap.getName())
                                                    .isEqualTo(resourceName);
                                            assertThat(foo).isEqualTo("bar");
                                            assertThat(addFuture).isDone();
                                        });
                                if (addOrUpdateFuture.isDone()) {
                                    check(
                                            assertFuture,
                                            () ->
                                                    assertThat(isDeleting(kubernetesConfigMap))
                                                            .isTrue());
                                } else {
                                    addOrUpdateFuture.complete(null);
                                }
                            })
                    .setOnDeletedConsumer(
                            resources -> {
                                check(assertFuture, () -> assertThat(deleteFuture).isNotDone());
                                deleteFuture.complete(null);
                            })
                    .build();
        }
    }

    private static void onAddedOrModified(
            List<KubernetesConfigMap> resources,
            Long expected,
            Long block,
            CompletableFuture expectedFuture,
            CompletableFuture assertFuture) {
        final KubernetesConfigMap kubernetesConfigMap = resources.get(0);
        final String valData = kubernetesConfigMap.getData().get("val");
        if (valData == null) {
            return;
        }
        final long newVal = Long.parseLong(valData);
        check(
                assertFuture,
                () -> assertThat(newVal > blockVal || isDeleting(kubernetesConfigMap)).isTrue());
        blockVal = newVal;
        block(block);
        if (expected == blockVal) {
            expectedFuture.complete(null);
        }
    }

    private static void block(Long block) {
        try {
            Thread.sleep(block);
        } catch (InterruptedException e) {
            // ignore
        }
    }

    private static void check(CompletableFuture<Void> future, Runnable assertFunc) {
        try {
            assertFunc.run();
        } catch (Throwable err) {
            future.completeExceptionally(err);
            throw err;
        }
    }

    private static boolean isDeleting(KubernetesConfigMap kubernetesConfigMap) {
        return kubernetesConfigMap.getInternalResource().getMetadata().getDeletionTimestamp()
                != null;
    }
}
