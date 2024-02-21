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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.testutils.FlinkAssertions;
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

    private final String configMapName = "shared-informer-test-cluster";

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
        client.deleteConfigMap(configMapName).get();
    }

    @Test
    @Timeout(120000)
    public void testWatch() throws Exception {

        try (KubernetesConfigMapSharedWatcher watcher =
                client.createConfigMapSharedWatcher(configMapName)) {
            TestingCallbackHandler handler = new TestingCallbackHandler(configMapName);
            try (Watch watch =
                    watcher.watch(configMapName, handler.build(), watchCallbackExecutorService)) {
                createConfigMap(configMapName);
                FlinkAssertions.assertThatFuture(handler.addFuture)
                        .as("The creation of the ConfigMap should have been processed, eventually.")
                        .eventuallySucceeds();

                updateConfigMap(configMapName, ImmutableMap.of("foo", "bar"));
                FlinkAssertions.assertThatFuture(handler.addOrUpdateFuture)
                        .as("The update of the ConfigMap should have been processed, eventually.")
                        .eventuallySucceeds();
                assertThat(handler.assertFuture).isNotCompletedExceptionally();

                client.deleteConfigMap(configMapName).get();
                FlinkAssertions.assertThatFuture(handler.deleteFuture)
                        .as("The deletion of the ConfigMap should have been processed, eventually.")
                        .eventuallySucceeds();
                if (handler.assertFuture.isCompletedExceptionally()) {
                    handler.assertFuture.get();
                }
            }
        }
    }

    @Test
    void testWatchWithBlockHandler() throws Exception {
        final String configMapName = getConfigMapName(System.currentTimeMillis());
        try (KubernetesConfigMapSharedWatcher watcher =
                client.createConfigMapSharedWatcher(configMapName)) {

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

    private void createConfigMap(String name) throws Exception {
        client.createConfigMap(
                        new KubernetesConfigMap(
                                new ConfigMapBuilder()
                                        .withNewMetadata()
                                        .withName(name)
                                        .endMetadata()
                                        .build()))
                .get();
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

    private Tuple2<TestingCallbackHandler, Watch> watch(
            String name, KubernetesConfigMapSharedWatcher watcher) {
        final TestingCallbackHandler handler = new TestingCallbackHandler(name);
        final Watch watch = watcher.watch(name, handler.build(), watchCallbackExecutorService);
        return Tuple2.of(handler, watch);
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
