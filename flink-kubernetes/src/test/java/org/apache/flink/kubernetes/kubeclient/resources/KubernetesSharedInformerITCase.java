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

import org.apache.flink.kubernetes.KubernetesResource;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.KubernetesConfigMapSharedWatcher;
import org.apache.flink.kubernetes.kubeclient.KubernetesSharedWatcher.Watch;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** IT Tests for the {@link KubernetesSharedInformer}. */
public class KubernetesSharedInformerITCase extends TestLogger {

    @ClassRule public static KubernetesResource kubernetesResource = new KubernetesResource();

    private final ImmutableMap<String, String> labels =
            ImmutableMap.of("app", "shared-informer-test-cluster");

    private FlinkKubeClient client;
    private ExecutorService watchCallbackExecutorService;

    @Before
    public void setUp() throws Exception {
        client = kubernetesResource.getFlinkKubeClient();
        watchCallbackExecutorService =
                Executors.newCachedThreadPool(new ExecutorThreadFactory("Watch-Callback"));
    }

    @After
    public void tearDown() throws Exception {
        ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, watchCallbackExecutorService);
        client.deleteConfigMapsByLabels(labels).get();
    }

    @Test(timeout = 120000)
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
                    assertFalse(handler.assertFuture.isCompletedExceptionally());
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
    public void testWatchWithBlockHandler() throws Exception {
        try (KubernetesConfigMapSharedWatcher watcher =
                client.createConfigMapSharedWatcher(labels)) {

            final String configMapName = getConfigMapName(System.currentTimeMillis());
            final long block = 500;
            final long maxUpdateVal = 30;
            final TestingBlockCallbackHandler handler =
                    new TestingBlockCallbackHandler(block, maxUpdateVal);
            final Watch watch = watcher.watch(configMapName, handler, watchCallbackExecutorService);
            createConfigMap(configMapName);
            for (int i = 1; i <= maxUpdateVal; i++) {
                updateConfigMap(configMapName, ImmutableMap.of("val", String.valueOf(i)));
            }
            try {
                handler.expectedFuture.get(120, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                fail("expected value: " + maxUpdateVal + ", actual: " + handler.val);
            }
            try {
                handler.assertFuture.get(2 * block, TimeUnit.MILLISECONDS);
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
            final Watch watch = watcher.watch(name, handler, watchCallbackExecutorService);
            watchers.add(watch);
        }
    }

    private String getConfigMapName(long id) {
        return "shared-informer-test-cluster-" + id;
    }

    private static final class TestingCallbackHandler
            extends NoOpWatchCallbackHandler<KubernetesConfigMap> {

        private final CompletableFuture<Void> addFuture = new CompletableFuture<>();
        private final CompletableFuture<Void> addOrUpdateFuture = new CompletableFuture<>();
        private final CompletableFuture<Void> deleteFuture = new CompletableFuture<>();
        private final CompletableFuture<Void> assertFuture = new CompletableFuture<>();

        private final String resourceName;

        private TestingCallbackHandler(String resourceName) {
            this.resourceName = resourceName;
        }

        @Override
        public void onAdded(List<KubernetesConfigMap> resources) {
            final KubernetesConfigMap kubernetesConfigMap = resources.get(0);
            check(
                    assertFuture,
                    () -> {
                        assertThat(kubernetesConfigMap.getName(), is(resourceName));
                        assertFalse(addFuture.isDone());
                        assertFalse(addOrUpdateFuture.isDone());
                    });
            addFuture.complete(null);
            final String foo = kubernetesConfigMap.getData().get("foo");
            if (foo != null) {
                check(assertFuture, () -> assertThat(foo, is("bar")));
                addOrUpdateFuture.complete(null);
            }
        }

        @Override
        public void onModified(List<KubernetesConfigMap> resources) {
            final KubernetesConfigMap kubernetesConfigMap = resources.get(0);
            final String foo = kubernetesConfigMap.getData().get("foo");
            check(
                    assertFuture,
                    () -> {
                        assertThat(kubernetesConfigMap.getName(), is(resourceName));
                        assertThat(foo, is("bar"));
                        assertTrue(addFuture.isDone());
                    });
            if (addOrUpdateFuture.isDone()) {
                check(assertFuture, () -> assertTrue(isDeleting(kubernetesConfigMap)));
            } else {
                addOrUpdateFuture.complete(null);
            }
        }

        @Override
        public void onDeleted(List<KubernetesConfigMap> resources) {
            check(assertFuture, () -> assertFalse(deleteFuture.isDone()));
            deleteFuture.complete(null);
        }
    }

    private static final class TestingBlockCallbackHandler
            extends NoOpWatchCallbackHandler<KubernetesConfigMap> {
        private final CompletableFuture<Void> expectedFuture = new CompletableFuture<>();
        private final CompletableFuture<Void> assertFuture = new CompletableFuture<>();
        private final long block;
        private final long expected;

        private volatile long val = 0;

        public TestingBlockCallbackHandler(long block, long expected) {
            this.block = block;
            this.expected = expected;
        }

        @Override
        public void onAdded(List<KubernetesConfigMap> resources) {
            onAddedOrModified(resources);
        }

        @Override
        public void onModified(List<KubernetesConfigMap> resources) {
            onAddedOrModified(resources);
        }

        private void onAddedOrModified(List<KubernetesConfigMap> resources) {
            final KubernetesConfigMap kubernetesConfigMap = resources.get(0);
            final String valData = kubernetesConfigMap.getData().get("val");
            if (valData == null) {
                return;
            }
            final long newVal = Long.parseLong(valData);
            check(assertFuture, () -> assertTrue(newVal > val || isDeleting(kubernetesConfigMap)));
            val = newVal;
            block();
            if (expected == val) {
                expectedFuture.complete(null);
            }
        }

        private void block() {
            try {
                Thread.sleep(block);
            } catch (InterruptedException e) {
                // ignore
            }
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
