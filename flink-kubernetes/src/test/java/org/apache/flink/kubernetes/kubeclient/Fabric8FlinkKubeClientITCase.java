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

import org.apache.flink.kubernetes.KubernetesResource;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.util.TestLogger;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * IT Tests for {@link org.apache.flink.kubernetes.kubeclient.Fabric8FlinkKubeClient} with real K8s
 * server and client.
 */
public class Fabric8FlinkKubeClientITCase extends TestLogger {

    @ClassRule public static KubernetesResource kubernetesResource = new KubernetesResource();

    private static final String TEST_CONFIG_MAP_NAME = "test-config-map";

    private static final long TIMEOUT = 120L * 1000L;

    private static final Map<String, String> data =
            new HashMap<String, String>() {
                {
                    put("key1", "0");
                    put("key2", "0");
                    put("key3", "0");
                }
            };

    private FlinkKubeClient flinkKubeClient;

    private ExecutorService executorService;

    @Before
    public void setup() throws Exception {
        flinkKubeClient = kubernetesResource.getFlinkKubeClient();
        flinkKubeClient
                .createConfigMap(
                        new KubernetesConfigMap(
                                new ConfigMapBuilder()
                                        .withNewMetadata()
                                        .withName(TEST_CONFIG_MAP_NAME)
                                        .endMetadata()
                                        .withData(data)
                                        .build()))
                .get();
        executorService =
                Executors.newFixedThreadPool(
                        data.size(), new ExecutorThreadFactory("test-leader-io"));
    }

    @After
    public void teardown() throws Exception {
        executorService.shutdownNow();
        flinkKubeClient.deleteConfigMap(TEST_CONFIG_MAP_NAME).get();
    }

    /**
     * {@link org.apache.flink.kubernetes.kubeclient.FlinkKubeClient#checkAndUpdateConfigMap} is a
     * transactional operation, we should definitely guarantee that the concurrent modification
     * could work.
     */
    @Test
    public void testCheckAndUpdateConfigMapConcurrently() throws Exception {
        // Start multiple instances to update ConfigMap concurrently
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        final int target = 10;
        final int updateIntervalMs = 100;
        for (String key : data.keySet()) {
            futures.add(
                    CompletableFuture.runAsync(
                            () -> {
                                for (int index = 0; index < target; index++) {
                                    final boolean updated =
                                            flinkKubeClient
                                                    .checkAndUpdateConfigMap(
                                                            TEST_CONFIG_MAP_NAME,
                                                            configMap -> {
                                                                final int newValue =
                                                                        Integer.valueOf(
                                                                                        configMap
                                                                                                .getData()
                                                                                                .get(
                                                                                                        key))
                                                                                + 1;
                                                                configMap
                                                                        .getData()
                                                                        .put(
                                                                                key,
                                                                                String.valueOf(
                                                                                        newValue));
                                                                return Optional.of(configMap);
                                                            })
                                                    .join();
                                    assertThat(updated, is(true));
                                    try {
                                        // Simulate the update interval
                                        Thread.sleep((long) (updateIntervalMs * Math.random()));
                                    } catch (InterruptedException e) {
                                        // noop
                                    }
                                }
                            },
                            executorService));
        }
        FutureUtils.waitForAll(futures).get(TIMEOUT, TimeUnit.MILLISECONDS);
        // All the value should be increased exactly to the target
        final Optional<KubernetesConfigMap> configMapOpt =
                flinkKubeClient.getConfigMap(TEST_CONFIG_MAP_NAME);
        assertThat(configMapOpt.isPresent(), is(true));
        assertThat(configMapOpt.get().getData().values(), everyItem(is(String.valueOf(target))));
    }
}
