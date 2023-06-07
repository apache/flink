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

import org.apache.flink.kubernetes.kubeclient.FlinkPod;
import org.apache.flink.kubernetes.kubeclient.TestingWatchCallbackHandler;

import io.fabric8.kubernetes.api.model.StatusBuilder;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.net.HttpURLConnection.HTTP_GONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for {@link KubernetesPodsWatcher}. */
class KubernetesPodsWatcherTest {

    private final List<KubernetesPod> podAddedList = new ArrayList<>();
    private final List<KubernetesPod> podModifiedList = new ArrayList<>();
    private final List<KubernetesPod> podDeletedList = new ArrayList<>();
    private final List<KubernetesPod> podErrorList = new ArrayList<>();

    @Test
    void testClosingWithNullException() {
        final KubernetesPodsWatcher podsWatcher =
                new KubernetesPodsWatcher(
                        TestingWatchCallbackHandler.<KubernetesPod>builder()
                                .setHandleErrorConsumer(e -> fail("Should not reach here."))
                                .build());
        podsWatcher.onClose(null);
    }

    @Test
    void testClosingWithException() {
        final AtomicBoolean called = new AtomicBoolean(false);
        final KubernetesPodsWatcher podsWatcher =
                new KubernetesPodsWatcher(
                        TestingWatchCallbackHandler.<KubernetesPod>builder()
                                .setHandleErrorConsumer(e -> called.set(true))
                                .build());
        podsWatcher.onClose(new WatcherException("exception"));
        assertThat(called.get()).isTrue();
    }

    @Test
    void testCallbackHandler() {
        FlinkPod pod = new FlinkPod.Builder().build();
        final KubernetesPodsWatcher podsWatcher =
                new KubernetesPodsWatcher(
                        TestingWatchCallbackHandler.<KubernetesPod>builder()
                                .setOnAddedConsumer(pods -> podAddedList.addAll(pods))
                                .setOnModifiedConsumer(pods -> podModifiedList.addAll(pods))
                                .setOnDeletedConsumer(pods -> podDeletedList.addAll(pods))
                                .setOnErrorConsumer(pods -> podErrorList.addAll(pods))
                                .build());
        podsWatcher.eventReceived(Watcher.Action.ADDED, pod.getPodWithoutMainContainer());
        podsWatcher.eventReceived(Watcher.Action.MODIFIED, pod.getPodWithoutMainContainer());
        podsWatcher.eventReceived(Watcher.Action.DELETED, pod.getPodWithoutMainContainer());
        podsWatcher.eventReceived(Watcher.Action.ERROR, pod.getPodWithoutMainContainer());

        assertThat(podAddedList).hasSize(1);
        assertThat(podModifiedList).hasSize(1);
        assertThat(podDeletedList).hasSize(1);
        assertThat(podErrorList).hasSize(1);
    }

    @Test
    void testClosingWithTooOldResourceVersion() {
        final String errMsg = "too old resource version";
        final KubernetesPodsWatcher podsWatcher =
                new KubernetesPodsWatcher(
                        TestingWatchCallbackHandler.<KubernetesPod>builder()
                                .setHandleErrorConsumer(
                                        e -> {
                                            assertThat(e)
                                                    .isInstanceOf(
                                                            KubernetesTooOldResourceVersionException
                                                                    .class)
                                                    .hasMessageContaining(errMsg);
                                        })
                                .build());
        podsWatcher.onClose(
                new WatcherException(
                        errMsg,
                        new KubernetesClientException(
                                errMsg, HTTP_GONE, new StatusBuilder().build())));
    }
}
