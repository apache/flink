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

package org.apache.flink.kubernetes.kubeclient.handlers;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** This handler will check the pod oom message, and report metric. */
public class PodOOMHandler implements FlinkKubeClient.PodModifyEventHandler {

    private static final Logger LOG = LoggerFactory.getLogger(PodOOMHandler.class);
    private static final String OOM_METRIC_NAME = "kubernetes.OOMKilled";
    private static final String OOM_KILLED_KEYWORD = "OOMKilled";

    private Counter oomCounter;
    // dead time to the set of oomKilled pods.
    // this is to avoid duplicate event message from api server.
    private Cache<String, Set<String>> tombstones;

    public PodOOMHandler(MetricGroup metricGroup) {
        this.oomCounter = metricGroup.counter(OOM_METRIC_NAME);
        this.tombstones = CacheBuilder.newBuilder().expireAfterWrite(1, TimeUnit.HOURS).build();
    }

    @Override
    public void handle(List<KubernetesPod> pods) {
        pods.forEach(
                p -> {
                    if (p.isTerminated()) {
                        Pod pod = p.getInternalResource();
                        String podName = pod.getMetadata().getName();
                        for (ContainerStatus containerStatus :
                                pod.getStatus().getContainerStatuses()) {
                            if (containerStatus.getState() != null
                                    && containerStatus.getState().getTerminated() != null
                                    && containerStatus.getState().getTerminated().getReason()
                                            != null
                                    && containerStatus.getState().getTerminated().getFinishedAt()
                                            != null) {
                                if (containerStatus
                                        .getState()
                                        .getTerminated()
                                        .getReason()
                                        .contains(OOM_KILLED_KEYWORD)) {
                                    String finishTime =
                                            containerStatus
                                                    .getState()
                                                    .getTerminated()
                                                    .getFinishedAt();
                                    Set<String> podKilledAtThisTime =
                                            tombstones.getIfPresent(finishTime);

                                    boolean newAdded = false;
                                    if (podKilledAtThisTime == null) {
                                        newAdded = true;
                                        podKilledAtThisTime = new HashSet<>();
                                        podKilledAtThisTime.add(podName);
                                        tombstones.put(finishTime, podKilledAtThisTime);
                                    } else {
                                        if (!podKilledAtThisTime.contains(podName)) {
                                            newAdded = true;
                                            podKilledAtThisTime.add(podName);
                                            tombstones.put(finishTime, podKilledAtThisTime);
                                        }
                                    }
                                    if (newAdded) {
                                        oomCounter.inc();
                                        LOG.info(
                                                "pod {} oom killed at {}, totally: {}",
                                                podName,
                                                finishTime,
                                                oomCounter.getCount());
                                    }
                                }
                            }
                        }
                    }
                });
    }

    @Override
    public void handle(KubernetesPod pod) {
        handle(Collections.singletonList(pod));
    }

    @VisibleForTesting
    public Cache<String, Set<String>> getTombstones() {
        return tombstones;
    }
}
