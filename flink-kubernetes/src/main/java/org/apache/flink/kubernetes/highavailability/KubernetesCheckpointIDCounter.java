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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesException;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.flink.kubernetes.utils.Constants.CHECKPOINT_COUNTER_KEY;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link CheckpointIDCounter} implementation for Kubernetes. The counter will be stored in
 * JobManager-{@link org.apache.flink.api.common.JobID}-leader ConfigMap. The key is {@link
 * org.apache.flink.kubernetes.utils.Constants#CHECKPOINT_COUNTER_KEY}, and value is counter value.
 */
public class KubernetesCheckpointIDCounter implements CheckpointIDCounter {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesCheckpointIDCounter.class);

    private final FlinkKubeClient kubeClient;

    private final String configMapName;

    private final String lockIdentity;

    private boolean running;

    public KubernetesCheckpointIDCounter(
            FlinkKubeClient kubeClient, String configMapName, String lockIdentity) {
        this.kubeClient = checkNotNull(kubeClient);
        this.configMapName = checkNotNull(configMapName);
        this.lockIdentity = checkNotNull(lockIdentity);

        this.running = false;
    }

    @Override
    public void start() {
        if (!running) {
            running = true;
        }
    }

    @Override
    public void shutdown(JobStatus jobStatus) {
        if (!running) {
            return;
        }
        running = false;

        LOG.info("Shutting down.");
        if (jobStatus.isGloballyTerminalState()) {
            LOG.info("Removing counter from ConfigMap {}", configMapName);
            kubeClient.checkAndUpdateConfigMap(
                    configMapName,
                    configMap -> {
                        if (KubernetesLeaderElector.hasLeadership(configMap, lockIdentity)) {
                            configMap.getData().remove(CHECKPOINT_COUNTER_KEY);
                            return Optional.of(configMap);
                        }
                        return Optional.empty();
                    });
        }
    }

    @Override
    public long getAndIncrement() throws Exception {
        final AtomicLong current = new AtomicLong();
        final boolean updated =
                kubeClient
                        .checkAndUpdateConfigMap(
                                configMapName,
                                configMap -> {
                                    if (KubernetesLeaderElector.hasLeadership(
                                            configMap, lockIdentity)) {
                                        final long currentValue = getCurrentCounter(configMap);
                                        current.set(currentValue);
                                        configMap
                                                .getData()
                                                .put(
                                                        CHECKPOINT_COUNTER_KEY,
                                                        String.valueOf(currentValue + 1));
                                        return Optional.of(configMap);
                                    }
                                    return Optional.empty();
                                })
                        .get();

        if (updated) {
            return current.get();
        } else {
            throw new KubernetesException(
                    "Failed to update ConfigMap "
                            + configMapName
                            + " since current KubernetesCheckpointIDCounter does not have the leadership.");
        }
    }

    @Override
    public long get() {
        return kubeClient
                .getConfigMap(configMapName)
                .map(this::getCurrentCounter)
                .orElseThrow(
                        () ->
                                new FlinkRuntimeException(
                                        new KubernetesException(
                                                "ConfigMap "
                                                        + configMapName
                                                        + " does not exist.")));
    }

    @Override
    public void setCount(long newCount) throws Exception {
        kubeClient
                .checkAndUpdateConfigMap(
                        configMapName,
                        configMap -> {
                            if (KubernetesLeaderElector.hasLeadership(configMap, lockIdentity)) {
                                final String existing =
                                        configMap.getData().get(CHECKPOINT_COUNTER_KEY);
                                final String newValue = String.valueOf(newCount);
                                if (existing == null || !existing.equals(newValue)) {
                                    configMap
                                            .getData()
                                            .put(CHECKPOINT_COUNTER_KEY, String.valueOf(newCount));
                                    return Optional.of(configMap);
                                }
                            }
                            return Optional.empty();
                        })
                .get();
    }

    private long getCurrentCounter(KubernetesConfigMap configMap) {
        if (configMap.getData().containsKey(CHECKPOINT_COUNTER_KEY)) {
            return Long.valueOf(configMap.getData().get(CHECKPOINT_COUNTER_KEY));
        } else {
            // Initial checkpoint id
            return 1;
        }
    }
}
