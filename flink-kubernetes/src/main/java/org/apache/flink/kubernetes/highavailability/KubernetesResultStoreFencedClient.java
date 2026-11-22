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

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.utils.KubernetesUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Accesses the ConfigMap of a Kubernetes result store. Each result is one entry of the ConfigMap
 * data: a dirty entry holds the value the store wrote for the result, and a clean entry holds the
 * value derived from it, or is removed if the store deletes on commit. Writes are fenced by a
 * leadership check and are idempotent, so the Kubernetes client may retry them. Reads are not
 * fenced.
 *
 * <p>The owning store calls the methods under its own lock.
 */
final class KubernetesResultStoreFencedClient {

    private static final Logger LOG =
            LoggerFactory.getLogger(KubernetesResultStoreFencedClient.class);

    private final FlinkKubeClient kubeClient;
    private final String configMapName;
    private final String clusterId;
    private final BooleanSupplier leadershipCheck;
    private final boolean deleteOnCommit;
    private final Predicate<String> isCleanValue;
    private final UnaryOperator<String> toCleanValue;

    /* Guarded by the lock of the owning store. */
    private boolean configMapCreated;

    /**
     * @param kubeClient the client used to access the ConfigMap.
     * @param configMapName the name of the ConfigMap holding the entries.
     * @param clusterId the cluster ID that determines the labels of the ConfigMap.
     * @param leadershipCheck tells whether this JobManager holds the leadership; it is evaluated
     *     right before every update, and an update without leadership fails.
     * @param deleteOnCommit whether entries are removed instead of marked as clean.
     * @param isCleanValue tells the value of a clean entry from the value of a dirty one.
     * @param toCleanValue derives the value of a clean entry from the value of the dirty one.
     */
    KubernetesResultStoreFencedClient(
            FlinkKubeClient kubeClient,
            String configMapName,
            String clusterId,
            BooleanSupplier leadershipCheck,
            boolean deleteOnCommit,
            Predicate<String> isCleanValue,
            UnaryOperator<String> toCleanValue) {
        this.kubeClient = checkNotNull(kubeClient);
        this.configMapName = checkNotNull(configMapName);
        this.clusterId = checkNotNull(clusterId);
        this.leadershipCheck = checkNotNull(leadershipCheck);
        this.deleteOnCommit = deleteOnCommit;
        this.isCleanValue = checkNotNull(isCleanValue);
        this.toCleanValue = checkNotNull(toCleanValue);
    }

    String getConfigMapName() {
        return configMapName;
    }

    /**
     * Adds a dirty entry. A retry that finds the entry with the same value succeeds.
     *
     * @throws IllegalStateException if the ConfigMap holds an entry with another value for the key.
     * @throws IOException if this JobManager does not hold the leadership, or the update failed.
     */
    void createDirtyEntry(String key, String value) throws IOException {
        final AtomicBoolean entryExists = new AtomicBoolean(false);

        final boolean updated =
                update(
                        configMap -> {
                            final Map<String, String> data = configMap.getData();
                            final String storedValue = data.get(key);
                            if (value.equals(storedValue)) {
                                // The client may retry an update that already succeeded, e.g.
                                // after a lost response. The entry is in place, so the retry is
                                // a success.
                                return Optional.of(configMap);
                            }
                            if (storedValue != null) {
                                entryExists.set(true);
                                return Optional.empty();
                            }
                            data.put(key, value);
                            return Optional.of(configMap);
                        });

        if (entryExists.get()) {
            throw new IllegalStateException(
                    String.format(
                            "ConfigMap %s already contains an entry for %s.", configMapName, key));
        }
        if (!updated) {
            throw new IOException(
                    String.format(
                            "Could not create the dirty entry %s in ConfigMap %s because this JobManager does not hold the leadership.",
                            key, configMapName));
        }
    }

    /**
     * Marks an entry as clean: removes it if the store deletes on commit, and replaces its value
     * with the clean value otherwise. An entry that is already clean stays as it is.
     *
     * @throws NoSuchElementException if the ConfigMap holds no entry for the key.
     * @throws IOException if this JobManager does not hold the leadership, or the update failed.
     */
    void markEntryAsClean(String key) throws IOException, NoSuchElementException {
        // Set once this update marked the entry, so that a retry of the same update, e.g. after a
        // lost response, tells a removed entry from one that never existed.
        final AtomicBoolean entryMarked = new AtomicBoolean(false);
        final AtomicBoolean entryMissing = new AtomicBoolean(false);

        final boolean updated =
                update(
                        configMap -> {
                            final Map<String, String> data = configMap.getData();
                            final String storedValue = data.get(key);
                            if (storedValue == null) {
                                if (entryMarked.get()) {
                                    return Optional.of(configMap);
                                }
                                entryMissing.set(true);
                                return Optional.empty();
                            }
                            if (isCleanValue.test(storedValue)) {
                                return Optional.of(configMap);
                            }
                            if (deleteOnCommit) {
                                data.remove(key);
                            } else {
                                data.put(key, toCleanValue.apply(storedValue));
                            }
                            entryMarked.set(true);
                            return Optional.of(configMap);
                        });

        if (entryMissing.get()) {
            throw new NoSuchElementException(
                    String.format(
                            "Could not mark the entry %s as clean as it is not present in ConfigMap %s.",
                            key, configMapName));
        }
        if (!updated) {
            throw new IOException(
                    String.format(
                            "Could not mark the entry %s as clean in ConfigMap %s because this JobManager does not hold the leadership.",
                            key, configMapName));
        }
    }

    Optional<String> getValue(String key) {
        return kubeClient
                .getConfigMap(configMapName)
                .map(configMap -> configMap.getData().get(key));
    }

    boolean isCleanValue(String value) {
        return isCleanValue.test(value);
    }

    boolean hasDirtyEntry(String key) {
        return getValue(key).map(value -> !isCleanValue.test(value)).orElse(false);
    }

    boolean hasCleanEntry(String key) {
        return getValue(key).map(isCleanValue::test).orElse(false);
    }

    /** The dirty entries by key; empty if the ConfigMap does not exist yet. */
    Map<String, String> getDirtyEntries() {
        final Optional<KubernetesConfigMap> configMap = kubeClient.getConfigMap(configMapName);
        if (!configMap.isPresent()) {
            return Collections.emptyMap();
        }

        final Map<String, String> dirtyEntries = new HashMap<>();
        for (Map.Entry<String, String> entry : configMap.get().getData().entrySet()) {
            if (!isCleanValue.test(entry.getValue())) {
                dirtyEntries.put(entry.getKey(), entry.getValue());
            }
        }
        return dirtyEntries;
    }

    /** Runs the update under the leadership check; {@code false} if nothing was written. */
    private boolean update(
            Function<KubernetesConfigMap, Optional<KubernetesConfigMap>> updateFunction)
            throws IOException {
        createConfigMapIfNeeded();
        try {
            return kubeClient
                    .checkAndUpdateConfigMap(
                            configMapName,
                            configMap ->
                                    leadershipCheck.getAsBoolean()
                                            ? updateFunction.apply(configMap)
                                            : Optional.empty())
                    .get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(
                    String.format("Interrupted while updating ConfigMap %s.", configMapName), e);
        } catch (ExecutionException e) {
            throw new IOException(
                    String.format("Could not update ConfigMap %s.", configMapName),
                    ExceptionUtils.stripExecutionException(e));
        }
    }

    private void createConfigMapIfNeeded() throws IOException {
        if (!configMapCreated) {
            LOG.info("Creating result store ConfigMap {}.", configMapName);
            try {
                KubernetesUtils.createConfigMapIfItDoesNotExist(
                        kubeClient, configMapName, clusterId);
            } catch (FlinkException e) {
                throw new IOException(e);
            }
            configMapCreated = true;
        }
    }
}
