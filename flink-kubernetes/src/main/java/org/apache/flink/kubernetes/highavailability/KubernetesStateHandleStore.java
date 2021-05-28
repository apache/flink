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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesException;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.persistence.StringResourceVersion;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.util.StateHandleStoreUtils.deserialize;
import static org.apache.flink.runtime.util.StateHandleStoreUtils.serializeOrDiscard;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Class which stores state via the provided {@link RetrievableStateStorageHelper} and writes the
 * returned state handle to ConfigMap.
 *
 * <p>Added state is persisted via {@link RetrievableStateHandle RetrievableStateHandles}, which in
 * turn are written to ConfigMap. This level of indirection is necessary to keep the amount of data
 * in ConfigMap small. ConfigMap is build for data less than 1MB whereas state can grow to multiple
 * MBs and GBs.
 *
 * <p>This is a very different implementation with {@link
 * org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore}. Benefit from the {@link
 * FlinkKubeClient#checkAndUpdateConfigMap} transactional operation, we could guarantee that only
 * the leader could update the store. Then we will completely get rid of the lock-and-release in
 * Zookeeper implementation.
 *
 * @param <T> Type of state
 */
public class KubernetesStateHandleStore<T extends Serializable>
        implements StateHandleStore<T, StringResourceVersion> {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesStateHandleStore.class);

    private final FlinkKubeClient kubeClient;

    private final String configMapName;

    private final RetrievableStateStorageHelper<T> storage;

    private final Predicate<String> configMapKeyFilter;

    private final String lockIdentity;

    /**
     * Creates a {@link KubernetesStateHandleStore}.
     *
     * @param kubeClient The Kubernetes client.
     * @param storage To persist the actual state and whose returned state handle is then written to
     *     ConfigMap
     * @param configMapName ConfigMap to store the state handle store pointer
     * @param configMapKeyFilter filter to get the expected keys for state handle
     * @param lockIdentity lock identity of current HA service
     */
    public KubernetesStateHandleStore(
            FlinkKubeClient kubeClient,
            String configMapName,
            RetrievableStateStorageHelper<T> storage,
            Predicate<String> configMapKeyFilter,
            String lockIdentity) {

        this.kubeClient = checkNotNull(kubeClient, "Kubernetes client");
        this.storage = checkNotNull(storage, "State storage");
        this.configMapName = checkNotNull(configMapName, "ConfigMap name");
        this.configMapKeyFilter = checkNotNull(configMapKeyFilter);
        this.lockIdentity = checkNotNull(lockIdentity, "Lock identity of current HA service");
    }

    /**
     * Creates a state handle, stores it in ConfigMap. We could guarantee that only the leader could
     * update the ConfigMap. Since “Get(check the leader)-and-Update(write back to the ConfigMap)”
     * is a transactional operation.
     *
     * @param key Key in ConfigMap
     * @param state State to be added
     * @throws AlreadyExistException if the name already exists
     * @throws PossibleInconsistentStateException if the write-to-Kubernetes operation failed. This
     *     indicates that it's not clear whether the new state was successfully written to
     *     Kubernetes or not. No state was discarded. Proper error handling has to be applied on the
     *     caller's side.
     * @throws Exception if persisting state or writing state handle failed
     */
    @Override
    public RetrievableStateHandle<T> addAndLock(String key, T state)
            throws PossibleInconsistentStateException, Exception {
        checkNotNull(key, "Key in ConfigMap.");
        checkNotNull(state, "State.");

        final RetrievableStateHandle<T> storeHandle = storage.store(state);

        final byte[] serializedStoreHandle = serializeOrDiscard(storeHandle);

        // initialize flag to serve the failure case
        boolean discardState = true;
        try {
            // a successful operation will result in the state not being discarded
            discardState =
                    !kubeClient
                            .checkAndUpdateConfigMap(
                                    configMapName,
                                    c -> {
                                        if (KubernetesLeaderElector.hasLeadership(
                                                c, lockIdentity)) {
                                            if (!c.getData().containsKey(key)) {
                                                c.getData()
                                                        .put(
                                                                key,
                                                                encodeStateHandle(
                                                                        serializedStoreHandle));
                                                return Optional.of(c);
                                            } else {
                                                throw new CompletionException(
                                                        getKeyAlreadyExistException(key));
                                            }
                                        }
                                        return Optional.empty();
                                    })
                            .get();
            return storeHandle;
        } catch (Exception ex) {
            final Optional<PossibleInconsistentStateException> possibleInconsistentStateException =
                    ExceptionUtils.findThrowable(ex, PossibleInconsistentStateException.class);
            if (possibleInconsistentStateException.isPresent()) {
                // it's unclear whether the state handle metadata was written to the ConfigMap -
                // hence, we don't discard the data
                discardState = false;
                throw possibleInconsistentStateException.get();
            }

            throw ExceptionUtils.findThrowable(ex, AlreadyExistException.class)
                    .orElseThrow(() -> ex);
        } finally {
            if (discardState) {
                storeHandle.discardState();
            }
        }
    }

    /**
     * Replaces a state handle in ConfigMap and discards the old state handle. Wo do not lock
     * resource version and then replace in Kubernetes. Since the ConfigMap is periodically updated
     * by leader, the resource version changes very fast. We use a "check-existence and update"
     * transactional operation instead.
     *
     * @param key Key in ConfigMap
     * @param resourceVersion resource version when checking existence via {@link #exists}.
     * @param state State to be added
     * @throws NotExistException if the name does not exist
     * @throws PossibleInconsistentStateException if a failure occurred during the update operation.
     *     It's unclear whether the operation actually succeeded or not. No state was discarded. The
     *     method's caller should handle this case properly.
     * @throws Exception if persisting state or writing state handle failed
     */
    @Override
    public void replace(String key, StringResourceVersion resourceVersion, T state)
            throws Exception {
        checkNotNull(key, "Key in ConfigMap.");
        checkNotNull(state, "State.");

        final RetrievableStateHandle<T> oldStateHandle = getAndLock(key);

        final RetrievableStateHandle<T> newStateHandle = storage.store(state);

        final byte[] serializedStateHandle = serializeOrDiscard(newStateHandle);

        // initialize flags to serve the failure case
        boolean discardOldState = false;
        boolean discardNewState = true;
        try {
            boolean success =
                    kubeClient
                            .checkAndUpdateConfigMap(
                                    configMapName,
                                    c -> {
                                        if (KubernetesLeaderElector.hasLeadership(
                                                c, lockIdentity)) {
                                            // Check the existence
                                            if (c.getData().containsKey(key)) {
                                                c.getData()
                                                        .put(
                                                                key,
                                                                encodeStateHandle(
                                                                        serializedStateHandle));
                                            } else {
                                                throw new CompletionException(
                                                        getKeyNotExistException(key));
                                            }
                                            return Optional.of(c);
                                        }
                                        return Optional.empty();
                                    })
                            .get();

            // swap subject for deletion in case of success
            discardOldState = success;
            discardNewState = !success;
        } catch (Exception ex) {
            final Optional<PossibleInconsistentStateException> possibleInconsistentStateException =
                    ExceptionUtils.findThrowable(ex, PossibleInconsistentStateException.class);
            if (possibleInconsistentStateException.isPresent()) {
                // it's unclear whether the state handle metadata was written to the ConfigMap -
                // hence, we don't discard any data
                discardNewState = false;
                throw possibleInconsistentStateException.get();
            }

            throw ExceptionUtils.findThrowable(ex, NotExistException.class).orElseThrow(() -> ex);
        } finally {
            if (discardNewState) {
                newStateHandle.discardState();
            }

            if (discardOldState) {
                oldStateHandle.discardState();
            }
        }
    }

    /**
     * Returns the resource version of the ConfigMap.
     *
     * @param key Key in ConfigMap
     * @return resource version in {@link StringResourceVersion} format.
     * @throws Exception if the check existence operation failed
     */
    @Override
    public StringResourceVersion exists(String key) throws Exception {
        checkNotNull(key, "Key in ConfigMap.");

        return kubeClient
                .getConfigMap(configMapName)
                .map(
                        configMap -> {
                            if (configMap.getData().containsKey(key)) {
                                return StringResourceVersion.valueOf(
                                        configMap.getResourceVersion());
                            }
                            return StringResourceVersion.notExisting();
                        })
                .orElseThrow(this::getConfigMapNotExistException);
    }

    /**
     * Gets the {@link RetrievableStateHandle} stored in the given ConfigMap.
     *
     * @param key Key in ConfigMap
     * @return The retrieved state handle from the specified ConfigMap and key
     * @throws IOException if the method failed to deserialize the stored state handle
     * @throws NotExistException when the name does not exist
     * @throws Exception if get state handle from ConfigMap failed
     */
    @Override
    public RetrievableStateHandle<T> getAndLock(String key) throws Exception {
        checkNotNull(key, "Key in ConfigMap.");

        final Optional<KubernetesConfigMap> optional = kubeClient.getConfigMap(configMapName);
        if (optional.isPresent()) {
            final KubernetesConfigMap configMap = optional.get();
            if (configMap.getData().containsKey(key)) {
                return deserializeObject(configMap.getData().get(key));
            } else {
                throw getKeyNotExistException(key);
            }
        } else {
            throw getConfigMapNotExistException();
        }
    }

    /**
     * Gets all available state handles from Kubernetes.
     *
     * @return All state handles from ConfigMap.
     */
    @Override
    public List<Tuple2<RetrievableStateHandle<T>, String>> getAllAndLock() {

        return kubeClient
                .getConfigMap(configMapName)
                .map(
                        configMap -> {
                            final List<Tuple2<RetrievableStateHandle<T>, String>> stateHandles =
                                    new ArrayList<>();
                            configMap.getData().entrySet().stream()
                                    .filter(entry -> configMapKeyFilter.test(entry.getKey()))
                                    .forEach(
                                            entry -> {
                                                try {
                                                    stateHandles.add(
                                                            new Tuple2<>(
                                                                    deserializeObject(
                                                                            entry.getValue()),
                                                                    entry.getKey()));
                                                } catch (IOException e) {
                                                    LOG.warn(
                                                            "ConfigMap {} contained corrupted data. Ignoring the key {}.",
                                                            configMapName,
                                                            entry.getKey());
                                                }
                                            });
                            return stateHandles;
                        })
                .orElse(Collections.emptyList());
    }

    /**
     * Return a list of all valid keys for state handles.
     *
     * @return List of valid state handle keys in Kubernetes ConfigMap
     * @throws Exception if get state handle names from ConfigMap failed.
     */
    @Override
    public Collection<String> getAllHandles() throws Exception {

        return kubeClient
                .getConfigMap(configMapName)
                .map(
                        configMap ->
                                configMap.getData().keySet().stream()
                                        .filter(configMapKeyFilter)
                                        .collect(Collectors.toList()))
                .orElseThrow(this::getConfigMapNotExistException);
    }

    /**
     * Remove the key in state config map. As well as the state on external storage will be removed.
     * It returns the {@link RetrievableStateHandle} stored under the given state node if any.
     *
     * @param key Key to be removed from ConfigMap
     * @return True if the state handle is removed successfully
     * @throws Exception if removing the key or discarding the state failed
     */
    @Override
    public boolean releaseAndTryRemove(String key) throws Exception {
        checkNotNull(key, "Key in ConfigMap.");
        final AtomicReference<RetrievableStateHandle<T>> stateHandleRefer = new AtomicReference<>();

        return kubeClient
                .checkAndUpdateConfigMap(
                        configMapName,
                        configMap -> {
                            if (KubernetesLeaderElector.hasLeadership(configMap, lockIdentity)) {
                                final String content = configMap.getData().remove(key);
                                if (content != null) {
                                    try {
                                        stateHandleRefer.set(deserializeObject(content));
                                    } catch (IOException e) {
                                        LOG.warn(
                                                "Could not retrieve the state handle of {} from ConfigMap {}.",
                                                key,
                                                configMapName,
                                                e);
                                    }
                                }
                                return Optional.of(configMap);
                            }
                            return Optional.empty();
                        })
                .whenComplete(
                        (succeed, ignore) -> {
                            if (succeed) {
                                if (stateHandleRefer.get() != null) {
                                    try {
                                        stateHandleRefer.get().discardState();
                                    } catch (Exception e) {
                                        throw new CompletionException(e);
                                    }
                                }
                            }
                        })
                .get();
    }

    /**
     * Remove all the state handle keys in the ConfigMap and discard the states.
     *
     * @throws Exception when removing the keys or discarding the state failed
     */
    @Override
    public void releaseAndTryRemoveAll() throws Exception {
        final List<RetrievableStateHandle<T>> validStateHandles = new ArrayList<>();
        kubeClient
                .checkAndUpdateConfigMap(
                        configMapName,
                        c -> {
                            if (KubernetesLeaderElector.hasLeadership(c, lockIdentity)) {
                                final Map<String, String> updateData = new HashMap<>(c.getData());
                                c.getData().entrySet().stream()
                                        .filter(entry -> configMapKeyFilter.test(entry.getKey()))
                                        .forEach(
                                                entry -> {
                                                    try {
                                                        validStateHandles.add(
                                                                deserializeObject(
                                                                        entry.getValue()));
                                                        updateData.remove(entry.getKey());
                                                    } catch (IOException e) {
                                                        LOG.warn(
                                                                "ConfigMap {} contained corrupted data. Ignoring the key {}.",
                                                                configMapName,
                                                                entry.getKey());
                                                    }
                                                });
                                c.getData().clear();
                                c.getData().putAll(updateData);
                                return Optional.of(c);
                            }
                            return Optional.empty();
                        })
                .whenComplete(
                        (succeed, ignore) -> {
                            if (succeed) {
                                Exception exception = null;
                                for (RetrievableStateHandle<T> stateHandle : validStateHandles) {
                                    try {
                                        stateHandle.discardState();
                                    } catch (Exception e) {
                                        exception = ExceptionUtils.firstOrSuppressed(e, exception);
                                    }
                                }
                                if (exception != null) {
                                    throw new CompletionException(
                                            new KubernetesException(
                                                    "Could not properly remove all state handles.",
                                                    exception));
                                }
                            }
                        })
                .get();
    }

    /**
     * Remove all the filtered keys in the ConfigMap.
     *
     * @throws Exception when removing the keys failed
     */
    @Override
    public void clearEntries() throws Exception {
        kubeClient
                .checkAndUpdateConfigMap(
                        configMapName,
                        c -> {
                            if (KubernetesLeaderElector.hasLeadership(c, lockIdentity)) {
                                c.getData().keySet().removeIf(configMapKeyFilter);
                                return Optional.of(c);
                            }
                            return Optional.empty();
                        })
                .get();
    }

    @Override
    public void release(String name) {
        // noop
    }

    @Override
    public void releaseAll() {
        // noop
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{configMapName='" + configMapName + "'}";
    }

    private RetrievableStateHandle<T> deserializeObject(String content) throws IOException {
        checkNotNull(content, "Content should not be null.");

        final byte[] data = Base64.getDecoder().decode(content);

        try {
            return deserialize(data);
        } catch (IOException | ClassNotFoundException e) {
            throw new IOException(
                    "Failed to deserialize state handle from ConfigMap data " + content + '.', e);
        }
    }

    private KubernetesException getConfigMapNotExistException() {
        return new KubernetesException(
                "ConfigMap "
                        + configMapName
                        + " does not exists. "
                        + "It may be deleted externally.");
    }

    private NotExistException getKeyNotExistException(String key) {
        return new NotExistException("Could not find " + key + " in ConfigMap " + configMapName);
    }

    private AlreadyExistException getKeyAlreadyExistException(String key) {
        return new AlreadyExistException(key + " already exists in ConfigMap " + configMapName);
    }

    private String encodeStateHandle(byte[] serializedStoreHandle) {
        return Base64.getEncoder().encodeToString(serializedStoreHandle);
    }
}
