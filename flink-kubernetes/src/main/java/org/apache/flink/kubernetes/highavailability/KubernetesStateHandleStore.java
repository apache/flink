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

import org.apache.flink.annotation.VisibleForTesting;
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
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.InstantiationUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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
 * @param <T> Type of the state we're storing.
 */
public class KubernetesStateHandleStore<T extends Serializable>
        implements StateHandleStore<T, StringResourceVersion> {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesStateHandleStore.class);

    private static <T extends Serializable> StateHandleWithDeleteMarker<T> deserializeStateHandle(
            String content) throws IOException {
        checkNotNull(content, "Content should not be null.");
        final byte[] data = Base64.getDecoder().decode(content);
        try {
            return deserialize(data);
        } catch (IOException | ClassNotFoundException e) {
            throw new IOException(
                    String.format(
                            "Failed to deserialize state handle from ConfigMap data %s.", content),
                    e);
        }
    }

    private static String toBase64(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    @VisibleForTesting
    static String serializeStateHandle(StateHandleWithDeleteMarker<?> stateHandle)
            throws IOException {
        return toBase64(InstantiationUtil.serializeObject(stateHandle));
    }

    /**
     * Wrapper around state object that allows us to implement idempotent {@link
     * #releaseAndTryRemove(String)}.
     *
     * @param <T> Type of the state we're storing.
     */
    @VisibleForTesting
    static class StateHandleWithDeleteMarker<T extends Serializable> implements StateObject {

        private final RetrievableStateHandle<T> inner;
        private final boolean markedForDeletion;

        StateHandleWithDeleteMarker(RetrievableStateHandle<T> inner) {
            this(inner, false);
        }

        private StateHandleWithDeleteMarker(
                RetrievableStateHandle<T> inner, boolean markedForDeletion) {
            this.inner = inner;
            this.markedForDeletion = markedForDeletion;
        }

        @Override
        public void discardState() throws Exception {
            inner.discardState();
        }

        @Override
        public long getStateSize() {
            return inner.getStateSize();
        }

        RetrievableStateHandle<T> getInner() {
            return inner;
        }

        boolean isMarkedForDeletion() {
            return markedForDeletion;
        }

        StateHandleWithDeleteMarker<T> toDeleting() {
            return new StateHandleWithDeleteMarker<>(inner, true);
        }
    }

    private final FlinkKubeClient kubeClient;

    private final String configMapName;

    private final RetrievableStateStorageHelper<T> storage;

    private final Predicate<String> configMapKeyFilter;

    @Nullable private final String lockIdentity;

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
            @Nullable String lockIdentity) {

        this.kubeClient = checkNotNull(kubeClient, "Kubernetes client");
        this.storage = checkNotNull(storage, "State storage");
        this.configMapName = checkNotNull(configMapName, "ConfigMap name");
        this.configMapKeyFilter = checkNotNull(configMapKeyFilter);
        this.lockIdentity = lockIdentity;
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

        final byte[] serializedStoreHandle =
                serializeOrDiscard(new StateHandleWithDeleteMarker<>(storeHandle));

        // initialize flag to serve the failure case
        boolean discardState = true;
        try {
            // a successful operation will result in the state not being discarded
            discardState =
                    !updateConfigMap(
                                    cm -> {
                                        try {
                                            return addEntry(cm, key, serializedStoreHandle);
                                        } catch (Exception e) {
                                            throw new CompletionException(e);
                                        }
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

        final RetrievableStateHandle<T> newStateHandle = storage.store(state);

        final byte[] serializedStateHandle =
                serializeOrDiscard(new StateHandleWithDeleteMarker<>(newStateHandle));

        // initialize flags to serve the failure case
        boolean discardOldState = false;
        boolean discardNewState = true;
        // We don't want to greedily pull the old state handle as we have to do that anyway in
        // replaceEntry method for check of delete markers.
        final AtomicReference<RetrievableStateHandle<T>> oldStateHandleRef =
                new AtomicReference<>();
        try {
            final boolean success =
                    updateConfigMap(
                                    cm -> {
                                        try {
                                            return replaceEntry(
                                                    cm,
                                                    key,
                                                    serializedStateHandle,
                                                    oldStateHandleRef);
                                        } catch (NotExistException e) {
                                            throw new CompletionException(e);
                                        }
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
                Objects.requireNonNull(
                                oldStateHandleRef.get(),
                                "state handle should have been set on success")
                        .discardState();
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
                            final String content = configMap.getData().get(key);
                            if (content != null) {
                                try {
                                    final StateHandleWithDeleteMarker<T> stateHandle =
                                            deserializeStateHandle(content);
                                    if (stateHandle.isMarkedForDeletion()) {
                                        return StringResourceVersion.notExisting();
                                    }
                                } catch (IOException e) {
                                    // Any calls to add or replace will try to remove this resource,
                                    // so we can simply treat it as non-existent.
                                    return StringResourceVersion.notExisting();
                                }
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
                final StateHandleWithDeleteMarker<T> result =
                        deserializeStateHandle(configMap.getData().get(key));
                if (result.isMarkedForDeletion()) {
                    throw getKeyMarkedAsDeletedException(key);
                }
                return result.getInner();
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
                                                    final StateHandleWithDeleteMarker<T> result =
                                                            deserializeStateHandle(
                                                                    entry.getValue());
                                                    if (!result.isMarkedForDeletion()) {
                                                        stateHandles.add(
                                                                new Tuple2<>(
                                                                        result.getInner(),
                                                                        entry.getKey()));
                                                    }
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
                                        .filter(
                                                k -> {
                                                    try {
                                                        final String content =
                                                                Objects.requireNonNull(
                                                                        configMap.getData().get(k));
                                                        return !deserializeStateHandle(content)
                                                                .isMarkedForDeletion();
                                                    } catch (IOException e) {
                                                        return false;
                                                    }
                                                })
                                        .collect(Collectors.toList()))
                .orElseThrow(this::getConfigMapNotExistException);
    }

    /**
     * Remove the key in state config map. As well as the state on external storage will be removed.
     * It returns the {@link RetrievableStateHandle} stored under the given state node if any.
     *
     * @param key Key to be removed from ConfigMap
     * @return True if the state handle isn't listed anymore.
     * @throws Exception if removing the key or discarding the state failed
     */
    @Override
    public boolean releaseAndTryRemove(String key) throws Exception {
        checkNotNull(key, "Key in ConfigMap.");
        final AtomicReference<RetrievableStateHandle<T>> stateHandleRefer = new AtomicReference<>();
        final AtomicBoolean stateHandleDoesNotExist = new AtomicBoolean(false);
        return updateConfigMap(
                        configMap -> {
                            final String content = configMap.getData().get(key);
                            if (content != null) {
                                try {
                                    final StateHandleWithDeleteMarker<T> result =
                                            deserializeStateHandle(content);
                                    if (!result.isMarkedForDeletion()) {
                                        // Mark the ConfigMap entry as deleting. This basically
                                        // starts a "removal transaction" that allows us to retry
                                        // the removal if needed.
                                        configMap
                                                .getData()
                                                .put(
                                                        key,
                                                        serializeStateHandle(result.toDeleting()));
                                    }
                                    stateHandleRefer.set(result.getInner());
                                } catch (IOException e) {
                                    logInvalidEntry(key, configMapName, e);
                                    // Remove entry from the config map as we can't recover from
                                    // this (the serialization would fail on the retry as well).
                                    Objects.requireNonNull(configMap.getData().remove(key));
                                }
                                return Optional.of(configMap);
                            } else {
                                stateHandleDoesNotExist.set(true);
                            }
                            return Optional.empty();
                        })
                .thenCompose(
                        updated -> {
                            if (updated && stateHandleRefer.get() != null) {
                                try {
                                    stateHandleRefer.get().discardState();
                                    return updateConfigMap(
                                            configMap -> {
                                                // Now we can safely commit the "removal
                                                // transaction" by removing the entry from the
                                                // ConfigMap.
                                                configMap.getData().remove(key);
                                                return Optional.of(configMap);
                                            });
                                } catch (Exception e) {
                                    throw new CompletionException(e);
                                }
                            }
                            return CompletableFuture.completedFuture(
                                    stateHandleDoesNotExist.get() || updated);
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
        updateConfigMap(
                        configMap -> {
                            configMap.getData().keySet().removeIf(configMapKeyFilter);
                            return Optional.of(configMap);
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

    private boolean isValidOperation(KubernetesConfigMap c) {
        return lockIdentity == null || KubernetesLeaderElector.hasLeadership(c, lockIdentity);
    }

    @VisibleForTesting
    CompletableFuture<Boolean> updateConfigMap(
            Function<KubernetesConfigMap, Optional<KubernetesConfigMap>> updateFn) {
        return kubeClient.checkAndUpdateConfigMap(
                configMapName,
                configMap -> {
                    if (isValidOperation(configMap)) {
                        return updateFn.apply(configMap);
                    }
                    return Optional.empty();
                });
    }

    /**
     * Adds entry into the ConfigMap. If the entry already exists and contains delete marker, we try
     * to finish the removal before the actual update.
     */
    private Optional<KubernetesConfigMap> addEntry(
            KubernetesConfigMap configMap, String key, byte[] serializedStateHandle)
            throws Exception {
        final String oldBase64Content = configMap.getData().get(key);
        final String newBase64Content = toBase64(serializedStateHandle);
        if (oldBase64Content != null) {
            try {
                final StateHandleWithDeleteMarker<T> stateHandle =
                        deserializeStateHandle(oldBase64Content);
                if (stateHandle.isMarkedForDeletion()) {
                    // This might be a left-over after the fail-over. As the remove operation is
                    // idempotent let's try to finish it.
                    if (!releaseAndTryRemove(key)) {
                        throw new IllegalStateException(
                                "Unable to remove the marked as deleting entry.");
                    }
                } else {
                    // It could happen that the kubernetes client retries a transaction that has
                    // already succeeded due to network issues. So we simply ignore when the
                    // new content is same as the existing one.
                    if (oldBase64Content.equals(newBase64Content)) {
                        return Optional.of(configMap);
                    }
                    throw getKeyAlreadyExistException(key);
                }
            } catch (IOException e) {
                // Just log the invalid entry, it will be overridden
                // by the update code path below.
                logInvalidEntry(key, configMapName, e);
            }
        }
        configMap.getData().put(key, newBase64Content);
        return Optional.of(configMap);
    }

    /**
     * Replace the entry in the ConfigMap. If the entry already exists and contains delete marker,
     * we treat it as non-existent and perform the best effort removal.
     */
    private Optional<KubernetesConfigMap> replaceEntry(
            KubernetesConfigMap configMap,
            String key,
            byte[] serializedStateHandle,
            AtomicReference<RetrievableStateHandle<T>> oldStateHandleRef)
            throws NotExistException {
        final String content = configMap.getData().get(key);
        if (content != null) {
            try {
                final StateHandleWithDeleteMarker<T> stateHandle = deserializeStateHandle(content);
                oldStateHandleRef.set(stateHandle.getInner());
                if (stateHandle.isMarkedForDeletion()) {
                    final NotExistException exception = getKeyNotExistException(key);
                    try {
                        // Try to finish the removal. We don't really care whether this succeeds or
                        // not, from the "replace" point of view, the entry doesn't exist.
                        releaseAndTryRemove(key);
                    } catch (Exception e) {
                        exception.addSuppressed(e);
                    }
                    throw exception;
                }
            } catch (IOException e) {
                // Just log the invalid entry, it will be removed by the update code path below.
                logInvalidEntry(key, configMapName, e);
            }
            configMap.getData().put(key, toBase64(serializedStateHandle));
            return Optional.of(configMap);
        }
        throw getKeyNotExistException(key);
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

    private NotExistException getKeyMarkedAsDeletedException(String key) {
        return new NotExistException(
                "Already marked for deletion " + key + " in ConfigMap " + configMapName);
    }

    private AlreadyExistException getKeyAlreadyExistException(String key) {
        return new AlreadyExistException(key + " already exists in ConfigMap " + configMapName);
    }

    private static void logInvalidEntry(String key, String configMapName, Throwable e) {
        LOG.warn(
                "Could not retrieve the state handle of '{}' from ConfigMap '{}'. Removing the entry as we don't have any way to recover.",
                key,
                configMapName,
                e);
    }
}
