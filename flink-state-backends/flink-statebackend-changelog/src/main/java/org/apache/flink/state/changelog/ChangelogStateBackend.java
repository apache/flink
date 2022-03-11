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

package org.apache.flink.state.changelog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointBoundKeyedStateHandle;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SavepointKeyedStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle.ChangelogStateBackendHandleImpl;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.delegate.DelegatingStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskmanager.AsynchronousException;
import org.apache.flink.state.changelog.restore.ChangelogBackendRestoreOperation;
import org.apache.flink.state.changelog.restore.ChangelogBackendRestoreOperation.BaseBackendBuilder;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This state backend holds the working state in the underlying delegatedStateBackend, and forwards
 * state changes to State Changelog.
 */
@Internal
public class ChangelogStateBackend implements DelegatingStateBackend, ConfigurableStateBackend {

    private static final long serialVersionUID = 1000L;

    private static final Logger LOG = LoggerFactory.getLogger(ChangelogStateBackend.class);

    private final StateBackend delegatedStateBackend;

    /**
     * Delegate a state backend by a ChangelogStateBackend.
     *
     * <p>As FLINK-22678 mentioned, we currently hide this constructor from user.
     *
     * @param stateBackend the delegated state backend.
     */
    ChangelogStateBackend(StateBackend stateBackend) {
        this.delegatedStateBackend = Preconditions.checkNotNull(stateBackend);

        Preconditions.checkArgument(
                !(stateBackend instanceof DelegatingStateBackend),
                "Recursive Delegation is not supported.");

        LOG.info(
                "ChangelogStateBackend is used, delegating {}.",
                delegatedStateBackend.getClass().getSimpleName());
    }

    @Override
    public <K> ChangelogKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws Exception {
        return restore(
                env,
                operatorIdentifier,
                keyGroupRange,
                ttlTimeProvider,
                stateHandles,
                baseHandles ->
                        (AbstractKeyedStateBackend<K>)
                                delegatedStateBackend.createKeyedStateBackend(
                                        env,
                                        jobID,
                                        operatorIdentifier,
                                        keySerializer,
                                        numberOfKeyGroups,
                                        keyGroupRange,
                                        kvStateRegistry,
                                        ttlTimeProvider,
                                        metricGroup,
                                        baseHandles,
                                        cancelStreamRegistry));
    }

    @Override
    public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
            Environment env,
            JobID jobID,
            String operatorIdentifier,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry,
            double managedMemoryFraction)
            throws Exception {
        return restore(
                env,
                operatorIdentifier,
                keyGroupRange,
                ttlTimeProvider,
                stateHandles,
                baseHandles ->
                        (AbstractKeyedStateBackend<K>)
                                delegatedStateBackend.createKeyedStateBackend(
                                        env,
                                        jobID,
                                        operatorIdentifier,
                                        keySerializer,
                                        numberOfKeyGroups,
                                        keyGroupRange,
                                        kvStateRegistry,
                                        ttlTimeProvider,
                                        metricGroup,
                                        baseHandles,
                                        cancelStreamRegistry,
                                        managedMemoryFraction));
    }

    @Override
    public OperatorStateBackend createOperatorStateBackend(
            Environment env,
            String operatorIdentifier,
            @Nonnull Collection<OperatorStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry)
            throws Exception {
        return delegatedStateBackend.createOperatorStateBackend(
                env, operatorIdentifier, stateHandles, cancelStreamRegistry);
    }

    @Override
    public boolean useManagedMemory() {
        return delegatedStateBackend.useManagedMemory();
    }

    @Override
    public StateBackend getDelegatedStateBackend() {
        return delegatedStateBackend;
    }

    @Override
    public StateBackend configure(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {

        if (delegatedStateBackend instanceof ConfigurableStateBackend) {
            return new ChangelogStateBackend(
                    ((ConfigurableStateBackend) delegatedStateBackend)
                            .configure(config, classLoader));
        }

        return this;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <K> ChangelogKeyedStateBackend<K> restore(
            Environment env,
            String operatorIdentifier,
            KeyGroupRange keyGroupRange,
            TtlTimeProvider ttlTimeProvider,
            Collection<KeyedStateHandle> stateHandles,
            BaseBackendBuilder<K> baseBackendBuilder)
            throws Exception {
        StateChangelogStorage<?> changelogStorage =
                Preconditions.checkNotNull(
                        env.getTaskStateManager().getStateChangelogStorage(),
                        "Changelog storage is null when creating and restoring"
                                + " the ChangelogKeyedStateBackend.");

        String subtaskName = env.getTaskInfo().getTaskNameWithSubtasks();
        ExecutionConfig executionConfig = env.getExecutionConfig();

        Collection<ChangelogStateBackendHandle> stateBackendHandles = castHandles(stateHandles);
        ChangelogKeyedStateBackend<K> keyedStateBackend =
                ChangelogBackendRestoreOperation.restore(
                        changelogStorage.createReader(),
                        env.getUserCodeClassLoader().asClassLoader(),
                        stateBackendHandles,
                        baseBackendBuilder,
                        (baseBackend, baseState) ->
                                new ChangelogKeyedStateBackend(
                                        baseBackend,
                                        subtaskName,
                                        executionConfig,
                                        ttlTimeProvider,
                                        changelogStorage.createWriter(
                                                operatorIdentifier, keyGroupRange),
                                        baseState,
                                        env.getCheckpointStorageAccess()));

        PeriodicMaterializationManager periodicMaterializationManager =
                new PeriodicMaterializationManager(
                        checkNotNull(env.getMainMailboxExecutor()),
                        checkNotNull(env.getAsyncOperationsThreadPool()),
                        subtaskName,
                        (message, exception) ->
                                env.failExternally(new AsynchronousException(message, exception)),
                        keyedStateBackend,
                        executionConfig.getPeriodicMaterializeIntervalMillis(),
                        executionConfig.getMaterializationMaxAllowedFailures(),
                        operatorIdentifier);

        // keyedStateBackend is responsible to close periodicMaterializationManager
        // This indicates periodicMaterializationManager binds to the keyedStateBackend
        // However PeriodicMaterializationManager can not be part of keyedStateBackend
        // because of cyclic reference
        keyedStateBackend.registerCloseable(periodicMaterializationManager);

        periodicMaterializationManager.start();

        return keyedStateBackend;
    }

    private Collection<ChangelogStateBackendHandle> castHandles(
            Collection<KeyedStateHandle> stateHandles) {
        if (stateHandles.stream().anyMatch(h -> !(h instanceof ChangelogStateBackendHandle))) {
            LOG.warn(
                    "Some state handles do not contain changelog: {} (ok if recovery from a savepoint)",
                    stateHandles);
        }
        return stateHandles.stream()
                .filter(Objects::nonNull)
                .map(this::getChangelogStateBackendHandle)
                .collect(Collectors.toList());
    }

    private ChangelogStateBackendHandle getChangelogStateBackendHandle(
            KeyedStateHandle keyedStateHandle) {
        if (keyedStateHandle instanceof ChangelogStateBackendHandle) {
            return (ChangelogStateBackendHandle) keyedStateHandle;
        } else if (keyedStateHandle instanceof SavepointKeyedStateHandle) {
            return new ChangelogStateBackendHandleImpl(
                    singletonList(keyedStateHandle),
                    emptyList(),
                    keyedStateHandle.getKeyGroupRange(),
                    getMaterializationID(keyedStateHandle),
                    0L);
        } else {
            throw new IllegalStateException(
                    String.format(
                            "Recovery not supported from %s with Changelog enabled. Consider taking a savepoint in %s format.",
                            keyedStateHandle.getClass(), SavepointFormatType.CANONICAL));
        }
    }

    private long getMaterializationID(KeyedStateHandle keyedStateHandle) {
        if (keyedStateHandle instanceof CheckpointBoundKeyedStateHandle) {
            return ((CheckpointBoundKeyedStateHandle) keyedStateHandle).getCheckpointId();
        } else {
            return 0L;
        }
    }
}
