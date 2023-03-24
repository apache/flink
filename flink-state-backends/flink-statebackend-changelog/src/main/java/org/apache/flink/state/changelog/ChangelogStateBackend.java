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
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.ConfigurableStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskmanager.AsynchronousException;
import org.apache.flink.state.changelog.restore.ChangelogBackendRestoreOperation;
import org.apache.flink.state.changelog.restore.ChangelogBackendRestoreOperation.BaseBackendBuilder;
import org.apache.flink.state.common.ChangelogMaterializationMetricGroup;
import org.apache.flink.state.common.PeriodicMaterializationManager;
import org.apache.flink.util.Preconditions;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This state backend holds the working state in the underlying delegatedStateBackend, and forwards
 * state changes to State Changelog.
 */
@Internal
public class ChangelogStateBackend extends AbstractChangelogStateBackend
        implements ConfigurableStateBackend {

    private static final long serialVersionUID = 1000L;

    ChangelogStateBackend(StateBackend stateBackend) {
        super(stateBackend);
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

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected <K> CheckpointableKeyedStateBackend<K> restore(
            Environment env,
            String operatorIdentifier,
            KeyGroupRange keyGroupRange,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            Collection<ChangelogStateBackendHandle> stateBackendHandles,
            BaseBackendBuilder<K> baseBackendBuilder)
            throws Exception {
        StateChangelogStorage<?> changelogStorage =
                Preconditions.checkNotNull(
                        env.getTaskStateManager().getStateChangelogStorage(),
                        "Changelog storage is null when creating and restoring"
                                + " the ChangelogKeyedStateBackend.");

        String subtaskName = env.getTaskInfo().getTaskNameWithSubtasks();
        ExecutionConfig executionConfig = env.getExecutionConfig();

        ChangelogStateFactory changelogStateFactory = new ChangelogStateFactory();
        CheckpointableKeyedStateBackend<K> keyedStateBackend =
                ChangelogBackendRestoreOperation.restore(
                        env.getTaskManagerInfo().getConfiguration(),
                        env.getUserCodeClassLoader().asClassLoader(),
                        env.getTaskStateManager(),
                        stateBackendHandles,
                        baseBackendBuilder,
                        (baseBackend, baseState) ->
                                new ChangelogKeyedStateBackend(
                                                baseBackend,
                                                subtaskName,
                                                executionConfig,
                                                ttlTimeProvider,
                                                new ChangelogStateBackendMetricGroup(metricGroup),
                                                changelogStorage.createWriter(
                                                        operatorIdentifier,
                                                        keyGroupRange,
                                                        env.getMainMailboxExecutor()),
                                                baseState,
                                                env.getCheckpointStorageAccess(),
                                                changelogStateFactory)
                                        .getChangelogRestoreTarget());

        ChangelogKeyedStateBackend<K> changelogKeyedStateBackend =
                (ChangelogKeyedStateBackend<K>) keyedStateBackend;
        PeriodicMaterializationManager periodicMaterializationManager =
                new PeriodicMaterializationManager(
                        checkNotNull(env.getMainMailboxExecutor()),
                        checkNotNull(env.getAsyncOperationsThreadPool()),
                        subtaskName,
                        (message, exception) ->
                                env.failExternally(new AsynchronousException(message, exception)),
                        changelogKeyedStateBackend,
                        new ChangelogMaterializationMetricGroup(metricGroup),
                        executionConfig.getPeriodicMaterializeIntervalMillis(),
                        executionConfig.getMaterializationMaxAllowedFailures(),
                        operatorIdentifier);

        // keyedStateBackend is responsible to close periodicMaterializationManager
        // This indicates periodicMaterializationManager binds to the keyedStateBackend
        // However PeriodicMaterializationManager can not be part of keyedStateBackend
        // because of cyclic reference
        changelogKeyedStateBackend.registerCloseable(periodicMaterializationManager);

        periodicMaterializationManager.start();

        return keyedStateBackend;
    }
}
