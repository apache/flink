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
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.state.changelog.restore.ChangelogBackendRestoreOperation;
import org.apache.flink.state.changelog.restore.ChangelogBackendRestoreOperation.BaseBackendBuilder;
import org.apache.flink.state.changelog.restore.ChangelogMigrationRestoreTarget;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * This state backend use delegatedStateBackend and State changes to restore to the
 * delegatedStateBackend in which switching Changelog from enabled to disabled.
 */
@Internal
public class DeactivatedChangelogStateBackend extends AbstractChangelogStateBackend {

    private static final long serialVersionUID = 1000L;

    DeactivatedChangelogStateBackend(StateBackend stateBackend) {
        super(stateBackend);
    }

    @Override
    protected <K> CheckpointableKeyedStateBackend<K> restore(
            Environment env,
            String operatorIdentifier,
            KeyGroupRange keyGroupRange,
            TtlTimeProvider ttlTimeProvider,
            MetricGroup metricGroup,
            Collection<ChangelogStateBackendHandle> stateBackendHandles,
            BaseBackendBuilder<K> baseBackendBuilder)
            throws Exception {
        // ChangelogKeyedStateBackend will use materialization id as the checkpoint id of delegated
        // state backend.
        // While switching Changelog from enabled to disabled,
        // the materialization id will be passed into the delegated keyed state backend whose
        // KeyedStateHandle implements CheckpointBoundKeyedStateHandle.
        // It will cause unforeseen exception because we couldn't know what the delegated keyed
        // state backend will do with checkpoint id.
        // So we need to rebound the checkpoint id to the real checkpoint id here.
        stateBackendHandles = reboundCheckpoint(stateBackendHandles);
        ChangelogStateFactory changelogStateFactory = new ChangelogStateFactory();

        return ChangelogBackendRestoreOperation.restore(
                env.getTaskManagerInfo().getConfiguration(),
                env.getUserCodeClassLoader().asClassLoader(),
                env.getTaskStateManager(),
                stateBackendHandles,
                baseBackendBuilder,
                (baseBackend, baseState) ->
                        new ChangelogMigrationRestoreTarget<>(baseBackend, changelogStateFactory));
    }

    private Collection<ChangelogStateBackendHandle> reboundCheckpoint(
            Collection<ChangelogStateBackendHandle> stateBackendHandles) {
        return stateBackendHandles.stream()
                .map(
                        changelogStateBackendHandle ->
                                changelogStateBackendHandle.rebound(
                                        changelogStateBackendHandle.getCheckpointId()))
                .collect(Collectors.toList());
    }
}
