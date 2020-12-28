/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.runtime.state;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.util.IOUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Builder class for {@link DefaultOperatorStateBackend} which handles all necessary initializations
 * and clean ups.
 */
public class DefaultOperatorStateBackendBuilder
        implements StateBackendBuilder<DefaultOperatorStateBackend, BackendBuildingException> {
    /** The user code classloader. */
    @VisibleForTesting protected final ClassLoader userClassloader;
    /** The execution configuration. */
    @VisibleForTesting protected final ExecutionConfig executionConfig;
    /** Flag to de/activate asynchronous snapshots. */
    @VisibleForTesting protected final boolean asynchronousSnapshots;
    /** State handles for restore. */
    @VisibleForTesting protected final Collection<OperatorStateHandle> restoreStateHandles;

    @VisibleForTesting protected final CloseableRegistry cancelStreamRegistry;

    public DefaultOperatorStateBackendBuilder(
            ClassLoader userClassloader,
            ExecutionConfig executionConfig,
            boolean asynchronousSnapshots,
            Collection<OperatorStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry) {
        this.userClassloader = userClassloader;
        this.executionConfig = executionConfig;
        this.asynchronousSnapshots = asynchronousSnapshots;
        this.restoreStateHandles = stateHandles;
        this.cancelStreamRegistry = cancelStreamRegistry;
    }

    @Override
    public DefaultOperatorStateBackend build() throws BackendBuildingException {
        Map<String, PartitionableListState<?>> registeredOperatorStates = new HashMap<>();
        Map<String, BackendWritableBroadcastState<?, ?>> registeredBroadcastStates =
                new HashMap<>();
        CloseableRegistry cancelStreamRegistryForBackend = new CloseableRegistry();
        AbstractSnapshotStrategy<OperatorStateHandle> snapshotStrategy =
                new DefaultOperatorStateBackendSnapshotStrategy(
                        userClassloader,
                        asynchronousSnapshots,
                        registeredOperatorStates,
                        registeredBroadcastStates,
                        cancelStreamRegistryForBackend);
        OperatorStateRestoreOperation restoreOperation =
                new OperatorStateRestoreOperation(
                        cancelStreamRegistry,
                        userClassloader,
                        registeredOperatorStates,
                        registeredBroadcastStates,
                        restoreStateHandles);
        try {
            restoreOperation.restore();
        } catch (Exception e) {
            IOUtils.closeQuietly(cancelStreamRegistryForBackend);
            throw new BackendBuildingException(
                    "Failed when trying to restore operator state backend", e);
        }
        return new DefaultOperatorStateBackend(
                executionConfig,
                cancelStreamRegistryForBackend,
                registeredOperatorStates,
                registeredBroadcastStates,
                new HashMap<>(),
                new HashMap<>(),
                snapshotStrategy);
    }
}
