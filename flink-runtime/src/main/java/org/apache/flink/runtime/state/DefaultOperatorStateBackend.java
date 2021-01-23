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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.RunnableFuture;

/** Default implementation of OperatorStateStore that provides the ability to make snapshots. */
@Internal
public class DefaultOperatorStateBackend implements OperatorStateBackend {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultOperatorStateBackend.class);

    /** The default namespace for state in cases where no state name is provided */
    public static final String DEFAULT_OPERATOR_STATE_NAME = "_default_";

    /** Map for all registered operator states. Maps state name -> state */
    private final Map<String, PartitionableListState<?>> registeredOperatorStates;

    /** Map for all registered operator broadcast states. Maps state name -> state */
    private final Map<String, BackendWritableBroadcastState<?, ?>> registeredBroadcastStates;

    /** CloseableRegistry to participate in the tasks lifecycle. */
    private final CloseableRegistry closeStreamOnCancelRegistry;

    /** Default typeSerializer. Only used for the default operator state. */
    private final JavaSerializer<Serializable> deprecatedDefaultJavaSerializer =
            new JavaSerializer<>();

    /** The execution configuration. */
    private final ExecutionConfig executionConfig;

    /**
     * Cache of already accessed states.
     *
     * <p>In contrast to {@link #registeredOperatorStates} which may be repopulated with restored
     * state, this map is always empty at the beginning.
     *
     * <p>TODO this map should be moved to a base class once we have proper hierarchy for the
     * operator state backends.
     *
     * @see <a href="https://issues.apache.org/jira/browse/FLINK-6849">FLINK-6849</a>
     */
    private final Map<String, PartitionableListState<?>> accessedStatesByName;

    private final Map<String, BackendWritableBroadcastState<?, ?>> accessedBroadcastStatesByName;

    private final AbstractSnapshotStrategy<OperatorStateHandle> snapshotStrategy;

    public DefaultOperatorStateBackend(
            ExecutionConfig executionConfig,
            CloseableRegistry closeStreamOnCancelRegistry,
            Map<String, PartitionableListState<?>> registeredOperatorStates,
            Map<String, BackendWritableBroadcastState<?, ?>> registeredBroadcastStates,
            Map<String, PartitionableListState<?>> accessedStatesByName,
            Map<String, BackendWritableBroadcastState<?, ?>> accessedBroadcastStatesByName,
            AbstractSnapshotStrategy<OperatorStateHandle> snapshotStrategy) {
        this.closeStreamOnCancelRegistry = closeStreamOnCancelRegistry;
        this.executionConfig = executionConfig;
        this.registeredOperatorStates = registeredOperatorStates;
        this.registeredBroadcastStates = registeredBroadcastStates;
        this.accessedStatesByName = accessedStatesByName;
        this.accessedBroadcastStatesByName = accessedBroadcastStatesByName;
        this.snapshotStrategy = snapshotStrategy;
    }

    public ExecutionConfig getExecutionConfig() {
        return executionConfig;
    }

    @Override
    public Set<String> getRegisteredStateNames() {
        return registeredOperatorStates.keySet();
    }

    @Override
    public Set<String> getRegisteredBroadcastStateNames() {
        return registeredBroadcastStates.keySet();
    }

    @Override
    public void close() throws IOException {
        closeStreamOnCancelRegistry.close();
    }

    @Override
    public void dispose() {
        IOUtils.closeQuietly(closeStreamOnCancelRegistry);
        registeredOperatorStates.clear();
        registeredBroadcastStates.clear();
    }

    // -------------------------------------------------------------------------------------------
    //  State access methods
    // -------------------------------------------------------------------------------------------

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> BroadcastState<K, V> getBroadcastState(
            final MapStateDescriptor<K, V> stateDescriptor) throws StateMigrationException {

        Preconditions.checkNotNull(stateDescriptor);
        String name = Preconditions.checkNotNull(stateDescriptor.getName());

        BackendWritableBroadcastState<K, V> previous =
                (BackendWritableBroadcastState<K, V>) accessedBroadcastStatesByName.get(name);

        if (previous != null) {
            checkStateNameAndMode(
                    previous.getStateMetaInfo().getName(),
                    name,
                    previous.getStateMetaInfo().getAssignmentMode(),
                    OperatorStateHandle.Mode.BROADCAST);
            return previous;
        }

        stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());
        TypeSerializer<K> broadcastStateKeySerializer =
                Preconditions.checkNotNull(stateDescriptor.getKeySerializer());
        TypeSerializer<V> broadcastStateValueSerializer =
                Preconditions.checkNotNull(stateDescriptor.getValueSerializer());

        BackendWritableBroadcastState<K, V> broadcastState =
                (BackendWritableBroadcastState<K, V>) registeredBroadcastStates.get(name);

        if (broadcastState == null) {
            broadcastState =
                    new HeapBroadcastState<>(
                            new RegisteredBroadcastStateBackendMetaInfo<>(
                                    name,
                                    OperatorStateHandle.Mode.BROADCAST,
                                    broadcastStateKeySerializer,
                                    broadcastStateValueSerializer));
            registeredBroadcastStates.put(name, broadcastState);
        } else {
            // has restored state; check compatibility of new state access

            checkStateNameAndMode(
                    broadcastState.getStateMetaInfo().getName(),
                    name,
                    broadcastState.getStateMetaInfo().getAssignmentMode(),
                    OperatorStateHandle.Mode.BROADCAST);

            RegisteredBroadcastStateBackendMetaInfo<K, V> restoredBroadcastStateMetaInfo =
                    broadcastState.getStateMetaInfo();

            // check whether new serializers are incompatible
            TypeSerializerSchemaCompatibility<K> keyCompatibility =
                    restoredBroadcastStateMetaInfo.updateKeySerializer(broadcastStateKeySerializer);
            if (keyCompatibility.isIncompatible()) {
                throw new StateMigrationException(
                        "The new key typeSerializer for broadcast state must not be incompatible.");
            }

            TypeSerializerSchemaCompatibility<V> valueCompatibility =
                    restoredBroadcastStateMetaInfo.updateValueSerializer(
                            broadcastStateValueSerializer);
            if (valueCompatibility.isIncompatible()) {
                throw new StateMigrationException(
                        "The new value typeSerializer for broadcast state must not be incompatible.");
            }

            broadcastState.setStateMetaInfo(restoredBroadcastStateMetaInfo);
        }

        accessedBroadcastStatesByName.put(name, broadcastState);
        return broadcastState;
    }

    @Override
    public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) throws Exception {
        return getListState(stateDescriptor, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE);
    }

    @Override
    public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor)
            throws Exception {
        return getListState(stateDescriptor, OperatorStateHandle.Mode.UNION);
    }

    // -------------------------------------------------------------------------------------------
    //  Snapshot
    // -------------------------------------------------------------------------------------------
    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {

        long syncStartTime = System.currentTimeMillis();

        RunnableFuture<SnapshotResult<OperatorStateHandle>> snapshotRunner =
                snapshotStrategy.snapshot(
                        checkpointId, timestamp, streamFactory, checkpointOptions);

        snapshotStrategy.logSyncCompleted(streamFactory, syncStartTime);
        return snapshotRunner;
    }

    private <S> ListState<S> getListState(
            ListStateDescriptor<S> stateDescriptor, OperatorStateHandle.Mode mode)
            throws StateMigrationException {

        Preconditions.checkNotNull(stateDescriptor);
        String name = Preconditions.checkNotNull(stateDescriptor.getName());

        @SuppressWarnings("unchecked")
        PartitionableListState<S> previous =
                (PartitionableListState<S>) accessedStatesByName.get(name);
        if (previous != null) {
            checkStateNameAndMode(
                    previous.getStateMetaInfo().getName(),
                    name,
                    previous.getStateMetaInfo().getAssignmentMode(),
                    mode);
            return previous;
        }

        // end up here if its the first time access after execution for the
        // provided state name; check compatibility of restored state, if any
        // TODO with eager registration in place, these checks should be moved to restore()

        stateDescriptor.initializeSerializerUnlessSet(getExecutionConfig());
        TypeSerializer<S> partitionStateSerializer =
                Preconditions.checkNotNull(stateDescriptor.getElementSerializer());

        @SuppressWarnings("unchecked")
        PartitionableListState<S> partitionableListState =
                (PartitionableListState<S>) registeredOperatorStates.get(name);

        if (null == partitionableListState) {
            // no restored state for the state name; simply create new state holder

            partitionableListState =
                    new PartitionableListState<>(
                            new RegisteredOperatorStateBackendMetaInfo<>(
                                    name, partitionStateSerializer, mode));

            registeredOperatorStates.put(name, partitionableListState);
        } else {
            // has restored state; check compatibility of new state access

            checkStateNameAndMode(
                    partitionableListState.getStateMetaInfo().getName(),
                    name,
                    partitionableListState.getStateMetaInfo().getAssignmentMode(),
                    mode);

            RegisteredOperatorStateBackendMetaInfo<S> restoredPartitionableListStateMetaInfo =
                    partitionableListState.getStateMetaInfo();

            // check compatibility to determine if new serializers are incompatible
            TypeSerializer<S> newPartitionStateSerializer = partitionStateSerializer.duplicate();

            TypeSerializerSchemaCompatibility<S> stateCompatibility =
                    restoredPartitionableListStateMetaInfo.updatePartitionStateSerializer(
                            newPartitionStateSerializer);
            if (stateCompatibility.isIncompatible()) {
                throw new StateMigrationException(
                        "The new state typeSerializer for operator state must not be incompatible.");
            }

            partitionableListState.setStateMetaInfo(restoredPartitionableListStateMetaInfo);
        }

        accessedStatesByName.put(name, partitionableListState);
        return partitionableListState;
    }

    private static void checkStateNameAndMode(
            String actualName,
            String expectedName,
            OperatorStateHandle.Mode actualMode,
            OperatorStateHandle.Mode expectedMode) {

        Preconditions.checkState(
                actualName.equals(expectedName),
                "Incompatible state names. "
                        + "Was ["
                        + actualName
                        + "], "
                        + "registered with ["
                        + expectedName
                        + "].");

        Preconditions.checkState(
                actualMode.equals(expectedMode),
                "Incompatible state assignment modes. "
                        + "Was ["
                        + actualMode
                        + "], "
                        + "registered with ["
                        + expectedMode
                        + "].");
    }
}
