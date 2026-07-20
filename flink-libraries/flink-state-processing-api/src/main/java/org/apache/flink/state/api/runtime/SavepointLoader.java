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

package org.apache.flink.state.api.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CustomRestoreSerializerFactory;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorBackendSerializationProxy;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.input.deserializer.MissingClassSerializerFactory;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Utility class for loading savepoint metadata and operator state information. */
@Internal
public final class SavepointLoader {
    private SavepointLoader() {}

    /**
     * Operator-level metadata loaded in a single I/O pass: the per-state serializer snapshots and
     * the backend key serializer snapshot.
     */
    public static final class OperatorStateMetadata {

        /** Per-state serializer snapshots, keyed by state name. */
        public final Map<String, StateMetaInfoSnapshot> stateSnapshots;

        /**
         * The key serializer snapshot shared by all states in the operator's keyed backend, or
         * {@code null} if none is available.
         */
        @Nullable public final TypeSerializerSnapshot<?> keySerializerSnapshot;

        OperatorStateMetadata(
                Map<String, StateMetaInfoSnapshot> stateSnapshots,
                @Nullable TypeSerializerSnapshot<?> keySerializerSnapshot) {
            this.stateSnapshots = stateSnapshots;
            this.keySerializerSnapshot = keySerializerSnapshot;
        }
    }

    /**
     * Non-keyed (operator) state metadata loaded in a single I/O pass. List/union and broadcast
     * states are kept in separate maps since a list/union state may share its name with a
     * differently-typed broadcast state.
     */
    public static final class NonKeyedOperatorStateMetadata {

        /** Per-state serializer snapshots for {@code ListState}/{@code UnionState}, by name. */
        public final Map<String, StateMetaInfoSnapshot> operatorStateSnapshots;

        /** Per-state serializer snapshots for {@code BroadcastState}, by name. */
        public final Map<String, StateMetaInfoSnapshot> broadcastStateSnapshots;

        NonKeyedOperatorStateMetadata(
                Map<String, StateMetaInfoSnapshot> operatorStateSnapshots,
                Map<String, StateMetaInfoSnapshot> broadcastStateSnapshots) {
            this.operatorStateSnapshots = operatorStateSnapshots;
            this.broadcastStateSnapshots = broadcastStateSnapshots;
        }
    }

    /**
     * Takes the given string (representing a pointer to a checkpoint) and resolves it to a file
     * status for the checkpoint's metadata file.
     *
     * <p>This should only be used when the user code class loader is the current classloader for
     * the thread.
     *
     * @param savepointPath The path to an external savepoint.
     * @return A state handle to savepoint's metadata.
     * @throws IOException Thrown, if the path cannot be resolved, the file system not accessed, or
     *     the path points to a location that does not seem to be a savepoint.
     */
    public static CheckpointMetadata loadSavepointMetadata(String savepointPath)
            throws IOException {
        CompletedCheckpointStorageLocation location =
                AbstractFsCheckpointStorageAccess.resolveCheckpointPointer(savepointPath);

        try (DataInputStream stream =
                new DataInputStream(location.getMetadataHandle().openInputStream())) {
            return Checkpoints.loadCheckpointMetadata(
                    stream, Thread.currentThread().getContextClassLoader(), savepointPath);
        }
    }

    /**
     * Loads all state metadata for an operator in a single I/O operation.
     *
     * @param savepointPath Path to the savepoint directory
     * @param operatorIdentifier Operator UID or hash
     * @return Map from state name to StateMetaInfoSnapshot
     * @throws IOException If reading fails
     */
    public static Map<String, StateMetaInfoSnapshot> loadOperatorStateMetadata(
            String savepointPath, OperatorIdentifier operatorIdentifier) throws IOException {
        return loadOperatorMetadata(savepointPath, operatorIdentifier).stateSnapshots;
    }

    /**
     * Loads both the per-state serializer snapshots and the backend key serializer snapshot for an
     * operator in a single I/O operation.
     *
     * @param savepointPath Path to the savepoint directory
     * @param operatorIdentifier Operator UID or hash
     * @return combined operator metadata
     * @throws IOException If reading fails
     */
    public static OperatorStateMetadata loadOperatorMetadata(
            String savepointPath, OperatorIdentifier operatorIdentifier) throws IOException {

        OperatorState operatorState = findOperatorState(savepointPath, operatorIdentifier);

        KeyedStateHandle keyedStateHandle =
                operatorState.getStates().stream()
                        .flatMap(s -> s.getManagedKeyedState().stream())
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "No keyed state found for operator "
                                                        + operatorIdentifier));

        KeyedBackendSerializationProxy<?> proxy = readSerializationProxy(keyedStateHandle);
        return new OperatorStateMetadata(
                byStateName(proxy.getStateMetaInfoSnapshots()), proxy.getKeySerializerSnapshot());
    }

    /**
     * Loads the per-state serializer snapshots of an operator's non-keyed (operator) state — {@code
     * ListState}/{@code UnionState}/{@code BroadcastState} — in a single I/O operation.
     */
    public static NonKeyedOperatorStateMetadata loadNonKeyedOperatorMetadata(
            String savepointPath, OperatorIdentifier operatorIdentifier) throws IOException {

        OperatorState operatorState = findOperatorState(savepointPath, operatorIdentifier);

        OperatorStateHandle operatorStateHandle =
                operatorState.getStates().stream()
                        .flatMap(s -> s.getManagedOperatorState().stream())
                        .findFirst()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "No operator state found for operator "
                                                        + operatorIdentifier));

        OperatorBackendSerializationProxy proxy = readSerializationProxy(operatorStateHandle);
        return new NonKeyedOperatorStateMetadata(
                byStateName(proxy.getOperatorStateMetaInfoSnapshots()),
                byStateName(proxy.getBroadcastStateMetaInfoSnapshots()));
    }

    private static OperatorState findOperatorState(
            String savepointPath, OperatorIdentifier operatorIdentifier) throws IOException {
        return loadSavepointMetadata(savepointPath).getOperatorStates().stream()
                .filter(state -> operatorIdentifier.getOperatorId().equals(state.getOperatorID()))
                .findFirst()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Operator "
                                                + operatorIdentifier
                                                + " not found in savepoint"));
    }

    private static Map<String, StateMetaInfoSnapshot> byStateName(
            List<StateMetaInfoSnapshot> snapshots) {
        return snapshots.stream()
                .collect(Collectors.toMap(StateMetaInfoSnapshot::getName, Function.identity()));
    }

    private static KeyedBackendSerializationProxy<?> readSerializationProxy(
            KeyedStateHandle stateHandle) throws IOException {

        // KeyGroupsStateHandle (heap/HashMapStateBackend) is itself a StreamStateHandle whose
        // stream starts with the metadata header. IncrementalKeyedStateHandle (RocksDB) instead
        // keeps the metadata in a separate handle, exposed via getMetaDataStateHandle().
        StreamStateHandle streamStateHandle;
        if (stateHandle instanceof KeyGroupsStateHandle) {
            streamStateHandle = ((KeyGroupsStateHandle) stateHandle).getDelegateStateHandle();
        } else if (stateHandle instanceof IncrementalKeyedStateHandle) {
            streamStateHandle =
                    ((IncrementalKeyedStateHandle) stateHandle).getMetaDataStateHandle();
        } else {
            throw new IllegalArgumentException(
                    "Unsupported KeyedStateHandle type: " + stateHandle.getClass());
        }

        try (FSDataInputStream inputStream = streamStateHandle.openInputStream()) {
            DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

            KeyedBackendSerializationProxy<?> proxy =
                    new KeyedBackendSerializationProxy<>(
                            Thread.currentThread().getContextClassLoader());
            CustomRestoreSerializerFactory.set(MissingClassSerializerFactory::create);
            proxy.read(inputView);

            return proxy;
        }
    }

    private static OperatorBackendSerializationProxy readSerializationProxy(
            OperatorStateHandle stateHandle) throws IOException {

        // Unlike keyed state, an OperatorStateHandle is itself a StreamStateHandle whose stream
        // starts with the metadata header, for every state backend.
        try (FSDataInputStream inputStream = stateHandle.openInputStream()) {
            DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

            OperatorBackendSerializationProxy proxy =
                    new OperatorBackendSerializationProxy(
                            Thread.currentThread().getContextClassLoader());
            CustomRestoreSerializerFactory.set(MissingClassSerializerFactory::create);
            proxy.read(inputView);

            return proxy;
        }
    }
}
