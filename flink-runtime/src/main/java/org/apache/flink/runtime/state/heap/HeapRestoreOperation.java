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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RestoreOperation;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.state.StateUtil.unexpectedStateHandleException;

/**
 * Implementation of heap restore operation.
 *
 * @param <K> The data type that the serializer serializes.
 */
public class HeapRestoreOperation<K> implements RestoreOperation<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(HeapRestoreOperation.class);

    private final Collection<KeyedStateHandle> restoreStateHandles;
    private final StateSerializerProvider<K> keySerializerProvider;
    private final ClassLoader userCodeClassLoader;
    private final Map<String, StateTable<K, ?, ?>> registeredKVStates;
    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;
    private final CloseableRegistry cancelStreamRegistry;
    @Nonnull private final KeyGroupRange keyGroupRange;
    private final HeapMetaInfoRestoreOperation<K> heapMetaInfoRestoreOperation;

    HeapRestoreOperation(
            @Nonnull Collection<KeyedStateHandle> restoreStateHandles,
            StateSerializerProvider<K> keySerializerProvider,
            ClassLoader userCodeClassLoader,
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            CloseableRegistry cancelStreamRegistry,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            @Nonnull KeyGroupRange keyGroupRange,
            int numberOfKeyGroups,
            StateTableFactory<K> stateTableFactory,
            InternalKeyContext<K> keyContext) {
        this.restoreStateHandles = restoreStateHandles;
        this.keySerializerProvider = keySerializerProvider;
        this.userCodeClassLoader = userCodeClassLoader;
        this.registeredKVStates = registeredKVStates;
        this.registeredPQStates = registeredPQStates;
        this.cancelStreamRegistry = cancelStreamRegistry;
        this.keyGroupRange = keyGroupRange;
        this.heapMetaInfoRestoreOperation =
                new HeapMetaInfoRestoreOperation<>(
                        keySerializerProvider,
                        priorityQueueSetFactory,
                        keyGroupRange,
                        numberOfKeyGroups,
                        stateTableFactory,
                        keyContext);
    }

    @Override
    public Void restore() throws Exception {

        registeredKVStates.clear();
        registeredPQStates.clear();

        boolean keySerializerRestored = false;

        for (KeyedStateHandle keyedStateHandle : restoreStateHandles) {

            if (keyedStateHandle == null) {
                continue;
            }

            if (!(keyedStateHandle instanceof KeyGroupsStateHandle)) {
                throw unexpectedStateHandleException(
                        KeyGroupsStateHandle.class, keyedStateHandle.getClass());
            }

            LOG.info("Starting to restore from state handle: {}.", keyedStateHandle);
            KeyGroupsStateHandle keyGroupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
            FSDataInputStream fsDataInputStream = keyGroupsStateHandle.openInputStream();
            cancelStreamRegistry.registerCloseable(fsDataInputStream);

            try {
                DataInputViewStreamWrapper inView =
                        new DataInputViewStreamWrapper(fsDataInputStream);

                KeyedBackendSerializationProxy<K> serializationProxy =
                        new KeyedBackendSerializationProxy<>(userCodeClassLoader);

                serializationProxy.read(inView);

                if (!keySerializerRestored) {
                    // fetch current serializer now because if it is incompatible, we can't access
                    // it anymore to improve the error message
                    TypeSerializer<K> currentSerializer =
                            keySerializerProvider.currentSchemaSerializer();
                    // check for key serializer compatibility; this also reconfigures the
                    // key serializer to be compatible, if it is required and is possible
                    TypeSerializerSchemaCompatibility<K> keySerializerSchemaCompat =
                            keySerializerProvider.setPreviousSerializerSnapshotForRestoredState(
                                    serializationProxy.getKeySerializerSnapshot());
                    if (keySerializerSchemaCompat.isCompatibleAfterMigration()
                            || keySerializerSchemaCompat.isIncompatible()) {
                        throw new StateMigrationException(
                                "The new key serializer ("
                                        + currentSerializer
                                        + ") must be compatible with the previous key serializer ("
                                        + keySerializerProvider.previousSchemaSerializer()
                                        + ").");
                    }

                    keySerializerRestored = true;
                }

                List<StateMetaInfoSnapshot> restoredMetaInfos =
                        serializationProxy.getStateMetaInfoSnapshots();

                final Map<Integer, StateMetaInfoSnapshot> kvStatesById =
                        this.heapMetaInfoRestoreOperation.createOrCheckStateForMetaInfo(
                                restoredMetaInfos, registeredKVStates, registeredPQStates);

                readStateHandleStateData(
                        fsDataInputStream,
                        inView,
                        keyGroupsStateHandle.getGroupRangeOffsets(),
                        kvStatesById,
                        restoredMetaInfos.size(),
                        serializationProxy.getReadVersion(),
                        serializationProxy.isUsingKeyGroupCompression());
                LOG.info("Finished restoring from state handle: {}.", keyedStateHandle);
            } finally {
                if (cancelStreamRegistry.unregisterCloseable(fsDataInputStream)) {
                    IOUtils.closeQuietly(fsDataInputStream);
                }
            }
        }
        return null;
    }

    private void readStateHandleStateData(
            FSDataInputStream fsDataInputStream,
            DataInputViewStreamWrapper inView,
            KeyGroupRangeOffsets keyGroupOffsets,
            Map<Integer, StateMetaInfoSnapshot> kvStatesById,
            int numStates,
            int readVersion,
            boolean isCompressed)
            throws IOException {

        final StreamCompressionDecorator streamCompressionDecorator =
                isCompressed
                        ? SnappyStreamCompressionDecorator.INSTANCE
                        : UncompressedStreamCompressionDecorator.INSTANCE;

        for (Tuple2<Integer, Long> groupOffset : keyGroupOffsets) {
            int keyGroupIndex = groupOffset.f0;
            long offset = groupOffset.f1;

            if (!keyGroupRange.contains(keyGroupIndex)) {
                LOG.debug(
                        "Key group {} doesn't belong to this backend with key group range: {}",
                        keyGroupIndex,
                        keyGroupRange);
                continue;
            }

            fsDataInputStream.seek(offset);

            int writtenKeyGroupIndex = inView.readInt();
            Preconditions.checkState(
                    writtenKeyGroupIndex == keyGroupIndex, "Unexpected key-group in restore.");

            try (InputStream kgCompressionInStream =
                    streamCompressionDecorator.decorateWithCompression(fsDataInputStream)) {

                readKeyGroupStateData(
                        kgCompressionInStream, kvStatesById, keyGroupIndex, numStates, readVersion);
            }
        }
    }

    private void readKeyGroupStateData(
            InputStream inputStream,
            Map<Integer, StateMetaInfoSnapshot> kvStatesById,
            int keyGroupIndex,
            int numStates,
            int readVersion)
            throws IOException {

        DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(inputStream);

        for (int i = 0; i < numStates; i++) {

            final int kvStateId = inView.readShort();
            final StateMetaInfoSnapshot stateMetaInfoSnapshot = kvStatesById.get(kvStateId);
            final StateSnapshotRestore registeredState;

            switch (stateMetaInfoSnapshot.getBackendStateType()) {
                case KEY_VALUE:
                    registeredState = registeredKVStates.get(stateMetaInfoSnapshot.getName());
                    break;
                case PRIORITY_QUEUE:
                    registeredState = registeredPQStates.get(stateMetaInfoSnapshot.getName());
                    break;
                default:
                    throw new IllegalStateException(
                            "Unexpected state type: "
                                    + stateMetaInfoSnapshot.getBackendStateType()
                                    + ".");
            }

            StateSnapshotKeyGroupReader keyGroupReader =
                    registeredState.keyGroupReader(readVersion);
            keyGroupReader.readMappingsInKeyGroup(inView, keyGroupIndex);
        }
    }
}
