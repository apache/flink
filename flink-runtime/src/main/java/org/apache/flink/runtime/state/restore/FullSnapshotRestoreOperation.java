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

package org.apache.flink.runtime.state.restore;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RestoreOperation;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StateMigrationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.FullSnapshotUtil.END_OF_KEY_GROUP_MARK;
import static org.apache.flink.runtime.state.FullSnapshotUtil.clearMetaDataFollowsFlag;
import static org.apache.flink.runtime.state.FullSnapshotUtil.hasMetaDataFollowsFlag;
import static org.apache.flink.runtime.state.StateUtil.unexpectedStateHandleException;

/**
 * The procedure of restoring state from a savepoint written with the unified binary format. All
 * state backends should support restoring from that format.
 *
 * <p>The format was adopted from the RocksDB state backend. It is as follows:
 *
 * <pre>
 *                    +-------------------------------------------------------------+
 *                    |                Keyed Backend Meta Information               |
 *  Meta Information  +--------------------+-----+----------------------------------+
 *                    |    State Meta 0    | ... |           State Meta M           |
 *                    +-------------------------------------------------------------+
 *                    |                       State ID (short)                      |
 *  State 0           +--------------------+-----+----------------------------------+
 *                    | State (K,V) pair 0 | ... | State (K,V) pair X (flipped MSB) |
 *                    +--------------------+-----+----------------------------------+
 *                    |                       State ID (short)                      |
 *  State 1           +--------------------+-----+----------------------------------+
 *                    | State (K,V) pair 0 | ... | State (K,V) pair X (flipped MSB) |
 *                    +--------------------+-----+----------------------------------+
 *                    |                       State ID (short)                      |
 *                    +--------------------+-----+----------------------------------+
 *  State M           | State (K,V) pair 0 | ... | State (K,V) pair X (flipped MSB) |
 *                    +--------------------+-----+----------------------------------+
 *                    |                END_OF_KEY_GROUP_MARK (0xFFFF)               |
 *                    +-------------------------------------------------------------+
 * </pre>
 *
 * <p>Additionally the format of the (K,V) pairs differs slightly depending on the type of the state
 * object:
 *
 * <pre>
 * +------------------+-------------------------------------+
 * |    ValueState    |    [CompositeKey(KG, K, NS), SV]    |
 * |------------------+-------------------------------------+
 * |    ListState     |    [CompositeKey(KG, K, NS), SV]    |
 * +------------------+-------------------------------------+
 * |     MapState     | [CompositeKey(KG, K, NS) :: UK, UV] |
 * +------------------+-------------------------------------+
 * | AggregatingState |    [CompositeKey(KG, K, NS), SV]    |
 * +------------------+-------------------------------------+
 * |   ReducingState  |    [CompositeKey(KG, K, NS), SV]    |
 * |                  +-------------------------------------+
 * |                  |    [CompositeKey(KG, K, NS), SV]    |
 * +------------------+-------------------------------------+
 * |      Timers      |    [KG :: TS :: K :: NS, (empty)]   |
 * +------------------+-------------------------------------+
 * </pre>
 *
 * <p>For detailed information see FLIP-41: https://cwiki.apache.org/confluence/x/VJDiBg
 *
 * @param <K> The data type of the key.
 */
@Internal
public class FullSnapshotRestoreOperation<K>
        implements RestoreOperation<ThrowingIterator<SavepointRestoreResult>> {

    private static final Logger LOG = LoggerFactory.getLogger(FullSnapshotRestoreOperation.class);

    private final KeyGroupRange keyGroupRange;
    private final ClassLoader userCodeClassLoader;
    private final Collection<KeyedStateHandle> restoreStateHandles;
    private final StateSerializerProvider<K> keySerializerProvider;

    private boolean isKeySerializerCompatibilityChecked;

    public FullSnapshotRestoreOperation(
            KeyGroupRange keyGroupRange,
            ClassLoader userCodeClassLoader,
            Collection<KeyedStateHandle> restoreStateHandles,
            StateSerializerProvider<K> keySerializerProvider) {
        this.keyGroupRange = keyGroupRange;
        this.userCodeClassLoader = userCodeClassLoader;
        this.restoreStateHandles =
                restoreStateHandles.stream().filter(Objects::nonNull).collect(Collectors.toList());
        this.keySerializerProvider = keySerializerProvider;
    }

    @Override
    public ThrowingIterator<SavepointRestoreResult> restore()
            throws IOException, StateMigrationException {
        return new ThrowingIterator<SavepointRestoreResult>() {

            private final Iterator<KeyedStateHandle> keyedStateHandlesIterator =
                    restoreStateHandles.iterator();

            @Override
            public boolean hasNext() {
                return keyedStateHandlesIterator.hasNext();
            }

            @Override
            public SavepointRestoreResult next() throws IOException, StateMigrationException {
                KeyedStateHandle keyedStateHandle = keyedStateHandlesIterator.next();
                if (!(keyedStateHandle instanceof KeyGroupsStateHandle)) {
                    throw unexpectedStateHandleException(
                            KeyGroupsStateHandle.class, keyedStateHandle.getClass());
                }
                KeyGroupsStateHandle groupsStateHandle = (KeyGroupsStateHandle) keyedStateHandle;
                return restoreKeyGroupsInStateHandle(groupsStateHandle);
            }

            @Override
            public void close() {}
        };
    }

    private SavepointRestoreResult restoreKeyGroupsInStateHandle(
            @Nonnull KeyGroupsStateHandle keyedStateHandle)
            throws IOException, StateMigrationException {
        FSDataInputStream currentStateHandleInStream = keyedStateHandle.openInputStream();
        KeyedBackendSerializationProxy<K> serializationProxy =
                readMetaData(new DataInputViewStreamWrapper(currentStateHandleInStream));
        KeyGroupsIterator groupsIterator =
                new KeyGroupsIterator(
                        keyGroupRange,
                        keyedStateHandle,
                        currentStateHandleInStream,
                        serializationProxy.isUsingKeyGroupCompression()
                                ? SnappyStreamCompressionDecorator.INSTANCE
                                : UncompressedStreamCompressionDecorator.INSTANCE);

        return new SavepointRestoreResult(
                serializationProxy.getStateMetaInfoSnapshots(), groupsIterator);
    }

    private KeyedBackendSerializationProxy<K> readMetaData(DataInputView dataInputView)
            throws IOException, StateMigrationException {
        // isSerializerPresenceRequired flag is set to false, since for the RocksDB state backend,
        // deserialization of state happens lazily during runtime; we depend on the fact
        // that the new serializer for states could be compatible, and therefore the restore can
        // continue
        // without old serializers required to be present.
        KeyedBackendSerializationProxy<K> serializationProxy =
                new KeyedBackendSerializationProxy<>(userCodeClassLoader);
        serializationProxy.read(dataInputView);
        if (!isKeySerializerCompatibilityChecked) {
            // fetch current serializer now because if it is incompatible, we can't access
            // it anymore to improve the error message
            TypeSerializer<K> currentSerializer = keySerializerProvider.currentSchemaSerializer();
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

            isKeySerializerCompatibilityChecked = true;
        }

        return serializationProxy;
    }

    private static class KeyGroupsIterator implements ThrowingIterator<KeyGroup> {
        @Nonnull private final KeyGroupRange keyGroupRange;
        @Nonnull private final Iterator<Tuple2<Integer, Long>> keyGroups;
        @Nonnull private final FSDataInputStream currentStateHandleInStream;
        @Nonnull private final StreamCompressionDecorator keygroupStreamCompressionDecorator;
        @Nonnull private final KeyGroupsStateHandle currentKeyGroupsStateHandle;

        private KeyGroupsIterator(
                @Nonnull KeyGroupRange keyGroupRange,
                @Nonnull KeyGroupsStateHandle currentKeyGroupsStateHandle,
                @Nonnull FSDataInputStream currentStateHandleInStream,
                @Nonnull StreamCompressionDecorator keygroupStreamCompressionDecorator) {
            this.keyGroupRange = keyGroupRange;
            this.keyGroups = currentKeyGroupsStateHandle.getGroupRangeOffsets().iterator();
            this.currentStateHandleInStream = currentStateHandleInStream;
            this.keygroupStreamCompressionDecorator = keygroupStreamCompressionDecorator;
            this.currentKeyGroupsStateHandle = currentKeyGroupsStateHandle;
            LOG.info("Starting to restore from state handle: {}.", currentKeyGroupsStateHandle);
        }

        public boolean hasNext() {
            return keyGroups.hasNext();
        }

        public KeyGroup next() throws IOException {
            Tuple2<Integer, Long> keyGroupOffset = keyGroups.next();
            int keyGroup = keyGroupOffset.f0;

            // Check that restored key groups all belong to the backend
            Preconditions.checkState(
                    keyGroupRange.contains(keyGroup), "The key group must belong to the backend");

            long offset = keyGroupOffset.f1;
            if (0L != offset) {
                currentStateHandleInStream.seek(offset);
                InputStream compressedKgIn =
                        keygroupStreamCompressionDecorator.decorateWithCompression(
                                currentStateHandleInStream);
                DataInputViewStreamWrapper compressedKgInputView =
                        new DataInputViewStreamWrapper(compressedKgIn);
                return new KeyGroup(keyGroup, new KeyGroupEntriesIterator(compressedKgInputView));
            } else {
                return new KeyGroup(keyGroup, new KeyGroupEntriesIterator());
            }
        }

        @Override
        public void close() throws IOException {
            currentStateHandleInStream.close();
            LOG.info("Finished restoring from state handle: {}.", currentKeyGroupsStateHandle);
        }
    }

    private static class KeyGroupEntriesIterator implements ThrowingIterator<KeyGroupEntry> {
        private final DataInputViewStreamWrapper kgInputView;
        private Integer currentKvStateId;

        private KeyGroupEntriesIterator(@Nonnull DataInputViewStreamWrapper kgInputView)
                throws IOException {
            this.kgInputView = kgInputView;
            this.currentKvStateId = END_OF_KEY_GROUP_MARK & kgInputView.readShort();
        }

        // creates an empty iterator
        private KeyGroupEntriesIterator() {
            this.kgInputView = null;
            this.currentKvStateId = null;
        }

        public boolean hasNext() {
            return currentKvStateId != null;
        }

        public KeyGroupEntry next() throws IOException {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            byte[] key = BytePrimitiveArraySerializer.INSTANCE.deserialize(kgInputView);
            byte[] value = BytePrimitiveArraySerializer.INSTANCE.deserialize(kgInputView);
            final int entryStateId = currentKvStateId;
            if (hasMetaDataFollowsFlag(key)) {
                // clear the signal bit in the key to make it ready for insertion
                // again
                clearMetaDataFollowsFlag(key);
                // TODO this could be aware of keyGroupPrefixBytes and write only
                // one byte if possible
                currentKvStateId = END_OF_KEY_GROUP_MARK & kgInputView.readShort();
                if (END_OF_KEY_GROUP_MARK == currentKvStateId) {
                    currentKvStateId = null;
                }
            }

            return new KeyGroupEntry(entryStateId, key, value);
        }

        @Override
        public void close() throws IOException {
            if (kgInputView != null) {
                kgInputView.close();
            }
        }
    }
}
