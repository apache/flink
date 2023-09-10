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

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoReader;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters.CURRENT_STATE_META_INFO_SNAPSHOT_VERSION;
import static org.assertj.core.api.Assertions.assertThat;

class SerializationProxiesTest {

    @Test
    void testKeyedBackendSerializationProxyRoundtrip() throws Exception {

        TypeSerializer<?> keySerializer = IntSerializer.INSTANCE;
        TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
        TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

        List<StateMetaInfoSnapshot> stateMetaInfoList = new ArrayList<>();

        stateMetaInfoList.add(
                new RegisteredKeyValueStateBackendMetaInfo<>(
                                StateDescriptor.Type.VALUE,
                                "a",
                                namespaceSerializer,
                                stateSerializer)
                        .snapshot());
        stateMetaInfoList.add(
                new RegisteredKeyValueStateBackendMetaInfo<>(
                                StateDescriptor.Type.VALUE,
                                "b",
                                namespaceSerializer,
                                stateSerializer)
                        .snapshot());
        stateMetaInfoList.add(
                new RegisteredKeyValueStateBackendMetaInfo<>(
                                StateDescriptor.Type.VALUE,
                                "c",
                                namespaceSerializer,
                                stateSerializer)
                        .snapshot());

        KeyedBackendSerializationProxy<?> serializationProxy =
                new KeyedBackendSerializationProxy<>(keySerializer, stateMetaInfoList, true);

        byte[] serialized;
        try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
            serializationProxy.write(new DataOutputViewStreamWrapper(out));
            serialized = out.toByteArray();
        }

        serializationProxy =
                new KeyedBackendSerializationProxy<>(
                        Thread.currentThread().getContextClassLoader());

        try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
            serializationProxy.read(new DataInputViewStreamWrapper(in));
        }

        assertThat(serializationProxy.isUsingKeyGroupCompression()).isTrue();
        assertThat(serializationProxy.getKeySerializerSnapshot())
                .isInstanceOf(IntSerializer.IntSerializerSnapshot.class);

        assertEqualStateMetaInfoSnapshotsLists(
                stateMetaInfoList, serializationProxy.getStateMetaInfoSnapshots());
    }

    @Test
    void testKeyedStateMetaInfoSerialization() throws Exception {

        String name = "test";
        TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
        TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

        StateMetaInfoSnapshot metaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                                StateDescriptor.Type.VALUE,
                                name,
                                namespaceSerializer,
                                stateSerializer)
                        .snapshot();

        byte[] serialized;
        try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
            StateMetaInfoSnapshotReadersWriters.getWriter()
                    .writeStateMetaInfoSnapshot(metaInfo, new DataOutputViewStreamWrapper(out));
            serialized = out.toByteArray();
        }

        try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
            final StateMetaInfoReader reader =
                    StateMetaInfoSnapshotReadersWriters.getReader(
                            CURRENT_STATE_META_INFO_SNAPSHOT_VERSION);
            metaInfo =
                    reader.readStateMetaInfoSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader());
        }

        assertThat(metaInfo.getName()).isEqualTo(name);
    }

    @Test
    void testOperatorBackendSerializationProxyRoundtrip() throws Exception {

        TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;
        TypeSerializer<?> keySerializer = DoubleSerializer.INSTANCE;
        TypeSerializer<?> valueSerializer = StringSerializer.INSTANCE;

        List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>();

        stateMetaInfoSnapshots.add(
                new RegisteredOperatorStateBackendMetaInfo<>(
                                "a", stateSerializer, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE)
                        .snapshot());
        stateMetaInfoSnapshots.add(
                new RegisteredOperatorStateBackendMetaInfo<>(
                                "b", stateSerializer, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE)
                        .snapshot());
        stateMetaInfoSnapshots.add(
                new RegisteredOperatorStateBackendMetaInfo<>(
                                "c", stateSerializer, OperatorStateHandle.Mode.UNION)
                        .snapshot());

        List<StateMetaInfoSnapshot> broadcastStateMetaInfoSnapshots = new ArrayList<>();

        broadcastStateMetaInfoSnapshots.add(
                new RegisteredBroadcastStateBackendMetaInfo<>(
                                "d",
                                OperatorStateHandle.Mode.BROADCAST,
                                keySerializer,
                                valueSerializer)
                        .snapshot());
        broadcastStateMetaInfoSnapshots.add(
                new RegisteredBroadcastStateBackendMetaInfo<>(
                                "e",
                                OperatorStateHandle.Mode.BROADCAST,
                                valueSerializer,
                                keySerializer)
                        .snapshot());

        OperatorBackendSerializationProxy serializationProxy =
                new OperatorBackendSerializationProxy(
                        stateMetaInfoSnapshots, broadcastStateMetaInfoSnapshots, true);

        byte[] serialized;
        try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
            serializationProxy.write(new DataOutputViewStreamWrapper(out));
            serialized = out.toByteArray();
        }

        serializationProxy =
                new OperatorBackendSerializationProxy(
                        Thread.currentThread().getContextClassLoader());

        try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
            serializationProxy.read(new DataInputViewStreamWrapper(in));
        }

        assertThat(serializationProxy.isUsingStateCompression()).isTrue();
        assertEqualStateMetaInfoSnapshotsLists(
                stateMetaInfoSnapshots, serializationProxy.getOperatorStateMetaInfoSnapshots());
        assertEqualStateMetaInfoSnapshotsLists(
                broadcastStateMetaInfoSnapshots,
                serializationProxy.getBroadcastStateMetaInfoSnapshots());
    }

    @Test
    void testOperatorStateMetaInfoSerialization() throws Exception {

        String name = "test";
        TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;

        StateMetaInfoSnapshot snapshot =
                new RegisteredOperatorStateBackendMetaInfo<>(
                                name, stateSerializer, OperatorStateHandle.Mode.UNION)
                        .snapshot();

        byte[] serialized;
        try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
            StateMetaInfoSnapshotReadersWriters.getWriter()
                    .writeStateMetaInfoSnapshot(snapshot, new DataOutputViewStreamWrapper(out));

            serialized = out.toByteArray();
        }

        try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
            final StateMetaInfoReader reader =
                    StateMetaInfoSnapshotReadersWriters.getReader(
                            CURRENT_STATE_META_INFO_SNAPSHOT_VERSION);
            snapshot =
                    reader.readStateMetaInfoSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader());
        }

        RegisteredOperatorStateBackendMetaInfo<?> restoredMetaInfo =
                new RegisteredOperatorStateBackendMetaInfo<>(snapshot);

        assertThat(restoredMetaInfo.getName()).isEqualTo(name);
        assertThat(restoredMetaInfo.getAssignmentMode()).isEqualTo(OperatorStateHandle.Mode.UNION);
        assertThat(restoredMetaInfo.getPartitionStateSerializer()).isEqualTo(stateSerializer);
    }

    @Test
    void testBroadcastStateMetaInfoSerialization() throws Exception {

        String name = "test";
        TypeSerializer<?> keySerializer = DoubleSerializer.INSTANCE;
        TypeSerializer<?> valueSerializer = StringSerializer.INSTANCE;

        StateMetaInfoSnapshot snapshot =
                new RegisteredBroadcastStateBackendMetaInfo<>(
                                name,
                                OperatorStateHandle.Mode.BROADCAST,
                                keySerializer,
                                valueSerializer)
                        .snapshot();

        byte[] serialized;
        try (ByteArrayOutputStreamWithPos out = new ByteArrayOutputStreamWithPos()) {
            StateMetaInfoSnapshotReadersWriters.getWriter()
                    .writeStateMetaInfoSnapshot(snapshot, new DataOutputViewStreamWrapper(out));

            serialized = out.toByteArray();
        }

        try (ByteArrayInputStreamWithPos in = new ByteArrayInputStreamWithPos(serialized)) {
            final StateMetaInfoReader reader =
                    StateMetaInfoSnapshotReadersWriters.getReader(
                            CURRENT_STATE_META_INFO_SNAPSHOT_VERSION);
            snapshot =
                    reader.readStateMetaInfoSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader());
        }

        RegisteredBroadcastStateBackendMetaInfo<?, ?> restoredMetaInfo =
                new RegisteredBroadcastStateBackendMetaInfo<>(snapshot);

        assertThat(restoredMetaInfo.getName()).isEqualTo(name);
        assertThat(restoredMetaInfo.getAssignmentMode())
                .isEqualTo(OperatorStateHandle.Mode.BROADCAST);
        assertThat(restoredMetaInfo.getKeySerializer()).isEqualTo(keySerializer);
        assertThat(restoredMetaInfo.getValueSerializer()).isEqualTo(valueSerializer);
    }

    /**
     * This test fixes the order of elements in the enum which is important for serialization. Do
     * not modify this test except if you are entirely sure what you are doing.
     */
    @Test
    void testFixTypeOrder() {
        // ensure all elements are covered
        assertThat(StateDescriptor.Type.values()).hasSize(7);
        // fix the order of elements to keep serialization format stable
        assertThat(StateDescriptor.Type.UNKNOWN.ordinal()).isZero();
        assertThat(StateDescriptor.Type.VALUE.ordinal()).isOne();
        assertThat(StateDescriptor.Type.LIST.ordinal()).isEqualTo(2);
        assertThat(StateDescriptor.Type.REDUCING.ordinal()).isEqualTo(3);
        assertThat(StateDescriptor.Type.FOLDING.ordinal()).isEqualTo(4);
        assertThat(StateDescriptor.Type.AGGREGATING.ordinal()).isEqualTo(5);
        assertThat(StateDescriptor.Type.MAP.ordinal()).isEqualTo(6);
    }

    private void assertEqualStateMetaInfoSnapshotsLists(
            List<StateMetaInfoSnapshot> expected, List<StateMetaInfoSnapshot> actual) {
        assertThat(actual).hasSameSizeAs(expected);
        for (int i = 0; i < expected.size(); ++i) {
            assertEqualStateMetaInfoSnapshots(expected.get(i), actual.get(i));
        }
    }

    private void assertEqualStateMetaInfoSnapshots(
            StateMetaInfoSnapshot expected, StateMetaInfoSnapshot actual) {
        assertThat(actual.getName()).isEqualTo(expected.getName());
        assertThat(actual.getBackendStateType()).isEqualTo(expected.getBackendStateType());
        assertThat(actual.getOptionsImmutable()).isEqualTo(expected.getOptionsImmutable());
        assertThat(actual.getSerializerSnapshotsImmutable())
                .isEqualTo(expected.getSerializerSnapshotsImmutable());
    }
}
