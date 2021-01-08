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
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters.CURRENT_STATE_META_INFO_SNAPSHOT_VERSION;

public class SerializationProxiesTest {

    @Test
    public void testKeyedBackendSerializationProxyRoundtrip() throws Exception {

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

        Assertions.assertTrue(serializationProxy.isUsingKeyGroupCompression());
        Assertions.assertTrue(
                serializationProxy.getKeySerializerSnapshot()
                        instanceof IntSerializer.IntSerializerSnapshot);

        assertEqualStateMetaInfoSnapshotsLists(
                stateMetaInfoList, serializationProxy.getStateMetaInfoSnapshots());
    }

    @Test
    public void testKeyedStateMetaInfoSerialization() throws Exception {

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
                            CURRENT_STATE_META_INFO_SNAPSHOT_VERSION,
                            StateMetaInfoSnapshotReadersWriters.StateTypeHint.KEYED_STATE);
            metaInfo =
                    reader.readStateMetaInfoSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader());
        }

        Assertions.assertEquals(name, metaInfo.getName());
    }

    @Test
    public void testOperatorBackendSerializationProxyRoundtrip() throws Exception {

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
                        stateMetaInfoSnapshots, broadcastStateMetaInfoSnapshots);

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

        assertEqualStateMetaInfoSnapshotsLists(
                stateMetaInfoSnapshots, serializationProxy.getOperatorStateMetaInfoSnapshots());
        assertEqualStateMetaInfoSnapshotsLists(
                broadcastStateMetaInfoSnapshots,
                serializationProxy.getBroadcastStateMetaInfoSnapshots());
    }

    @Test
    public void testOperatorStateMetaInfoSerialization() throws Exception {

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
                            CURRENT_STATE_META_INFO_SNAPSHOT_VERSION,
                            StateMetaInfoSnapshotReadersWriters.StateTypeHint.OPERATOR_STATE);
            snapshot =
                    reader.readStateMetaInfoSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader());
        }

        RegisteredOperatorStateBackendMetaInfo<?> restoredMetaInfo =
                new RegisteredOperatorStateBackendMetaInfo<>(snapshot);

        Assertions.assertEquals(name, restoredMetaInfo.getName());
        Assertions.assertEquals(
                OperatorStateHandle.Mode.UNION, restoredMetaInfo.getAssignmentMode());
        Assertions.assertEquals(stateSerializer, restoredMetaInfo.getPartitionStateSerializer());
    }

    @Test
    public void testBroadcastStateMetaInfoSerialization() throws Exception {

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
                            CURRENT_STATE_META_INFO_SNAPSHOT_VERSION,
                            StateMetaInfoSnapshotReadersWriters.StateTypeHint.OPERATOR_STATE);
            snapshot =
                    reader.readStateMetaInfoSnapshot(
                            new DataInputViewStreamWrapper(in),
                            Thread.currentThread().getContextClassLoader());
        }

        RegisteredBroadcastStateBackendMetaInfo<?, ?> restoredMetaInfo =
                new RegisteredBroadcastStateBackendMetaInfo<>(snapshot);

        Assertions.assertEquals(name, restoredMetaInfo.getName());
        Assertions.assertEquals(
                OperatorStateHandle.Mode.BROADCAST, restoredMetaInfo.getAssignmentMode());
        Assertions.assertEquals(keySerializer, restoredMetaInfo.getKeySerializer());
        Assertions.assertEquals(valueSerializer, restoredMetaInfo.getValueSerializer());
    }

    /**
     * This test fixes the order of elements in the enum which is important for serialization. Do
     * not modify this test except if you are entirely sure what you are doing.
     */
    @Test
    public void testFixTypeOrder() {
        // ensure all elements are covered
        Assertions.assertEquals(7, StateDescriptor.Type.values().length);
        // fix the order of elements to keep serialization format stable
        Assertions.assertEquals(0, StateDescriptor.Type.UNKNOWN.ordinal());
        Assertions.assertEquals(1, StateDescriptor.Type.VALUE.ordinal());
        Assertions.assertEquals(2, StateDescriptor.Type.LIST.ordinal());
        Assertions.assertEquals(3, StateDescriptor.Type.REDUCING.ordinal());
        Assertions.assertEquals(4, StateDescriptor.Type.FOLDING.ordinal());
        Assertions.assertEquals(5, StateDescriptor.Type.AGGREGATING.ordinal());
        Assertions.assertEquals(6, StateDescriptor.Type.MAP.ordinal());
    }

    private void assertEqualStateMetaInfoSnapshotsLists(
            List<StateMetaInfoSnapshot> expected, List<StateMetaInfoSnapshot> actual) {
        Assertions.assertEquals(expected.size(), actual.size());
        for (int i = 0; i < expected.size(); ++i) {
            assertEqualStateMetaInfoSnapshots(expected.get(i), actual.get(i));
        }
    }

    private void assertEqualStateMetaInfoSnapshots(
            StateMetaInfoSnapshot expected, StateMetaInfoSnapshot actual) {
        Assertions.assertEquals(expected.getName(), actual.getName());
        Assertions.assertEquals(expected.getBackendStateType(), actual.getBackendStateType());
        Assertions.assertEquals(expected.getOptionsImmutable(), actual.getOptionsImmutable());
        Assertions.assertEquals(
                expected.getSerializerSnapshotsImmutable(),
                actual.getSerializerSnapshotsImmutable());
    }
}
