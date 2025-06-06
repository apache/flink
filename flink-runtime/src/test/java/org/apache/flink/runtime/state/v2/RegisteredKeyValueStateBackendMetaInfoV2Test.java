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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class RegisteredKeyValueStateBackendMetaInfoV2Test {
    @Test
    void testRegisteredKeyValueStateBackendMetaInfoV2SerializationRoundtrip() throws Exception {

        TypeSerializer<?> keySerializer = IntSerializer.INSTANCE;
        TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
        TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;
        TypeSerializer<?> userKeySerializer = StringSerializer.INSTANCE;

        List<StateMetaInfoSnapshot> stateMetaInfoList = new ArrayList<>();

        stateMetaInfoList.add(
                new org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo<>(
                                "a",
                                org.apache.flink.api.common.state.v2.StateDescriptor.Type.VALUE,
                                namespaceSerializer,
                                stateSerializer)
                        .snapshot());
        stateMetaInfoList.add(
                new org.apache.flink.runtime.state.v2
                                .RegisteredKeyAndUserKeyValueStateBackendMetaInfo<>(
                                "b",
                                org.apache.flink.api.common.state.v2.StateDescriptor.Type.MAP,
                                namespaceSerializer,
                                stateSerializer,
                                userKeySerializer)
                        .snapshot());
        stateMetaInfoList.add(
                new org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo<>(
                                "c",
                                org.apache.flink.api.common.state.v2.StateDescriptor.Type.VALUE,
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
    void testMapKeyedStateMetaInfoSerialization() throws Exception {

        TypeSerializer<?> keySerializer = IntSerializer.INSTANCE;
        TypeSerializer<?> namespaceSerializer = LongSerializer.INSTANCE;
        TypeSerializer<?> stateSerializer = DoubleSerializer.INSTANCE;
        TypeSerializer<?> userKeySerializer = StringSerializer.INSTANCE;
        List<StateMetaInfoSnapshot> stateMetaInfoList = new ArrayList<>();

        // create StateMetaInfoSnapshot without userKeySerializer
        StateMetaInfoSnapshot oldStateMeta =
                new org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo<>(
                                "test1",
                                org.apache.flink.api.common.state.v2.StateDescriptor.Type.MAP,
                                namespaceSerializer,
                                stateSerializer)
                        .snapshot();

        StateMetaInfoSnapshot oldStateMetaWithUserKey =
                new org.apache.flink.runtime.state.v2
                                .RegisteredKeyAndUserKeyValueStateBackendMetaInfo<>(
                                "test2",
                                org.apache.flink.api.common.state.v2.StateDescriptor.Type.MAP,
                                namespaceSerializer,
                                stateSerializer,
                                userKeySerializer)
                        .snapshot();

        stateMetaInfoList.add(oldStateMeta);
        stateMetaInfoList.add(oldStateMetaWithUserKey);

        assertThat(oldStateMeta.getBackendStateType())
                .isEqualTo(StateMetaInfoSnapshot.BackendStateType.KEY_VALUE_V2);
        assertThat(oldStateMetaWithUserKey.getBackendStateType())
                .isEqualTo(StateMetaInfoSnapshot.BackendStateType.KEY_VALUE_V2);

        assertThat(
                        oldStateMeta.getTypeSerializerSnapshot(
                                StateMetaInfoSnapshot.CommonSerializerKeys.USER_KEY_SERIALIZER))
                .isNull();
        assertThat(
                        oldStateMetaWithUserKey
                                .getTypeSerializerSnapshot(
                                        StateMetaInfoSnapshot.CommonSerializerKeys
                                                .USER_KEY_SERIALIZER)
                                .restoreSerializer())
                .isEqualTo(userKeySerializer);

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

        List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                serializationProxy.getStateMetaInfoSnapshots();

        org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo restoredMetaInfo =
                (RegisteredKeyValueStateBackendMetaInfo)
                        RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(
                                stateMetaInfoSnapshots.get(0));
        assertThat(restoredMetaInfo.getClass())
                .isEqualTo(
                        org.apache.flink.runtime.state.v2
                                .RegisteredKeyAndUserKeyValueStateBackendMetaInfo.class);
        assertThat(restoredMetaInfo.getName()).isEqualTo("test1");
        assertThat(
                        ((RegisteredKeyAndUserKeyValueStateBackendMetaInfo) restoredMetaInfo)
                                .getUserKeySerializer())
                .isNull();
        assertThat(restoredMetaInfo.getStateSerializer()).isEqualTo(DoubleSerializer.INSTANCE);
        assertThat(restoredMetaInfo.getNamespaceSerializer()).isEqualTo(LongSerializer.INSTANCE);

        org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo restoredMetaInfo1 =
                (RegisteredKeyValueStateBackendMetaInfo)
                        RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(
                                stateMetaInfoSnapshots.get(1));
        assertThat(restoredMetaInfo1.getClass())
                .isEqualTo(
                        org.apache.flink.runtime.state.v2
                                .RegisteredKeyAndUserKeyValueStateBackendMetaInfo.class);
        assertThat(restoredMetaInfo1.getName()).isEqualTo("test2");
        assertThat(
                        ((RegisteredKeyAndUserKeyValueStateBackendMetaInfo) restoredMetaInfo1)
                                .getUserKeySerializer())
                .isEqualTo(StringSerializer.INSTANCE);
        assertThat(restoredMetaInfo1.getStateSerializer()).isEqualTo(DoubleSerializer.INSTANCE);
        assertThat(restoredMetaInfo1.getNamespaceSerializer()).isEqualTo(LongSerializer.INSTANCE);
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
