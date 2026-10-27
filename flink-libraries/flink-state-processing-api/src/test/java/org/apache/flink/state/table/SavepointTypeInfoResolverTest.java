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

package org.apache.flink.state.table;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link SavepointTypeInfoResolver#resolveNamespaceSerializer}. */
class SavepointTypeInfoResolverTest {

    private static final String STATE_NAME = "window-state";

    @Test
    void resolveNamespaceSerializerReturnsRestoredSerializer() {
        Map<String, TypeSerializerSnapshot<?>> serializerSnapshots = new HashMap<>();
        serializerSnapshots.put(
                StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString(),
                LongSerializer.INSTANCE.snapshotConfiguration());

        StateMetaInfoSnapshot metaInfoSnapshot =
                new StateMetaInfoSnapshot(
                        STATE_NAME,
                        StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                        Collections.emptyMap(),
                        serializerSnapshots);

        SavepointTypeInfoResolver resolver =
                new SavepointTypeInfoResolver(
                        Collections.singletonMap(STATE_NAME, metaInfoSnapshot),
                        new SerializerConfigImpl(),
                        null);

        TypeSerializer<?> namespaceSerializer = resolver.resolveNamespaceSerializer(STATE_NAME);

        assertThat(namespaceSerializer).isInstanceOf(LongSerializer.class);
    }

    @Test
    void resolveNamespaceSerializerThrowsWhenStateNotFound() {
        SavepointTypeInfoResolver resolver =
                new SavepointTypeInfoResolver(
                        Collections.emptyMap(), new SerializerConfigImpl(), null);

        assertThatThrownBy(() -> resolver.resolveNamespaceSerializer(STATE_NAME))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(STATE_NAME)
                .hasMessageContaining("not found in preloaded metadata");
    }

    @Test
    void resolveNamespaceSerializerThrowsWhenNamespaceSerializerMissing() {
        Map<String, TypeSerializerSnapshot<?>> serializerSnapshots = new HashMap<>();
        serializerSnapshots.put(
                StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString(),
                StringSerializer.INSTANCE.snapshotConfiguration());

        StateMetaInfoSnapshot metaInfoSnapshot =
                new StateMetaInfoSnapshot(
                        STATE_NAME,
                        StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                        Collections.emptyMap(),
                        serializerSnapshots);

        SavepointTypeInfoResolver resolver =
                new SavepointTypeInfoResolver(
                        Collections.singletonMap(STATE_NAME, metaInfoSnapshot),
                        new SerializerConfigImpl(),
                        null);

        assertThatThrownBy(() -> resolver.resolveNamespaceSerializer(STATE_NAME))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(STATE_NAME)
                .hasMessageContaining("no namespace serializer in metadata");
    }
}
