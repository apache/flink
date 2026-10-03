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

package org.apache.flink.state.api.schema;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link StateSchemaExtractor}. */
class StateSchemaExtractorTest {

    // -------------------------------------------------------------------------
    // Tests
    // -------------------------------------------------------------------------

    @Test
    void testExtractSingleValueState() throws IOException {
        StateMetaInfoSnapshot stateSnap =
                buildValueStateSnapshot("my-state", StateDescriptor.Type.VALUE);

        KeyedBackendSerializationProxy<Integer> proxy =
                new KeyedBackendSerializationProxy<>(
                        IntSerializer.INSTANCE, Collections.singletonList(stateSnap), false);

        List<StateSchemaInfo> result = roundTrip(proxy);

        assertThat(result).hasSize(1);
        StateSchemaInfo info = result.get(0);
        assertThat(info.stateName).isEqualTo("my-state");
        assertThat(info.stateKind).isEqualTo(StateDescriptor.Type.VALUE);
        assertThat(info.valueSnapshot).isInstanceOf(IntSerializer.IntSerializerSnapshot.class);
        assertThat(info.keySnapshot).isInstanceOf(IntSerializer.IntSerializerSnapshot.class);
        assertThat(info.mapKeySnapshot).isNull();
    }

    @Test
    void testExtractMultipleStates() throws IOException {
        StateMetaInfoSnapshot valueState =
                buildValueStateSnapshot("int-state", StateDescriptor.Type.VALUE);
        StateMetaInfoSnapshot stringState =
                buildValueStateSnapshotWithStringValue("str-state", StateDescriptor.Type.VALUE);

        KeyedBackendSerializationProxy<Integer> proxy =
                new KeyedBackendSerializationProxy<>(
                        IntSerializer.INSTANCE, Arrays.asList(valueState, stringState), false);

        List<StateSchemaInfo> result = roundTrip(proxy);

        assertThat(result).hasSize(2);

        assertThat(result.get(0).stateName).isEqualTo("int-state");
        assertThat(result.get(0).valueSnapshot)
                .isInstanceOf(IntSerializer.IntSerializerSnapshot.class);

        assertThat(result.get(1).stateName).isEqualTo("str-state");
        assertThat(result.get(1).valueSnapshot)
                .isInstanceOf(StringSerializer.StringSerializerSnapshot.class);
    }

    @Test
    void testExtractMapState() throws IOException {
        Map<String, String> options = new HashMap<>();
        options.put(
                StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString(),
                StateDescriptor.Type.MAP.toString());

        Map<String, org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<?>>
                serializerSnapshots = new LinkedHashMap<>();
        serializerSnapshots.put(
                StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString(),
                new VoidNamespaceSerializer.VoidNamespaceSerializerSnapshot());
        serializerSnapshots.put(
                StateMetaInfoSnapshot.CommonSerializerKeys.USER_KEY_SERIALIZER.toString(),
                new StringSerializer.StringSerializerSnapshot());
        serializerSnapshots.put(
                StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString(),
                new LongSerializer.LongSerializerSnapshot());

        StateMetaInfoSnapshot mapSnap =
                new StateMetaInfoSnapshot(
                        "map-state",
                        StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                        options,
                        serializerSnapshots);

        KeyedBackendSerializationProxy<Integer> proxy =
                new KeyedBackendSerializationProxy<>(
                        IntSerializer.INSTANCE, Collections.singletonList(mapSnap), false);

        List<StateSchemaInfo> result = roundTrip(proxy);

        assertThat(result).hasSize(1);
        StateSchemaInfo info = result.get(0);
        assertThat(info.stateName).isEqualTo("map-state");
        assertThat(info.stateKind).isEqualTo(StateDescriptor.Type.MAP);
        assertThat(info.mapKeySnapshot)
                .isInstanceOf(StringSerializer.StringSerializerSnapshot.class);
        assertThat(info.valueSnapshot).isInstanceOf(LongSerializer.LongSerializerSnapshot.class);
    }

    @Test
    void testEmptyStates() throws IOException {
        KeyedBackendSerializationProxy<Integer> proxy =
                new KeyedBackendSerializationProxy<>(
                        IntSerializer.INSTANCE, Collections.emptyList(), false);

        List<StateSchemaInfo> result = roundTrip(proxy);

        assertThat(result).isNotNull().isEmpty();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static StateMetaInfoSnapshot buildValueStateSnapshot(
            String name, StateDescriptor.Type type) {
        Map<String, String> options = new HashMap<>();
        options.put(
                StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString(),
                type.toString());

        Map<String, org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<?>>
                serializerSnapshots = new LinkedHashMap<>();
        serializerSnapshots.put(
                StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString(),
                new VoidNamespaceSerializer.VoidNamespaceSerializerSnapshot());
        serializerSnapshots.put(
                StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString(),
                new IntSerializer.IntSerializerSnapshot());

        return new StateMetaInfoSnapshot(
                name,
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                options,
                serializerSnapshots);
    }

    private static StateMetaInfoSnapshot buildValueStateSnapshotWithStringValue(
            String name, StateDescriptor.Type type) {
        Map<String, String> options = new HashMap<>();
        options.put(
                StateMetaInfoSnapshot.CommonOptionsKeys.KEYED_STATE_TYPE.toString(),
                type.toString());

        Map<String, org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<?>>
                serializerSnapshots = new LinkedHashMap<>();
        serializerSnapshots.put(
                StateMetaInfoSnapshot.CommonSerializerKeys.NAMESPACE_SERIALIZER.toString(),
                new VoidNamespaceSerializer.VoidNamespaceSerializerSnapshot());
        serializerSnapshots.put(
                StateMetaInfoSnapshot.CommonSerializerKeys.VALUE_SERIALIZER.toString(),
                new StringSerializer.StringSerializerSnapshot());

        return new StateMetaInfoSnapshot(
                name,
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE,
                options,
                serializerSnapshots);
    }

    private static List<StateSchemaInfo> roundTrip(KeyedBackendSerializationProxy<?> proxy)
            throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        proxy.write(out);

        DataInputDeserializer in = new DataInputDeserializer(out.getSharedBuffer());
        return StateSchemaExtractor.extractSchema(in);
    }
}
