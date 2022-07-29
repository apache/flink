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

package org.apache.flink.streaming.api.runners.python.beam.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.utils.ByteArrayWrapper;
import org.apache.flink.streaming.api.utils.ByteArrayWrapperSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;

import javax.annotation.Nullable;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.setCurrentKeyForStreaming;

/** A {@link BeamStateStore} that returns keyed states based on {@link BeamFnApi.StateRequest}. */
public class BeamKeyedStateStore implements BeamStateStore {

    private final KeyedStateBackend<?> keyedStateBackend;

    private final TypeSerializer<?> keySerializer;

    private final TypeSerializer<byte[]> valueSerializer;

    @Nullable private final TypeSerializer<?> namespaceSerializer;

    /** Reusable InputStream used to holding the elements to be deserialized. */
    private final ByteArrayInputStreamWithPos bais;

    /** InputStream Wrapper. */
    private final DataInputViewStreamWrapper baisWrapper;

    /** The cache of the stateDescriptors. */
    private final Map<String, StateDescriptor<?, ?>> stateDescriptorCache;

    public BeamKeyedStateStore(
            KeyedStateBackend<?> keyedStateBackend,
            TypeSerializer<?> keySerializer,
            @Nullable TypeSerializer<?> namespaceSerializer) {
        this.keyedStateBackend = keyedStateBackend;
        this.keySerializer = keySerializer;
        this.valueSerializer =
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO.createSerializer(
                        new ExecutionConfig());
        this.namespaceSerializer = namespaceSerializer;
        this.bais = new ByteArrayInputStreamWithPos();
        this.baisWrapper = new DataInputViewStreamWrapper(bais);
        this.stateDescriptorCache = new HashMap<>();
    }

    @Override
    public ListState<byte[]> getListState(BeamFnApi.StateRequest request) throws Exception {
        if (!request.getStateKey().hasBagUserState()) {
            throw new RuntimeException("Unsupported keyed bag state request: " + request);
        }
        BeamFnApi.StateKey.BagUserState bagUserState = request.getStateKey().getBagUserState();

        // Retrieve key
        byte[] keyBytes = bagUserState.getKey().toByteArray();
        bais.setBuffer(keyBytes, 0, keyBytes.length);
        Object key = keySerializer.deserialize(baisWrapper);
        setCurrentKey(key);

        // Retrieve state descriptor
        byte[] data = Base64.getDecoder().decode(bagUserState.getUserStateId());
        FlinkFnApi.StateDescriptor stateDescriptor = FlinkFnApi.StateDescriptor.parseFrom(data);
        String stateName = PYTHON_STATE_PREFIX + stateDescriptor.getStateName();
        ListStateDescriptor<byte[]> listStateDescriptor;
        StateDescriptor<?, ?> cachedStateDescriptor = stateDescriptorCache.get(stateName);

        if (cachedStateDescriptor instanceof ListStateDescriptor) {
            listStateDescriptor = (ListStateDescriptor<byte[]>) cachedStateDescriptor;
        } else if (cachedStateDescriptor == null) {
            listStateDescriptor = new ListStateDescriptor<>(stateName, valueSerializer);
            if (stateDescriptor.hasStateTtlConfig()) {
                FlinkFnApi.StateDescriptor.StateTTLConfig stateTtlConfigProto =
                        stateDescriptor.getStateTtlConfig();
                StateTtlConfig stateTtlConfig =
                        ProtoUtils.parseStateTtlConfigFromProto(stateTtlConfigProto);
                listStateDescriptor.enableTimeToLive(stateTtlConfig);
            }
            stateDescriptorCache.put(stateName, listStateDescriptor);
        } else {
            throw new RuntimeException(
                    String.format(
                            "State name corrupt detected: "
                                    + "'%s' is used both as LIST state and '%s' state at the same time.",
                            stateName, cachedStateDescriptor.getType()));
        }

        byte[] windowBytes = bagUserState.getWindow().toByteArray();
        if (windowBytes.length != 0) {
            Preconditions.checkNotNull(namespaceSerializer);
            bais.setBuffer(windowBytes, 0, windowBytes.length);
            Object namespace = namespaceSerializer.deserialize(baisWrapper);
            return keyedStateBackend.getPartitionedState(
                    namespace, (TypeSerializer<Object>) namespaceSerializer, listStateDescriptor);
        } else {
            return keyedStateBackend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, listStateDescriptor);
        }
    }

    @Override
    public MapState<ByteArrayWrapper, byte[]> getMapState(BeamFnApi.StateRequest request)
            throws Exception {
        if (!request.getStateKey().hasMultimapSideInput()) {
            throw new RuntimeException("Unsupported keyed map state request: " + request);
        }
        BeamFnApi.StateKey.MultimapSideInput mapUserState =
                request.getStateKey().getMultimapSideInput();

        // Retrieve key
        byte[] keyBytes = mapUserState.getKey().toByteArray();
        bais.setBuffer(keyBytes, 0, keyBytes.length);
        Object key = keySerializer.deserialize(baisWrapper);
        setCurrentKey(key);

        // Retrieve state descriptor
        byte[] data = Base64.getDecoder().decode(mapUserState.getSideInputId());
        FlinkFnApi.StateDescriptor stateDescriptor = FlinkFnApi.StateDescriptor.parseFrom(data);
        String stateName = PYTHON_STATE_PREFIX + stateDescriptor.getStateName();
        StateDescriptor cachedStateDescriptor = stateDescriptorCache.get(stateName);
        MapStateDescriptor<ByteArrayWrapper, byte[]> mapStateDescriptor;

        if (cachedStateDescriptor instanceof MapStateDescriptor) {
            mapStateDescriptor =
                    (MapStateDescriptor<ByteArrayWrapper, byte[]>) cachedStateDescriptor;
        } else if (cachedStateDescriptor == null) {
            mapStateDescriptor =
                    new MapStateDescriptor<>(
                            stateName, ByteArrayWrapperSerializer.INSTANCE, valueSerializer);
            if (stateDescriptor.hasStateTtlConfig()) {
                FlinkFnApi.StateDescriptor.StateTTLConfig stateTtlConfigProto =
                        stateDescriptor.getStateTtlConfig();
                StateTtlConfig stateTtlConfig =
                        ProtoUtils.parseStateTtlConfigFromProto(stateTtlConfigProto);
                mapStateDescriptor.enableTimeToLive(stateTtlConfig);
            }
            stateDescriptorCache.put(stateName, mapStateDescriptor);
        } else {
            throw new RuntimeException(
                    String.format(
                            "State name corrupt detected: "
                                    + "'%s' is used both as MAP state and '%s' state at the same time.",
                            stateName, cachedStateDescriptor.getType()));
        }

        byte[] windowBytes = mapUserState.getWindow().toByteArray();
        if (windowBytes.length != 0) {
            Preconditions.checkNotNull(namespaceSerializer);
            bais.setBuffer(windowBytes, 0, windowBytes.length);
            Object namespace = namespaceSerializer.deserialize(baisWrapper);
            return keyedStateBackend.getPartitionedState(
                    namespace, (TypeSerializer<Object>) namespaceSerializer, mapStateDescriptor);
        } else {
            return keyedStateBackend.getPartitionedState(
                    VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, mapStateDescriptor);
        }
    }

    private void setCurrentKey(Object key) {
        if (keyedStateBackend.getKeySerializer() instanceof RowDataSerializer) {
            setCurrentKeyForStreaming(
                    (KeyedStateBackend<? super BinaryRowData>) keyedStateBackend,
                    ((RowDataSerializer) keyedStateBackend.getKeySerializer())
                            .toBinaryRow((RowData) key));
        } else {
            setCurrentKeyForStreaming((KeyedStateBackend<Object>) keyedStateBackend, key);
        }
    }
}
