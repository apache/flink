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
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.streaming.api.utils.ByteArrayWrapper;
import org.apache.flink.streaming.api.utils.ByteArrayWrapperSerializer;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;

import java.util.Base64;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * A {@link BeamStateStore} that returns operator states based on {@link BeamFnApi.StateRequest}.
 */
public class BeamOperatorStateStore implements BeamStateStore {

    private final OperatorStateBackend operatorStateBackend;

    private final TypeSerializer<byte[]> valueSerializer;

    /** The cache of the stateDescriptors. */
    private final Map<String, StateDescriptor<?, ?>> stateDescriptorCache;

    public BeamOperatorStateStore(OperatorStateBackend operatorStateBackend) {
        this.operatorStateBackend = operatorStateBackend;
        this.valueSerializer =
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO.createSerializer(
                        new ExecutionConfig());
        this.stateDescriptorCache = new HashMap<>();
    }

    /** Currently list state and union-list state is not supported. */
    @Override
    public ListState<byte[]> getListState(BeamFnApi.StateRequest request) throws Exception {
        throw new RuntimeException("Operator list state is still not supported");
    }

    /** Returns a {@link BroadcastState} wrapped in {@link MapState} interface. */
    @Override
    public MapState<ByteArrayWrapper, byte[]> getMapState(BeamFnApi.StateRequest request)
            throws Exception {
        if (!request.getStateKey().hasMultimapKeysSideInput()) {
            throw new RuntimeException("Unsupported broadcast state request: " + request);
        }
        BeamFnApi.StateKey.MultimapKeysSideInput mapUserState =
                request.getStateKey().getMultimapKeysSideInput();

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

        // Currently, operator state is only supported to be used as broadcast state in PyFlink
        final BroadcastState<ByteArrayWrapper, byte[]> broadcastState =
                operatorStateBackend.getBroadcastState(mapStateDescriptor);
        return new MapState<ByteArrayWrapper, byte[]>() {
            @Override
            public byte[] get(ByteArrayWrapper key) throws Exception {
                return broadcastState.get(key);
            }

            @Override
            public void put(ByteArrayWrapper key, byte[] value) throws Exception {
                broadcastState.put(key, value);
            }

            @Override
            public void putAll(Map<ByteArrayWrapper, byte[]> map) throws Exception {
                broadcastState.putAll(map);
            }

            @Override
            public void remove(ByteArrayWrapper key) throws Exception {
                broadcastState.remove(key);
            }

            @Override
            public boolean contains(ByteArrayWrapper key) throws Exception {
                return broadcastState.contains(key);
            }

            @Override
            public Iterable<Map.Entry<ByteArrayWrapper, byte[]>> entries() throws Exception {
                return broadcastState.entries();
            }

            @Override
            public Iterable<ByteArrayWrapper> keys() throws Exception {
                final Iterator<Map.Entry<ByteArrayWrapper, byte[]>> iterator = iterator();
                return () ->
                        new Iterator<ByteArrayWrapper>() {

                            @Override
                            public boolean hasNext() {
                                return iterator.hasNext();
                            }

                            @Override
                            public ByteArrayWrapper next() {
                                return iterator.next().getKey();
                            }
                        };
            }

            @Override
            public Iterable<byte[]> values() throws Exception {
                final Iterator<Map.Entry<ByteArrayWrapper, byte[]>> iterator = iterator();
                return () ->
                        new Iterator<byte[]>() {

                            @Override
                            public boolean hasNext() {
                                return iterator.hasNext();
                            }

                            @Override
                            public byte[] next() {
                                return iterator.next().getValue();
                            }
                        };
            }

            @Override
            public Iterator<Map.Entry<ByteArrayWrapper, byte[]>> iterator() throws Exception {
                return broadcastState.entries().iterator();
            }

            @Override
            public boolean isEmpty() throws Exception {
                return iterator().hasNext();
            }

            @Override
            public void clear() {
                broadcastState.clear();
            }
        };
    }
}
