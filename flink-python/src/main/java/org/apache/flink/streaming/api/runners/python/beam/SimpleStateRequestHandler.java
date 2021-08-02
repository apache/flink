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

package org.apache.flink.streaming.api.runners.python.beam;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalMergingState;
import org.apache.flink.streaming.api.utils.ByteArrayWrapper;
import org.apache.flink.streaming.api.utils.ByteArrayWrapperSerializer;
import org.apache.flink.streaming.api.utils.ProtoUtils;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.AbstractRowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.base.Charsets;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf.ByteString;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.setCurrentKeyForStreaming;

/** A state request handler which handles the state request from Python side. */
@Internal
public class SimpleStateRequestHandler implements StateRequestHandler {

    private static final String CLEAR_CACHED_ITERATOR_MARK = "clear_iterators";
    private static final String MERGE_NAMESPACES_MARK = "merge_namespaces";
    private static final String PYTHON_STATE_PREFIX = "python-state-";

    // map state GET request flags
    private static final byte GET_FLAG = 0;
    private static final byte ITERATE_FLAG = 1;
    private static final byte CHECK_EMPTY_FLAG = 2;

    // map state GET response flags
    private static final byte EXIST_FLAG = 0;
    private static final byte IS_NONE_FLAG = 1;
    private static final byte NOT_EXIST_FLAG = 2;
    private static final byte IS_EMPTY_FLAG = 3;
    private static final byte NOT_EMPTY_FLAG = 4;

    // map state APPEND request flags
    private static final byte DELETE = 0;
    private static final byte SET_NONE = 1;
    private static final byte SET_VALUE = 2;

    private static final BeamFnApi.StateGetResponse.Builder NOT_EXIST_RESPONSE =
            BeamFnApi.StateGetResponse.newBuilder()
                    .setData(ByteString.copyFrom(new byte[] {NOT_EXIST_FLAG}));
    private static final BeamFnApi.StateGetResponse.Builder IS_NONE_RESPONSE =
            BeamFnApi.StateGetResponse.newBuilder()
                    .setData(ByteString.copyFrom(new byte[] {IS_NONE_FLAG}));
    private static final BeamFnApi.StateGetResponse.Builder IS_EMPTY_RESPONSE =
            BeamFnApi.StateGetResponse.newBuilder()
                    .setData(ByteString.copyFrom(new byte[] {IS_EMPTY_FLAG}));
    private static final BeamFnApi.StateGetResponse.Builder NOT_EMPTY_RESPONSE =
            BeamFnApi.StateGetResponse.newBuilder()
                    .setData(ByteString.copyFrom(new byte[] {NOT_EMPTY_FLAG}));

    private final TypeSerializer keySerializer;
    private final TypeSerializer namespaceSerializer;
    private final TypeSerializer<byte[]> valueSerializer;
    private final KeyedStateBackend keyedStateBackend;

    /** Reusable InputStream used to holding the elements to be deserialized. */
    private final ByteArrayInputStreamWithPos bais;

    /** InputStream Wrapper. */
    private final DataInputViewStreamWrapper baisWrapper;

    /** Reusable OutputStream used to holding the serialized input elements. */
    private final ByteArrayOutputStreamWithPos baos;

    /** OutputStream Wrapper. */
    private final DataOutputViewStreamWrapper baosWrapper;

    /** The cache of the stateDescriptors. */
    private final Map<String, StateDescriptor> stateDescriptorCache;

    /** The cache of the map state iterators. */
    private final Map<ByteArrayWrapper, Iterator> mapStateIteratorCache;

    private final int mapStateIterateResponseBatchSize;

    private final ByteArrayWrapper reuseByteArrayWrapper = new ByteArrayWrapper(new byte[0]);

    /** Let StateRequestHandler for user state only use a single cache token. */
    private final BeamFnApi.ProcessBundleRequest.CacheToken cacheToken;

    SimpleStateRequestHandler(
            KeyedStateBackend keyedStateBackend,
            TypeSerializer keySerializer,
            TypeSerializer namespaceSerializer,
            Map<String, String> config) {
        this.keyedStateBackend = keyedStateBackend;
        TypeSerializer frameworkKeySerializer = keyedStateBackend.getKeySerializer();
        if (!(frameworkKeySerializer instanceof AbstractRowDataSerializer
                || frameworkKeySerializer instanceof RowSerializer)) {
            throw new RuntimeException("Currently SimpleStateRequestHandler only support row key!");
        }
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.valueSerializer =
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO.createSerializer(
                        new ExecutionConfig());
        bais = new ByteArrayInputStreamWithPos();
        baisWrapper = new DataInputViewStreamWrapper(bais);
        baos = new ByteArrayOutputStreamWithPos();
        baosWrapper = new DataOutputViewStreamWrapper(baos);
        stateDescriptorCache = new HashMap<>();
        mapStateIteratorCache = new HashMap<>();
        mapStateIterateResponseBatchSize =
                Integer.valueOf(
                        config.getOrDefault(
                                PythonOptions.MAP_STATE_ITERATE_RESPONSE_BATCH_SIZE.key(),
                                PythonOptions.MAP_STATE_ITERATE_RESPONSE_BATCH_SIZE
                                        .defaultValue()
                                        .toString()));
        if (mapStateIterateResponseBatchSize <= 0) {
            throw new RuntimeException(
                    String.format(
                            "The value of '%s' must be greater than 0!",
                            PythonOptions.MAP_STATE_ITERATE_RESPONSE_BATCH_SIZE.key()));
        }
        cacheToken = createCacheToken();
    }

    @Override
    public Iterable<BeamFnApi.ProcessBundleRequest.CacheToken> getCacheTokens() {
        return Collections.singleton(cacheToken);
    }

    @Override
    public CompletionStage<BeamFnApi.StateResponse.Builder> handle(BeamFnApi.StateRequest request)
            throws Exception {
        BeamFnApi.StateKey.TypeCase typeCase = request.getStateKey().getTypeCase();
        synchronized (keyedStateBackend) {
            if (typeCase.equals(BeamFnApi.StateKey.TypeCase.BAG_USER_STATE)) {
                return handleBagState(request);
            } else if (typeCase.equals(BeamFnApi.StateKey.TypeCase.MULTIMAP_SIDE_INPUT)) {
                return handleMapState(request);
            } else {
                throw new RuntimeException("Unsupported state type: " + typeCase);
            }
        }
    }

    private CompletionStage<BeamFnApi.StateResponse.Builder> handleBagState(
            BeamFnApi.StateRequest request) throws Exception {
        if (request.getStateKey().hasBagUserState()) {
            BeamFnApi.StateKey.BagUserState bagUserState = request.getStateKey().getBagUserState();
            // get key
            byte[] keyBytes = bagUserState.getKey().toByteArray();
            bais.setBuffer(keyBytes, 0, keyBytes.length);
            Object key = keySerializer.deserialize(baisWrapper);
            if (keyedStateBackend.getKeySerializer() instanceof RowDataSerializer) {
                setCurrentKeyForStreaming(
                        keyedStateBackend,
                        ((RowDataSerializer) keyedStateBackend.getKeySerializer())
                                .toBinaryRow((RowData) key));
            } else {
                setCurrentKeyForStreaming(keyedStateBackend, key);
            }
        } else {
            throw new RuntimeException("Unsupported bag state request: " + request);
        }

        switch (request.getRequestCase()) {
            case GET:
                return handleBagGetRequest(request);
            case APPEND:
                return handleBagAppendRequest(request);
            case CLEAR:
                return handleBagClearRequest(request);
            default:
                throw new RuntimeException(
                        String.format(
                                "Unsupported request type %s for user state.",
                                request.getRequestCase()));
        }
    }

    private List<ByteString> convertToByteString(ListState<byte[]> listState) throws Exception {
        List<ByteString> ret = new LinkedList<>();
        if (listState.get() == null) {
            return ret;
        }
        for (byte[] v : listState.get()) {
            ret.add(ByteString.copyFrom(v));
        }
        return ret;
    }

    private CompletionStage<BeamFnApi.StateResponse.Builder> handleBagGetRequest(
            BeamFnApi.StateRequest request) throws Exception {

        ListState<byte[]> partitionedState = getListState(request);
        List<ByteString> byteStrings = convertToByteString(partitionedState);

        return CompletableFuture.completedFuture(
                BeamFnApi.StateResponse.newBuilder()
                        .setId(request.getId())
                        .setGet(
                                BeamFnApi.StateGetResponse.newBuilder()
                                        .setData(ByteString.copyFrom(byteStrings))));
    }

    private CompletionStage<BeamFnApi.StateResponse.Builder> handleBagAppendRequest(
            BeamFnApi.StateRequest request) throws Exception {

        ListState<byte[]> partitionedState = getListState(request);
        if (request.getStateKey()
                .getBagUserState()
                .getTransformId()
                .equals(MERGE_NAMESPACES_MARK)) {
            // get namespaces to merge
            byte[] namespacesBytes = request.getAppend().getData().toByteArray();
            bais.setBuffer(namespacesBytes, 0, namespacesBytes.length);
            int namespaceCount = baisWrapper.readInt();
            Set<Object> namespaces = new HashSet<>();
            for (int i = 0; i < namespaceCount; i++) {
                namespaces.add(namespaceSerializer.deserialize(baisWrapper));
            }
            byte[] targetNamespaceByte =
                    request.getStateKey().getBagUserState().getWindow().toByteArray();
            bais.setBuffer(targetNamespaceByte, 0, targetNamespaceByte.length);
            Object targetNamespace = namespaceSerializer.deserialize(baisWrapper);
            ((InternalMergingState) partitionedState).mergeNamespaces(targetNamespace, namespaces);
        } else {
            // get values
            byte[] valueBytes = request.getAppend().getData().toByteArray();
            partitionedState.add(valueBytes);
        }
        return CompletableFuture.completedFuture(
                BeamFnApi.StateResponse.newBuilder()
                        .setId(request.getId())
                        .setAppend(BeamFnApi.StateAppendResponse.getDefaultInstance()));
    }

    private CompletionStage<BeamFnApi.StateResponse.Builder> handleBagClearRequest(
            BeamFnApi.StateRequest request) throws Exception {

        ListState<byte[]> partitionedState = getListState(request);

        partitionedState.clear();
        return CompletableFuture.completedFuture(
                BeamFnApi.StateResponse.newBuilder()
                        .setId(request.getId())
                        .setClear(BeamFnApi.StateClearResponse.getDefaultInstance()));
    }

    private ListState<byte[]> getListState(BeamFnApi.StateRequest request) throws Exception {
        BeamFnApi.StateKey.BagUserState bagUserState = request.getStateKey().getBagUserState();
        byte[] data = Base64.getDecoder().decode(bagUserState.getUserStateId());
        FlinkFnApi.StateDescriptor stateDescriptor = FlinkFnApi.StateDescriptor.parseFrom(data);
        String stateName = PYTHON_STATE_PREFIX + stateDescriptor.getStateName();
        ListStateDescriptor<byte[]> listStateDescriptor;
        StateDescriptor cachedStateDescriptor = stateDescriptorCache.get(stateName);
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
            bais.setBuffer(windowBytes, 0, windowBytes.length);
            Object namespace = namespaceSerializer.deserialize(baisWrapper);
            return (ListState<byte[]>)
                    keyedStateBackend.getPartitionedState(
                            namespace, namespaceSerializer, listStateDescriptor);
        } else {
            return (ListState<byte[]>)
                    keyedStateBackend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            listStateDescriptor);
        }
    }

    private CompletionStage<BeamFnApi.StateResponse.Builder> handleMapState(
            BeamFnApi.StateRequest request) throws Exception {
        // Currently the `beam_fn_api.proto` does not support MapState, so we use the
        // the `MultimapSideInput` message to mark the state as a MapState for now.
        if (request.getStateKey().hasMultimapSideInput()) {
            BeamFnApi.StateKey.MultimapSideInput mapUserState =
                    request.getStateKey().getMultimapSideInput();
            // get key
            byte[] keyBytes = mapUserState.getKey().toByteArray();
            bais.setBuffer(keyBytes, 0, keyBytes.length);
            Object key = keySerializer.deserialize(baisWrapper);
            if (keyedStateBackend.getKeySerializer() instanceof RowDataSerializer) {
                setCurrentKeyForStreaming(
                        keyedStateBackend,
                        ((RowDataSerializer) keyedStateBackend.getKeySerializer())
                                .toBinaryRow((RowData) key));
            } else {
                setCurrentKeyForStreaming(keyedStateBackend, key);
            }
        } else {
            throw new RuntimeException("Unsupported bag state request: " + request);
        }

        switch (request.getRequestCase()) {
            case GET:
                return handleMapGetRequest(request);
            case APPEND:
                return handleMapAppendRequest(request);
            case CLEAR:
                return handleMapClearRequest(request);
            default:
                throw new RuntimeException(
                        String.format(
                                "Unsupported request type %s for user state.",
                                request.getRequestCase()));
        }
    }

    private CompletionStage<BeamFnApi.StateResponse.Builder> handleMapGetRequest(
            BeamFnApi.StateRequest request) throws Exception {
        MapState<ByteArrayWrapper, byte[]> mapState = getMapState(request);
        // The continuation token structure of GET request is:
        // [flag (1 byte)][serialized map key]
        // The continuation token structure of CHECK_EMPTY request is:
        // [flag (1 byte)]
        // The continuation token structure of ITERATE request is:
        // [flag (1 byte)][iterate type (1 byte)][iterator token length (int32)][iterator token]
        byte[] getRequest = request.getGet().getContinuationToken().toByteArray();
        byte getFlag = getRequest[0];
        BeamFnApi.StateGetResponse.Builder response;
        switch (getFlag) {
            case GET_FLAG:
                reuseByteArrayWrapper.setData(getRequest);
                reuseByteArrayWrapper.setOffset(1);
                reuseByteArrayWrapper.setLimit(getRequest.length);
                response = handleMapGetValueRequest(reuseByteArrayWrapper, mapState);
                break;
            case CHECK_EMPTY_FLAG:
                response = handleMapCheckEmptyRequest(mapState);
                break;
            case ITERATE_FLAG:
                bais.setBuffer(getRequest, 1, getRequest.length - 1);
                IterateType iterateType = IterateType.fromOrd(baisWrapper.readByte());
                int iterateTokenLength = baisWrapper.readInt();
                ByteArrayWrapper iterateToken;
                if (iterateTokenLength > 0) {
                    reuseByteArrayWrapper.setData(getRequest);
                    reuseByteArrayWrapper.setOffset(bais.getPosition());
                    reuseByteArrayWrapper.setLimit(bais.getPosition() + iterateTokenLength);
                    iterateToken = reuseByteArrayWrapper;
                } else {
                    iterateToken = null;
                }
                response = handleMapIterateRequest(mapState, iterateType, iterateToken);
                break;
            default:
                throw new RuntimeException(
                        String.format(
                                "Unsupported get request type: '%d' for map state.", getFlag));
        }

        return CompletableFuture.completedFuture(
                BeamFnApi.StateResponse.newBuilder().setId(request.getId()).setGet(response));
    }

    private CompletionStage<BeamFnApi.StateResponse.Builder> handleMapAppendRequest(
            BeamFnApi.StateRequest request) throws Exception {
        // The structure of append request bytes is:
        // [number of requests (int32)][append request flag (1 byte)][map key length
        // (int32)][map key]
        // [map value length (int32)][map value][append request flag (1 byte)][map key length
        // (int32)][map key]
        // ...
        byte[] appendBytes = request.getAppend().getData().toByteArray();
        bais.setBuffer(appendBytes, 0, appendBytes.length);
        MapState<ByteArrayWrapper, byte[]> mapState = getMapState(request);
        int subRequestNum = baisWrapper.readInt();
        for (int i = 0; i < subRequestNum; i++) {
            byte requestFlag = baisWrapper.readByte();
            int keyLength = baisWrapper.readInt();
            reuseByteArrayWrapper.setData(appendBytes);
            reuseByteArrayWrapper.setOffset(bais.getPosition());
            reuseByteArrayWrapper.setLimit(bais.getPosition() + keyLength);
            baisWrapper.skipBytesToRead(keyLength);
            switch (requestFlag) {
                case DELETE:
                    mapState.remove(reuseByteArrayWrapper);
                    break;
                case SET_NONE:
                    mapState.put(reuseByteArrayWrapper.copy(), null);
                    break;
                case SET_VALUE:
                    int valueLength = baisWrapper.readInt();
                    byte[] valueBytes = new byte[valueLength];
                    int readLength = baisWrapper.read(valueBytes);
                    assert valueLength == readLength;
                    mapState.put(reuseByteArrayWrapper.copy(), valueBytes);
                    break;
                default:
                    throw new RuntimeException(
                            String.format(
                                    "Unsupported append request type: '%d' for map state.",
                                    requestFlag));
            }
        }
        return CompletableFuture.completedFuture(
                BeamFnApi.StateResponse.newBuilder()
                        .setId(request.getId())
                        .setAppend(BeamFnApi.StateAppendResponse.getDefaultInstance()));
    }

    private CompletionStage<BeamFnApi.StateResponse.Builder> handleMapClearRequest(
            BeamFnApi.StateRequest request) throws Exception {
        if (request.getStateKey()
                .getMultimapSideInput()
                .getTransformId()
                .equals(CLEAR_CACHED_ITERATOR_MARK)) {
            mapStateIteratorCache.clear();
        } else {
            MapState<ByteArrayWrapper, byte[]> partitionedState = getMapState(request);
            partitionedState.clear();
        }
        return CompletableFuture.completedFuture(
                BeamFnApi.StateResponse.newBuilder()
                        .setId(request.getId())
                        .setClear(BeamFnApi.StateClearResponse.getDefaultInstance()));
    }

    private BeamFnApi.StateGetResponse.Builder handleMapGetValueRequest(
            ByteArrayWrapper key, MapState<ByteArrayWrapper, byte[]> mapState) throws Exception {
        if (mapState.contains(key)) {
            byte[] value = mapState.get(key);
            if (value == null) {
                return IS_NONE_RESPONSE;
            } else {
                baos.reset();
                baosWrapper.writeByte(EXIST_FLAG);
                baosWrapper.write(value);
                return BeamFnApi.StateGetResponse.newBuilder()
                        .setData(ByteString.copyFrom(baos.toByteArray()));
            }
        } else {
            return NOT_EXIST_RESPONSE;
        }
    }

    private BeamFnApi.StateGetResponse.Builder handleMapCheckEmptyRequest(
            MapState<ByteArrayWrapper, byte[]> mapState) throws Exception {
        if (mapState.isEmpty()) {
            return IS_EMPTY_RESPONSE;
        } else {
            return NOT_EMPTY_RESPONSE;
        }
    }

    private BeamFnApi.StateGetResponse.Builder handleMapIterateRequest(
            MapState<ByteArrayWrapper, byte[]> mapState,
            IterateType iterateType,
            ByteArrayWrapper iteratorToken)
            throws Exception {
        final Iterator iterator;
        if (iteratorToken == null) {
            switch (iterateType) {
                case ITEMS:
                case VALUES:
                    iterator = mapState.iterator();
                    break;
                case KEYS:
                    iterator = mapState.keys().iterator();
                    break;
                default:
                    throw new RuntimeException("Unsupported iterate type: " + iterateType);
            }
        } else {
            iterator = mapStateIteratorCache.get(iteratorToken);
            if (iterator == null) {
                throw new RuntimeException("The cached iterator does not exist!");
            }
        }
        baos.reset();
        switch (iterateType) {
            case ITEMS:
            case VALUES:
                Iterator<Map.Entry<ByteArrayWrapper, byte[]>> entryIterator = iterator;
                for (int i = 0; i < mapStateIterateResponseBatchSize; i++) {
                    if (entryIterator.hasNext()) {
                        Map.Entry<ByteArrayWrapper, byte[]> entry = entryIterator.next();
                        ByteArrayWrapper key = entry.getKey();
                        baosWrapper.write(
                                key.getData(), key.getOffset(), key.getLimit() - key.getOffset());
                        baosWrapper.writeBoolean(entry.getValue() != null);
                        if (entry.getValue() != null) {
                            baosWrapper.write(entry.getValue());
                        }
                    } else {
                        break;
                    }
                }
                break;
            case KEYS:
                Iterator<ByteArrayWrapper> keyIterator = iterator;
                for (int i = 0; i < mapStateIterateResponseBatchSize; i++) {
                    if (keyIterator.hasNext()) {
                        ByteArrayWrapper key = keyIterator.next();
                        baosWrapper.write(
                                key.getData(), key.getOffset(), key.getLimit() - key.getOffset());
                    } else {
                        break;
                    }
                }
                break;
            default:
                throw new RuntimeException("Unsupported iterate type: " + iterateType);
        }
        if (!iterator.hasNext()) {
            if (iteratorToken != null) {
                mapStateIteratorCache.remove(iteratorToken);
            }
            iteratorToken = null;
        } else {
            if (iteratorToken == null) {
                iteratorToken = new ByteArrayWrapper(UUID.randomUUID().toString().getBytes());
            }
            mapStateIteratorCache.put(iteratorToken, iterator);
        }
        BeamFnApi.StateGetResponse.Builder responseBuilder =
                BeamFnApi.StateGetResponse.newBuilder()
                        .setData(ByteString.copyFrom(baos.toByteArray()));
        if (iteratorToken != null) {
            responseBuilder.setContinuationToken(
                    ByteString.copyFrom(
                            iteratorToken.getData(),
                            iteratorToken.getOffset(),
                            iteratorToken.getLimit() - iteratorToken.getOffset()));
        }
        return responseBuilder;
    }

    private MapState<ByteArrayWrapper, byte[]> getMapState(BeamFnApi.StateRequest request)
            throws Exception {
        BeamFnApi.StateKey.MultimapSideInput mapUserState =
                request.getStateKey().getMultimapSideInput();
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
            bais.setBuffer(windowBytes, 0, windowBytes.length);
            Object namespace = namespaceSerializer.deserialize(baisWrapper);
            return (MapState<ByteArrayWrapper, byte[]>)
                    keyedStateBackend.getPartitionedState(
                            namespace, namespaceSerializer, mapStateDescriptor);
        } else {
            return (MapState<ByteArrayWrapper, byte[]>)
                    keyedStateBackend.getPartitionedState(
                            VoidNamespace.INSTANCE,
                            VoidNamespaceSerializer.INSTANCE,
                            mapStateDescriptor);
        }
    }

    private BeamFnApi.ProcessBundleRequest.CacheToken createCacheToken() {
        ByteString token =
                ByteString.copyFrom(UUID.randomUUID().toString().getBytes(Charsets.UTF_8));
        return BeamFnApi.ProcessBundleRequest.CacheToken.newBuilder()
                .setUserState(
                        BeamFnApi.ProcessBundleRequest.CacheToken.UserState.getDefaultInstance())
                .setToken(token)
                .build();
    }

    /** The type of the Python map state iterate request. */
    private enum IterateType {

        /** Equivalent to iterate {@link Map#entrySet() }. */
        ITEMS((byte) 0),

        /** Equivalent to iterate {@link Map#keySet() }. */
        KEYS((byte) 1),

        /** Equivalent to iterate {@link Map#values() }. */
        VALUES((byte) 2);

        private final byte ord;

        IterateType(byte ord) {
            this.ord = ord;
        }

        public byte getOrd() {
            return ord;
        }

        public static IterateType fromOrd(byte ord) {
            switch (ord) {
                case 0:
                    return ITEMS;
                case 1:
                    return KEYS;
                case 2:
                    return VALUES;
                default:
                    throw new RuntimeException("Unsupported ordinal: " + ord);
            }
        }
    }
}
