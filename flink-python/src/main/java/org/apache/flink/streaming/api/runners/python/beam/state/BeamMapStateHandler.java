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

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.api.utils.ByteArrayWrapper;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/** BeamMapStateHandler handles operations on a {@link MapState}. */
public class BeamMapStateHandler
        extends AbstractBeamStateHandler<MapState<ByteArrayWrapper, byte[]>> {

    private static final String CLEAR_CACHED_ITERATOR_MARK = "clear_iterators";

    /** Map state GET request flags. */
    private static final byte GET_FLAG = 0;

    private static final byte ITERATE_FLAG = 1;
    private static final byte CHECK_EMPTY_FLAG = 2;

    /** Map state GET response flags. */
    private static final byte EXIST_FLAG = 0;

    private static final byte IS_NONE_FLAG = 1;
    private static final byte NOT_EXIST_FLAG = 2;
    private static final byte IS_EMPTY_FLAG = 3;
    private static final byte NOT_EMPTY_FLAG = 4;

    /** Map state APPEND request flags. */
    private static final byte DELETE = 0;

    private static final byte SET_NONE = 1;
    private static final byte SET_VALUE = 2;

    /** Predefined Beam state responses. */
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

    /** Reusable InputStream used to holding the elements to be deserialized. */
    private final ByteArrayInputStreamWithPos bais;

    /** InputStream Wrapper. */
    private final DataInputViewStreamWrapper baisWrapper;

    /** Reusable OutputStream used to holding the serialized input elements. */
    private final ByteArrayOutputStreamWithPos baos;

    /** OutputStream Wrapper. */
    private final DataOutputViewStreamWrapper baosWrapper;

    /** Reusable ByteArrayWrapper for decoding request data. */
    private final ByteArrayWrapper reuseByteArrayWrapper = new ByteArrayWrapper(new byte[0]);

    /** The cache of the map state iterators. */
    private final Map<ByteArrayWrapper, Iterator<?>> mapStateIteratorCache = new HashMap<>();

    private final int mapStateIterateResponseBatchSize;

    public BeamMapStateHandler(ReadableConfig config) {
        this.bais = new ByteArrayInputStreamWithPos();
        this.baisWrapper = new DataInputViewStreamWrapper(bais);
        this.baos = new ByteArrayOutputStreamWithPos();
        this.baosWrapper = new DataOutputViewStreamWrapper(baos);
        this.mapStateIterateResponseBatchSize =
                config.get(PythonOptions.MAP_STATE_ITERATE_RESPONSE_BATCH_SIZE);
        if (mapStateIterateResponseBatchSize <= 0) {
            throw new RuntimeException(
                    String.format(
                            "The value of '%s' must be greater than 0!",
                            PythonOptions.MAP_STATE_ITERATE_RESPONSE_BATCH_SIZE.key()));
        }
    }

    public BeamFnApi.StateResponse.Builder handleGet(
            BeamFnApi.StateRequest request, MapState<ByteArrayWrapper, byte[]> mapState)
            throws Exception {
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
                response = handleGetValue(reuseByteArrayWrapper, mapState);
                break;
            case CHECK_EMPTY_FLAG:
                response = handleCheckEmpty(mapState);
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
                response = handleIterate(mapState, iterateType, iterateToken);
                break;
            default:
                throw new RuntimeException(
                        String.format(
                                "Unsupported get request type: '%d' for map state.", getFlag));
        }

        return BeamFnApi.StateResponse.newBuilder().setId(request.getId()).setGet(response);
    }

    public BeamFnApi.StateResponse.Builder handleAppend(
            BeamFnApi.StateRequest request, MapState<ByteArrayWrapper, byte[]> mapState)
            throws Exception {
        // The structure of append request bytes is:
        // [number of requests (int32)][append request flag (1 byte)][map key length
        // (int32)][map key]
        // [map value length (int32)][map value][append request flag (1 byte)][map key length
        // (int32)][map key]
        // ...
        byte[] appendBytes = request.getAppend().getData().toByteArray();
        bais.setBuffer(appendBytes, 0, appendBytes.length);
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
        return BeamFnApi.StateResponse.newBuilder()
                .setId(request.getId())
                .setAppend(BeamFnApi.StateAppendResponse.getDefaultInstance());
    }

    public BeamFnApi.StateResponse.Builder handleClear(
            BeamFnApi.StateRequest request, MapState<ByteArrayWrapper, byte[]> mapState)
            throws Exception {
        if (request.getStateKey()
                .getMultimapSideInput()
                .getTransformId()
                .equals(CLEAR_CACHED_ITERATOR_MARK)) {
            mapStateIteratorCache.clear();
        } else {
            mapState.clear();
        }
        return BeamFnApi.StateResponse.newBuilder()
                .setId(request.getId())
                .setClear(BeamFnApi.StateClearResponse.getDefaultInstance());
    }

    private BeamFnApi.StateGetResponse.Builder handleGetValue(
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

    private BeamFnApi.StateGetResponse.Builder handleCheckEmpty(
            MapState<ByteArrayWrapper, byte[]> mapState) throws Exception {
        if (mapState.isEmpty()) {
            return IS_EMPTY_RESPONSE;
        } else {
            return NOT_EMPTY_RESPONSE;
        }
    }

    private BeamFnApi.StateGetResponse.Builder handleIterate(
            MapState<ByteArrayWrapper, byte[]> mapState,
            IterateType iterateType,
            ByteArrayWrapper iteratorToken)
            throws Exception {
        final Iterator<?> iterator;
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
                Iterator<Map.Entry<ByteArrayWrapper, byte[]>> entryIterator =
                        (Iterator<Map.Entry<ByteArrayWrapper, byte[]>>) iterator;
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
                Iterator<ByteArrayWrapper> keyIterator = (Iterator<ByteArrayWrapper>) iterator;
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
}
