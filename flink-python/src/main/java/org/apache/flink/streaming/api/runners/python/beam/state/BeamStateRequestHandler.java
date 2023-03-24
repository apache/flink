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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.streaming.api.utils.ByteArrayWrapper;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.fnexecution.state.StateRequestHandler;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.common.base.Charsets;
import org.apache.beam.vendor.grpc.v1p48p1.com.google.protobuf.ByteString;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * The handler for Beam state requests sent from Python side, which does actual operations on Flink
 * state.
 */
public class BeamStateRequestHandler implements StateRequestHandler {

    private final BeamStateStore keyedStateStore;

    private final BeamStateStore operatorStateStore;

    private final BeamStateHandler<ListState<byte[]>> bagStateHandler;

    private final BeamStateHandler<MapState<ByteArrayWrapper, byte[]>> mapStateHandler;

    private final BeamFnApi.ProcessBundleRequest.CacheToken cacheToken;

    public BeamStateRequestHandler(
            BeamStateStore keyedStateStore,
            BeamStateStore operatorStateStore,
            BeamStateHandler<ListState<byte[]>> bagStateHandler,
            BeamStateHandler<MapState<ByteArrayWrapper, byte[]>> mapStateHandler) {
        this.keyedStateStore = keyedStateStore;
        this.operatorStateStore = operatorStateStore;
        this.bagStateHandler = bagStateHandler;
        this.mapStateHandler = mapStateHandler;
        this.cacheToken = createCacheToken();
    }

    @Override
    public CompletionStage<BeamFnApi.StateResponse.Builder> handle(BeamFnApi.StateRequest request)
            throws Exception {
        BeamFnApi.StateKey.TypeCase typeCase = request.getStateKey().getTypeCase();
        ListState<byte[]> listState;
        MapState<ByteArrayWrapper, byte[]> mapState;

        switch (typeCase) {
            case BAG_USER_STATE:
                listState = keyedStateStore.getListState(request);
                return CompletableFuture.completedFuture(
                        bagStateHandler.handle(request, listState));
            case MULTIMAP_SIDE_INPUT:
                mapState = keyedStateStore.getMapState(request);
                return CompletableFuture.completedFuture(mapStateHandler.handle(request, mapState));
            case MULTIMAP_KEYS_SIDE_INPUT:
                mapState = operatorStateStore.getMapState(request);
                return CompletableFuture.completedFuture(mapStateHandler.handle(request, mapState));
            default:
                throw new RuntimeException("Unsupported state type: " + typeCase);
        }
    }

    @Override
    public Iterable<BeamFnApi.ProcessBundleRequest.CacheToken> getCacheTokens() {
        return Collections.singleton(cacheToken);
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

    /**
     * Create a {@link BeamStateRequestHandler}.
     *
     * @param keyedStateBackend if null, {@link BeamStateRequestHandler} would throw an error when
     *     receive keyed-state requests.
     * @param operatorStateBackend if null, {@link BeamStateRequestHandler} would throw an error
     *     when receive operator-state requests.
     * @param keySerializer key serializer for {@link KeyedStateBackend}, must not be null if {@code
     *     keyedStatedBackend} is not null.
     * @param namespaceSerializer namespace serializer for {@link KeyedStateBackend}, could be null
     *     when there's no window logic involved.
     * @param config state-related configurations
     * @return A new {@link BeamBagStateHandler}
     */
    public static BeamStateRequestHandler of(
            @Nullable KeyedStateBackend<?> keyedStateBackend,
            @Nullable OperatorStateBackend operatorStateBackend,
            @Nullable TypeSerializer<?> keySerializer,
            @Nullable TypeSerializer<?> namespaceSerializer,
            ReadableConfig config) {
        BeamStateStore keyedStateStore = BeamStateStore.unsupported();
        if (keyedStateBackend != null) {
            assert keySerializer != null;
            keyedStateStore =
                    new BeamKeyedStateStore(keyedStateBackend, keySerializer, namespaceSerializer);
        }

        BeamStateStore operatorStateStore = BeamStateStore.unsupported();
        if (operatorStateBackend != null) {
            operatorStateStore = new BeamOperatorStateStore(operatorStateBackend);
        }

        BeamStateHandler<ListState<byte[]>> bagStateBeamStateHandler =
                new BeamBagStateHandler(namespaceSerializer);
        BeamStateHandler<MapState<ByteArrayWrapper, byte[]>> mapStateBeamStateHandler =
                new BeamMapStateHandler(config);

        return new BeamStateRequestHandler(
                keyedStateStore,
                operatorStateStore,
                bagStateBeamStateHandler,
                mapStateBeamStateHandler);
    }
}
