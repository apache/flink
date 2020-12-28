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

package org.apache.flink.queryablestate.server;

import org.apache.flink.annotation.Internal;
import org.apache.flink.queryablestate.exceptions.UnknownKeyOrNamespaceException;
import org.apache.flink.queryablestate.exceptions.UnknownKvStateIdException;
import org.apache.flink.queryablestate.messages.KvStateInternalRequest;
import org.apache.flink.queryablestate.messages.KvStateResponse;
import org.apache.flink.queryablestate.network.AbstractServerHandler;
import org.apache.flink.queryablestate.network.messages.MessageSerializer;
import org.apache.flink.queryablestate.network.stats.KvStateRequestStats;
import org.apache.flink.runtime.query.KvStateEntry;
import org.apache.flink.runtime.query.KvStateInfo;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;

import java.util.concurrent.CompletableFuture;

/**
 * This handler dispatches asynchronous tasks, which query {@link InternalKvState} instances and
 * write the result to the channel.
 *
 * <p>The network threads receive the message, deserialize it and dispatch the query task. The
 * actual query is handled in a separate thread as it might otherwise block the network threads
 * (file I/O etc.).
 */
@Internal
@ChannelHandler.Sharable
public class KvStateServerHandler
        extends AbstractServerHandler<KvStateInternalRequest, KvStateResponse> {

    /** KvState registry holding references to the KvState instances. */
    private final KvStateRegistry registry;

    /**
     * Create the handler used by the {@link KvStateServerImpl}.
     *
     * @param server the {@link KvStateServerImpl} using the handler.
     * @param kvStateRegistry registry to query.
     * @param serializer the {@link MessageSerializer} used to (de-) serialize the different
     *     messages.
     * @param stats server statistics collector.
     */
    public KvStateServerHandler(
            final KvStateServerImpl server,
            final KvStateRegistry kvStateRegistry,
            final MessageSerializer<KvStateInternalRequest, KvStateResponse> serializer,
            final KvStateRequestStats stats) {

        super(server, serializer, stats);
        this.registry = Preconditions.checkNotNull(kvStateRegistry);
    }

    @Override
    public CompletableFuture<KvStateResponse> handleRequest(
            final long requestId, final KvStateInternalRequest request) {
        final CompletableFuture<KvStateResponse> responseFuture = new CompletableFuture<>();

        try {
            final KvStateEntry<?, ?, ?> kvState = registry.getKvState(request.getKvStateId());
            if (kvState == null) {
                responseFuture.completeExceptionally(
                        new UnknownKvStateIdException(getServerName(), request.getKvStateId()));
            } else {
                byte[] serializedKeyAndNamespace = request.getSerializedKeyAndNamespace();

                byte[] serializedResult = getSerializedValue(kvState, serializedKeyAndNamespace);
                if (serializedResult != null) {
                    responseFuture.complete(new KvStateResponse(serializedResult));
                } else {
                    responseFuture.completeExceptionally(
                            new UnknownKeyOrNamespaceException(getServerName()));
                }
            }
            return responseFuture;
        } catch (Throwable t) {
            String errMsg =
                    "Error while processing request with ID "
                            + requestId
                            + ". Caused by: "
                            + ExceptionUtils.stringifyException(t);
            responseFuture.completeExceptionally(new RuntimeException(errMsg));
            return responseFuture;
        }
    }

    private static <K, N, V> byte[] getSerializedValue(
            final KvStateEntry<K, N, V> entry, final byte[] serializedKeyAndNamespace)
            throws Exception {

        final InternalKvState<K, N, V> state = entry.getState();
        final KvStateInfo<K, N, V> infoForCurrentThread = entry.getInfoForCurrentThread();

        return state.getSerializedValue(
                serializedKeyAndNamespace,
                infoForCurrentThread.getKeySerializer(),
                infoForCurrentThread.getNamespaceSerializer(),
                infoForCurrentThread.getStateValueSerializer());
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        return CompletableFuture.completedFuture(null);
    }
}
