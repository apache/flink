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

package org.apache.flink.runtime.rest.util;

import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

/**
 * Utility {@link TestRestHandler} maintaining a queue of CompletableFuture's.
 *
 * @param <G> The RestfulGateway used by the handler.
 * @param <REQ> The RequestBody type the handler is processing.
 * @param <RES> The ResponseBody type the handler is returning.
 * @param <M> The MessageParameters type utilized by this handler.
 */
public class TestRestHandler<
                G extends RestfulGateway,
                REQ extends RequestBody,
                RES extends ResponseBody,
                M extends MessageParameters>
        extends AbstractRestHandler<G, REQ, RES, M> {

    private final Queue<CompletableFuture<RES>> responseQueue;

    public TestRestHandler(
            GatewayRetriever<G> gatewayRetriever,
            MessageHeaders<REQ, RES, M> messageHeaders,
            CompletableFuture<RES>... responses) {
        super(gatewayRetriever, RpcUtils.INF_TIMEOUT, Collections.emptyMap(), messageHeaders);

        responseQueue = new ArrayDeque<>(Arrays.asList(responses));
    }

    @Override
    protected CompletableFuture<RES> handleRequest(
            @Nullable HandlerRequest<REQ, M> request, @Nullable G gateway)
            throws RestHandlerException {
        final CompletableFuture<RES> result = responseQueue.poll();

        if (result != null) {
            return result;
        } else {
            return FutureUtils.completedExceptionally(
                    new NoSuchElementException("No pre-defined Futures left."));
        }
    }
}
