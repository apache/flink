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

package org.apache.flink.runtime.rest.handler.cluster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.AbstractHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.FlinkException;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;

import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Base class for serving files from the JobManager. */
public abstract class AbstractJobManagerFileHandler<M extends MessageParameters>
        extends AbstractHandler<RestfulGateway, EmptyRequestBody, M> {

    protected AbstractJobManagerFileHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            UntypedResponseMessageHeaders<EmptyRequestBody, M> messageHeaders) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
    }

    @Override
    protected CompletableFuture<Void> respondToRequest(
            ChannelHandlerContext ctx,
            HttpRequest httpRequest,
            HandlerRequest<EmptyRequestBody, M> handlerRequest,
            RestfulGateway gateway) {
        File file = getFile(handlerRequest);
        if (file != null && file.exists()) {
            try {
                HandlerUtils.transferFile(ctx, file, httpRequest);
            } catch (FlinkException e) {
                throw new CompletionException(
                        new FlinkException("Could not transfer file to client.", e));
            }
            return CompletableFuture.completedFuture(null);
        } else {
            return HandlerUtils.sendErrorResponse(
                    ctx,
                    httpRequest,
                    new ErrorResponseBody("This file does not exist in JobManager log dir."),
                    HttpResponseStatus.NOT_FOUND,
                    Collections.emptyMap());
        }
    }

    @Nullable
    protected abstract File getFile(HandlerRequest<EmptyRequestBody, M> handlerRequest);
}
