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

package org.apache.flink.runtime.webmonitor.history;

import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.router.RoutedRequest;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Abstract base class for handling HTTP requests for files in the History Server.
 *
 * <p>This handler serves files from a specified root directory and can optionally refresh archives
 * from a remote source if configured. Subclasses should implement the {@link
 * #respondWithFile(ChannelHandlerContext, HttpRequest, String)} method to provide specific behavior
 * for serving files.
 *
 * <p>The handler also logs errors and sends appropriate error responses if an exception occurs
 * while processing a request.
 */
@ChannelHandler.Sharable
public abstract class HistoryServerServerHandler
        extends SimpleChannelInboundHandler<RoutedRequest> {

    /** Default logger, if none is specified. */
    protected static final Logger LOG = LoggerFactory.getLogger(HistoryServerServerHandler.class);

    protected final File rootPath;

    public HistoryServerServerHandler(File rootPath) throws IOException {
        this.rootPath = checkNotNull(rootPath).getCanonicalFile();
    }

    // ------------------------------------------------------------------------
    //  Responses to requests
    // ------------------------------------------------------------------------

    @Override
    public void channelRead0(ChannelHandlerContext ctx, RoutedRequest routedRequest)
            throws Exception {
        String requestPath = routedRequest.getPath();

        try {
            respondWithFile(ctx, routedRequest.getRequest(), requestPath);
        } catch (RestHandlerException rhe) {
            HandlerUtils.sendErrorResponse(
                    ctx,
                    routedRequest.getRequest(),
                    new ErrorResponseBody(rhe.getMessage()),
                    rhe.getHttpResponseStatus(),
                    Collections.emptyMap());
        }
    }

    /** Response when running with leading JobManager. */
    protected abstract void respondWithFile(
            ChannelHandlerContext ctx, HttpRequest request, String requestPath) throws Exception;

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        if (ctx.channel().isActive()) {
            LOG.error("Caught exception", cause);
            HandlerUtils.sendErrorResponse(
                    ctx,
                    false,
                    new ErrorResponseBody("Internal server error."),
                    INTERNAL_SERVER_ERROR,
                    Collections.emptyMap());
        }
    }
}
