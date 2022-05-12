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
 * limitations under the License
 */

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.rest.handler.router.RoutedRequest;
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.LogUrlResponse;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerIdPathParameter;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Handler for retrieving the log url of a specified TaskManager or JobManager. */
@ChannelHandler.Sharable
public class GeneratedLogUrlHandler extends SimpleChannelInboundHandler<RoutedRequest<Object>> {

    private final CompletableFuture<String> patternFuture;

    public GeneratedLogUrlHandler(CompletableFuture<String> patternFuture) {
        this.patternFuture = patternFuture;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RoutedRequest<Object> routedRequest)
            throws Exception {
        try {
            Map<String, String> pathParams = routedRequest.getRouteResult().pathParams();
            final String taskManagerId = pathParams.get(TaskManagerIdPathParameter.KEY);
            final String jobId = Preconditions.checkNotNull(pathParams.get(JobIDPathParameter.KEY));
            final LogUrlResponse response =
                    new LogUrlResponse(
                            generateLogUrl(
                                    patternFuture.get(5, TimeUnit.MILLISECONDS),
                                    jobId,
                                    taskManagerId));

            HandlerUtils.sendResponse(
                    ctx,
                    routedRequest.getRequest(),
                    response,
                    HttpResponseStatus.OK,
                    Collections.emptyMap());
        } catch (Exception e) {
            HandlerUtils.sendErrorResponse(
                    ctx,
                    routedRequest.getRequest(),
                    new ErrorResponseBody(e.getMessage()),
                    HttpResponseStatus.INTERNAL_SERVER_ERROR,
                    Collections.emptyMap());
        }
    }

    @VisibleForTesting
    static String generateLogUrl(String pattern, String jobId, String taskManagerId) {
        String generatedUrl = pattern.replaceAll("<jobid>", jobId);
        if (null != taskManagerId) {
            generatedUrl = generatedUrl.replaceAll("<tmid>", taskManagerId);
        }
        return generatedUrl;
    }
}
