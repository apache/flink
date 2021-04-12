/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.handler.cluster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.LogInfo;
import org.apache.flink.runtime.rest.messages.LogListInfo;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/** Handler which serves detailed JobManager log list information. */
public class JobManagerLogListHandler
        extends AbstractRestHandler<
                RestfulGateway, EmptyRequestBody, LogListInfo, EmptyMessageParameters> {

    private final File logDir;

    public JobManagerLogListHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, LogListInfo, EmptyMessageParameters> messageHeaders,
            @Nullable File logDir) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);

        this.logDir = logDir;
    }

    @Override
    protected CompletableFuture<LogListInfo> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request,
            @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        if (logDir == null) {
            return CompletableFuture.completedFuture(new LogListInfo(Collections.emptyList()));
        }
        final File[] logFiles = logDir.listFiles();
        if (logFiles == null) {
            return FutureUtils.completedExceptionally(
                    new IOException("Could not list files in " + logDir));
        }
        final List<LogInfo> logsWithLength =
                Arrays.stream(logFiles)
                        .filter(File::isFile)
                        .map(logFile -> new LogInfo(logFile.getName(), logFile.length()))
                        .collect(Collectors.toList());
        return CompletableFuture.completedFuture(new LogListInfo(logsWithLength));
    }
}
