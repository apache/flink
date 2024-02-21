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

package org.apache.flink.runtime.rest.handler.taskmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.blob.TransientBlobKey;
import org.apache.flink.runtime.blob.TransientBlobService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.UntypedResponseMessageHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.concurrent.FutureUtils;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** Testing implementation of {@link TaskManagerStdoutFileHandler}. */
public class TestingTaskManagerStdoutFileHandler extends TaskManagerStdoutFileHandler {
    private final Exception exceptionForRequestFileUpload;

    public TestingTaskManagerStdoutFileHandler(
            @Nonnull GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            @Nonnull Time timeout,
            @Nonnull Map<String, String> responseHeaders,
            @Nonnull
                    UntypedResponseMessageHeaders<EmptyRequestBody, TaskManagerMessageParameters>
                            untypedResponseMessageHeaders,
            @Nonnull GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever,
            @Nonnull TransientBlobService transientBlobService,
            @Nonnull Time cacheEntryDuration,
            @Nonnull Exception exceptionForRequestFileUpload) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                untypedResponseMessageHeaders,
                resourceManagerGatewayRetriever,
                transientBlobService,
                cacheEntryDuration);
        this.exceptionForRequestFileUpload = exceptionForRequestFileUpload;
    }

    @Override
    protected CompletableFuture<TransientBlobKey> requestFileUpload(
            ResourceManagerGateway resourceManagerGateway,
            Tuple2<ResourceID, String> taskManagerIdAndFileName) {
        return FutureUtils.completedExceptionally(exceptionForRequestFileUpload);
    }
}
