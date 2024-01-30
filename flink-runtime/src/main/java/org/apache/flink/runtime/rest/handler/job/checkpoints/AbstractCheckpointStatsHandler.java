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

package org.apache.flink.runtime.rest.handler.job.checkpoints;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rest.NotFoundException;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.guava31.com.google.common.cache.Cache;

import javax.annotation.Nonnull;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

/**
 * Abstract class for checkpoint handlers that will cache the {@link CheckpointStatsSnapshot}
 * object.
 *
 * @param <R> the response type
 * @param <M> the message parameters
 */
@Internal
public abstract class AbstractCheckpointStatsHandler<
                R extends ResponseBody, M extends JobMessageParameters>
        extends AbstractRestHandler<RestfulGateway, EmptyRequestBody, R, M> {

    private final Executor executor;
    private final Cache<JobID, CompletableFuture<CheckpointStatsSnapshot>>
            checkpointStatsSnapshotCache;

    protected AbstractCheckpointStatsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            MessageHeaders<EmptyRequestBody, R, M> messageHeaders,
            Cache<JobID, CompletableFuture<CheckpointStatsSnapshot>> checkpointStatsSnapshotCache,
            Executor executor) {
        super(leaderRetriever, timeout, responseHeaders, messageHeaders);
        this.executor = executor;
        this.checkpointStatsSnapshotCache = checkpointStatsSnapshotCache;
    }

    @Override
    protected CompletableFuture<R> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        JobID jobId = request.getPathParameter(JobIDPathParameter.class);

        try {
            return checkpointStatsSnapshotCache
                    .get(jobId, () -> gateway.requestCheckpointStats(jobId, timeout))
                    .thenApplyAsync(
                            checkpointStatsSnapshot -> {
                                try {
                                    return handleCheckpointStatsRequest(
                                            request, checkpointStatsSnapshot);
                                } catch (RestHandlerException e) {
                                    throw new CompletionException(e);
                                }
                            },
                            executor)
                    .exceptionally(
                            throwable -> {
                                throwable = ExceptionUtils.stripCompletionException(throwable);
                                if (throwable instanceof FlinkJobNotFoundException) {
                                    throw new CompletionException(
                                            new NotFoundException(
                                                    String.format("Job %s not found", jobId),
                                                    throwable));
                                } else {
                                    throw new CompletionException(throwable);
                                }
                            });
        } catch (ExecutionException e) {
            CompletableFuture<R> future = new CompletableFuture<>();
            future.completeExceptionally(e);
            return future;
        }
    }

    protected abstract R handleCheckpointStatsRequest(
            HandlerRequest<EmptyRequestBody> request,
            CheckpointStatsSnapshot checkpointStatsSnapshot)
            throws RestHandlerException;
}
