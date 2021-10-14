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

package org.apache.flink.client.deployment;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.commons.io.IOUtils;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link JobClient} interface that uses a {@link ClusterClient}
 * underneath..
 */
public class ClusterClientJobClientAdapter<ClusterID>
        implements JobClient, CoordinationRequestGateway {

    private final ClusterClientProvider<ClusterID> clusterClientProvider;

    private final JobID jobID;

    private final ClassLoader classLoader;

    public ClusterClientJobClientAdapter(
            final ClusterClientProvider<ClusterID> clusterClientProvider,
            final JobID jobID,
            final ClassLoader classLoader) {
        this.jobID = checkNotNull(jobID);
        this.clusterClientProvider = checkNotNull(clusterClientProvider);
        this.classLoader = classLoader;
    }

    @Override
    public JobID getJobID() {
        return jobID;
    }

    @Override
    public CompletableFuture<JobStatus> getJobStatus() {
        return bridgeClientRequest(
                clusterClientProvider, (clusterClient -> clusterClient.getJobStatus(jobID)));
    }

    @Override
    public CompletableFuture<Void> cancel() {
        return bridgeClientRequest(
                clusterClientProvider,
                (clusterClient -> clusterClient.cancel(jobID).thenApply((ignored) -> null)));
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            boolean advanceToEndOfEventTime, @Nullable String savepointDirectory) {
        return bridgeClientRequest(
                clusterClientProvider,
                (clusterClient ->
                        clusterClient.stopWithSavepoint(
                                jobID, advanceToEndOfEventTime, savepointDirectory)));
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(@Nullable String savepointDirectory) {
        return bridgeClientRequest(
                clusterClientProvider,
                (clusterClient -> clusterClient.triggerSavepoint(jobID, savepointDirectory)));
    }

    @Override
    public CompletableFuture<Map<String, Object>> getAccumulators() {
        checkNotNull(classLoader);

        return bridgeClientRequest(
                clusterClientProvider,
                (clusterClient -> clusterClient.getAccumulators(jobID, classLoader)));
    }

    @Override
    public CompletableFuture<JobExecutionResult> getJobExecutionResult() {
        checkNotNull(classLoader);

        return bridgeClientRequest(
                clusterClientProvider,
                (clusterClient ->
                        clusterClient
                                .requestJobResult(jobID)
                                .thenApply(
                                        (jobResult) -> {
                                            try {
                                                return jobResult.toJobExecutionResult(classLoader);
                                            } catch (Throwable t) {
                                                throw new CompletionException(
                                                        new ProgramInvocationException(
                                                                "Job failed", jobID, t));
                                            }
                                        })));
    }

    @Override
    public CompletableFuture<CoordinationResponse> sendCoordinationRequest(
            OperatorID operatorId, CoordinationRequest request) {
        return bridgeClientRequest(
                clusterClientProvider,
                clusterClient -> clusterClient.sendCoordinationRequest(jobID, operatorId, request));
    }

    private static <T> CompletableFuture<T> bridgeClientRequest(
            ClusterClientProvider<?> clusterClientProvider,
            Function<ClusterClient<?>, CompletableFuture<T>> resultRetriever) {

        ClusterClient<?> clusterClient = clusterClientProvider.getClusterClient();

        CompletableFuture<T> resultFuture;
        try {
            resultFuture = resultRetriever.apply(clusterClient);
        } catch (Throwable throwable) {
            IOUtils.closeQuietly(clusterClient::close);
            return FutureUtils.completedExceptionally(throwable);
        }

        return resultFuture.whenCompleteAsync(
                (jobResult, throwable) -> IOUtils.closeQuietly(clusterClient::close));
    }
}
