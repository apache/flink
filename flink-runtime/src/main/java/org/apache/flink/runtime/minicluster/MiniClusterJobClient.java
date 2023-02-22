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

package org.apache.flink.runtime.minicluster;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** A {@link JobClient} for a {@link MiniCluster}. */
public final class MiniClusterJobClient implements JobClient, CoordinationRequestGateway {

    private static final Logger LOG = LoggerFactory.getLogger(MiniClusterJobClient.class);

    private final JobID jobID;
    private final MiniCluster miniCluster;
    private final ClassLoader classLoader;
    private final CompletableFuture<JobResult> jobResultFuture;

    /**
     * Creates a {@link MiniClusterJobClient} for the given {@link JobID} and {@link MiniCluster}.
     * This will shut down the {@code MiniCluster} after job result retrieval if {@code
     * shutdownCluster} is {@code true}.
     */
    public MiniClusterJobClient(
            JobID jobID,
            MiniCluster miniCluster,
            ClassLoader classLoader,
            JobFinalizationBehavior finalizationBehaviour) {
        this.jobID = jobID;
        this.miniCluster = miniCluster;
        this.classLoader = classLoader;

        if (finalizationBehaviour == JobFinalizationBehavior.SHUTDOWN_CLUSTER) {
            // Make sure to shutdown the cluster when the job completes.
            jobResultFuture =
                    miniCluster
                            .requestJobResult(jobID)
                            .whenComplete((result, throwable) -> shutDownCluster(miniCluster));
        } else if (finalizationBehaviour == JobFinalizationBehavior.NOTHING) {
            jobResultFuture = miniCluster.requestJobResult(jobID);
        } else {
            throw new IllegalArgumentException(
                    "Unexpected shutdown behavior: " + finalizationBehaviour);
        }
    }

    @Override
    public JobID getJobID() {
        return jobID;
    }

    @Override
    public CompletableFuture<JobStatus> getJobStatus() {
        return miniCluster.getJobStatus(jobID);
    }

    @Override
    public CompletableFuture<Void> cancel() {
        return miniCluster.cancelJob(jobID).thenAccept(result -> {});
    }

    @Override
    public CompletableFuture<String> stopWithSavepoint(
            boolean terminate,
            @Nullable String savepointDirectory,
            SavepointFormatType formatType) {
        return miniCluster.stopWithSavepoint(jobID, savepointDirectory, terminate, formatType);
    }

    @Override
    public CompletableFuture<String> triggerSavepoint(
            @Nullable String savepointDirectory, SavepointFormatType formatType) {
        return miniCluster.triggerSavepoint(jobID, savepointDirectory, false, formatType);
    }

    @Override
    public CompletableFuture<Map<String, Object>> getAccumulators() {
        final CompletableFuture<JobExecutionResult> jobExecutionResult = getJobExecutionResult();
        if (jobExecutionResult.isDone()) {
            return jobExecutionResult.thenApply(JobExecutionResult::getAllAccumulatorResults);
        } else {
            return miniCluster
                    .getExecutionGraph(jobID)
                    .thenApply(AccessExecutionGraph::getAccumulatorsSerialized)
                    .thenApply(
                            accumulators -> {
                                try {
                                    return AccumulatorHelper.deserializeAndUnwrapAccumulators(
                                            accumulators, classLoader);
                                } catch (Exception e) {
                                    throw new CompletionException(
                                            "Cannot deserialize and unwrap accumulators properly.",
                                            e);
                                }
                            });
        }
    }

    @Override
    public CompletableFuture<JobExecutionResult> getJobExecutionResult() {
        return jobResultFuture.thenApply(
                result -> {
                    try {
                        return result.toJobExecutionResult(classLoader);
                    } catch (Exception e) {
                        throw new CompletionException(
                                "Failed to convert JobResult to JobExecutionResult.", e);
                    }
                });
    }

    @Override
    public CompletableFuture<CoordinationResponse> sendCoordinationRequest(
            OperatorID operatorId, CoordinationRequest request) {
        try {
            SerializedValue<CoordinationRequest> serializedRequest = new SerializedValue<>(request);
            return miniCluster.deliverCoordinationRequestToCoordinator(
                    jobID, operatorId, serializedRequest);
        } catch (IOException e) {
            return FutureUtils.completedExceptionally(e);
        }
    }

    @Override
    public void reportHeartbeat(long expiredTimestamp) {
        miniCluster.reportHeartbeat(jobID, expiredTimestamp);
    }

    private static void shutDownCluster(MiniCluster miniCluster) {
        miniCluster
                .closeAsync()
                .whenComplete(
                        (ignored, throwable) -> {
                            if (throwable != null) {
                                LOG.warn("Shutdown of MiniCluster failed.", throwable);
                            }
                        });
    }

    /** Determines the behavior of the {@link MiniClusterJobClient} when the job finishes. */
    public enum JobFinalizationBehavior {
        /** Shut down the {@link MiniCluster} when the job finishes. */
        SHUTDOWN_CLUSTER,

        /** Don't do anything when the job finishes. */
        NOTHING
    }
}
