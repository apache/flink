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

package org.apache.flink.runtime.webmonitor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.SerializedValue;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Gateway for restful endpoints.
 *
 * <p>Gateways which implement this method run a REST endpoint which is reachable under the returned
 * address.
 */
public interface RestfulGateway extends RpcGateway {

    /**
     * Cancel the given job.
     *
     * @param jobId identifying the job to cancel
     * @param timeout of the operation
     * @return A future acknowledge if the cancellation succeeded
     */
    CompletableFuture<Acknowledge> cancelJob(JobID jobId, @RpcTimeout Time timeout);

    /**
     * Requests the {@link ArchivedExecutionGraph} for the given jobId. If there is no such graph,
     * then the future is completed with a {@link FlinkJobNotFoundException}.
     *
     * @param jobId identifying the job whose {@link ArchivedExecutionGraph} is requested
     * @param timeout for the asynchronous operation
     * @return Future containing the {@link ArchivedExecutionGraph} for the given jobId, otherwise
     *     {@link FlinkJobNotFoundException}
     */
    default CompletableFuture<ArchivedExecutionGraph> requestJob(
            JobID jobId, @RpcTimeout Time timeout) {
        return requestExecutionGraphInfo(jobId, timeout)
                .thenApply(ExecutionGraphInfo::getArchivedExecutionGraph);
    }

    /**
     * Requests the {@link ExecutionGraphInfo} containing additional information besides the {@link
     * ArchivedExecutionGraph}. If there is no such graph, then the future is completed with a
     * {@link FlinkJobNotFoundException}.
     *
     * @param jobId identifying the job whose {@link ExecutionGraphInfo} is requested
     * @param timeout for the asynchronous operation
     * @return Future containing the {@link ExecutionGraphInfo} for the given jobId, otherwise
     *     {@link FlinkJobNotFoundException}
     */
    CompletableFuture<ExecutionGraphInfo> requestExecutionGraphInfo(
            JobID jobId, @RpcTimeout Time timeout);

    /**
     * Requests the {@link JobResult} of a job specified by the given jobId.
     *
     * @param jobId identifying the job for which to retrieve the {@link JobResult}.
     * @param timeout for the asynchronous operation
     * @return Future which is completed with the job's {@link JobResult} once the job has finished
     */
    CompletableFuture<JobResult> requestJobResult(JobID jobId, @RpcTimeout Time timeout);

    /**
     * Requests job details currently being executed on the Flink cluster.
     *
     * @param timeout for the asynchronous operation
     * @return Future containing the job details
     */
    CompletableFuture<MultipleJobsDetails> requestMultipleJobDetails(@RpcTimeout Time timeout);

    /**
     * Requests the cluster status overview.
     *
     * @param timeout for the asynchronous operation
     * @return Future containing the status overview
     */
    CompletableFuture<ClusterOverview> requestClusterOverview(@RpcTimeout Time timeout);

    /**
     * Requests the addresses of the {@link MetricQueryService} to query.
     *
     * @param timeout for the asynchronous operation
     * @return Future containing the collection of metric query service addresses to query
     */
    CompletableFuture<Collection<String>> requestMetricQueryServiceAddresses(
            @RpcTimeout Time timeout);

    /**
     * Requests the addresses for the TaskManagers' {@link MetricQueryService} to query.
     *
     * @param timeout for the asynchronous operation
     * @return Future containing the collection of instance ids and the corresponding metric query
     *     service address
     */
    CompletableFuture<Collection<Tuple2<ResourceID, String>>>
            requestTaskManagerMetricQueryServiceAddresses(@RpcTimeout Time timeout);

    /**
     * Triggers a savepoint with the given savepoint directory as a target.
     *
     * @param jobId ID of the job for which the savepoint should be triggered.
     * @param targetDirectory Target directory for the savepoint.
     * @param timeout Timeout for the asynchronous operation
     * @return A future to the {@link CompletedCheckpoint#getExternalPointer() external pointer} of
     *     the savepoint.
     */
    default CompletableFuture<String> triggerSavepoint(
            JobID jobId, String targetDirectory, boolean cancelJob, @RpcTimeout Time timeout) {
        throw new UnsupportedOperationException();
    }

    /**
     * Stops the job with a savepoint.
     *
     * @param jobId ID of the job for which the savepoint should be triggered.
     * @param targetDirectory to which to write the savepoint data or null if the default savepoint
     *     directory should be used
     * @param terminate flag indicating if the job should terminate or just suspend
     * @param timeout for the rpc call
     * @return Future which is completed with the savepoint path once completed
     */
    default CompletableFuture<String> stopWithSavepoint(
            final JobID jobId,
            final String targetDirectory,
            final boolean terminate,
            @RpcTimeout final Time timeout) {
        throw new UnsupportedOperationException();
    }

    /**
     * Dispose the given savepoint.
     *
     * @param savepointPath identifying the savepoint to dispose
     * @param timeout RPC timeout
     * @return A future acknowledge if the disposal succeeded
     */
    default CompletableFuture<Acknowledge> disposeSavepoint(
            final String savepointPath, @RpcTimeout final Time timeout) {
        throw new UnsupportedOperationException();
    }

    /**
     * Request the {@link JobStatus} of the given job.
     *
     * @param jobId identifying the job for which to retrieve the JobStatus
     * @param timeout for the asynchronous operation
     * @return A future to the {@link JobStatus} of the given job
     */
    default CompletableFuture<JobStatus> requestJobStatus(JobID jobId, @RpcTimeout Time timeout) {
        throw new UnsupportedOperationException();
    }

    default CompletableFuture<Acknowledge> shutDownCluster() {
        throw new UnsupportedOperationException();
    }

    /**
     * Deliver a coordination request to a specified coordinator and return the response.
     *
     * @param jobId identifying the job which the coordinator belongs to
     * @param operatorId identifying the coordinator to receive the request
     * @param serializedRequest serialized request to deliver
     * @param timeout RPC timeout
     * @return A future containing the response. The response will fail with a {@link
     *     org.apache.flink.util.FlinkException} if the task is not running, or no
     *     operator/coordinator exists for the given ID, or the coordinator cannot handle client
     *     events.
     */
    default CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            JobID jobId,
            OperatorID operatorId,
            SerializedValue<CoordinationRequest> serializedRequest,
            @RpcTimeout Time timeout) {
        throw new UnsupportedOperationException();
    }
}
