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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.RescalingBehaviour;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.runtime.rest.handler.legacy.backpressure.OperatorBackPressureStatsResponse;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Gateway for restful endpoints.
 *
 * <p>Gateways which implement this method run a REST endpoint which is reachable
 * under the returned address.
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
	 * Stop the given job.
	 *
	 * @param jobId identifying the job to stop
	 * @param timeout of the operation
	 * @return A future acknowledge if the stopping succeeded
	 */
	CompletableFuture<Acknowledge> stopJob(JobID jobId, @RpcTimeout Time timeout);

	/**
	 * Requests the REST address of this {@link RpcEndpoint}.
	 *
	 * @param timeout for this operation
	 * @return Future REST endpoint address
	 */
	CompletableFuture<String> requestRestAddress(@RpcTimeout  Time timeout);

	/**
	 * Requests the {@link AccessExecutionGraph} for the given jobId. If there is no such graph, then
	 * the future is completed with a {@link FlinkJobNotFoundException}.
	 *
	 * @param jobId identifying the job whose AccessExecutionGraph is requested
	 * @param timeout for the asynchronous operation
	 * @return Future containing the AccessExecutionGraph for the given jobId, otherwise {@link FlinkJobNotFoundException}
	 */
	CompletableFuture<? extends AccessExecutionGraph> requestJob(JobID jobId, @RpcTimeout Time timeout);

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
	CompletableFuture<MultipleJobsDetails> requestMultipleJobDetails(
		@RpcTimeout Time timeout);

	/**
	 * Requests the cluster status overview.
	 *
	 * @param timeout for the asynchronous operation
	 * @return Future containing the status overview
	 */
	CompletableFuture<ClusterOverview> requestClusterOverview(@RpcTimeout Time timeout);

	/**
	 * Requests the paths for the {@link MetricQueryService} to query.
	 *
	 * @param timeout for the asynchronous operation
	 * @return Future containing the collection of metric query service paths to query
	 */
	CompletableFuture<Collection<String>> requestMetricQueryServicePaths(@RpcTimeout Time timeout);

	/**
	 * Requests the paths for the TaskManager's {@link MetricQueryService} to query.
	 *
	 * @param timeout for the asynchronous operation
	 * @return Future containing the collection of instance ids and the corresponding metric query service path
	 */
	CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServicePaths(@RpcTimeout Time timeout);

	/**
	 * Triggers a savepoint with the given savepoint directory as a target.
	 *
	 * @param jobId           ID of the job for which the savepoint should be triggered.
	 * @param targetDirectory Target directory for the savepoint.
	 * @param timeout         Timeout for the asynchronous operation
	 * @return A future to the {@link CompletedCheckpoint#getExternalPointer() external pointer} of
	 * the savepoint.
	 */
	default CompletableFuture<String> triggerSavepoint(
			JobID jobId,
			String targetDirectory,
			boolean cancelJob,
			@RpcTimeout Time timeout) {
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
			final String savepointPath,
			@RpcTimeout final Time timeout) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Request the {@link JobStatus} of the given job.
	 *
	 * @param jobId identifying the job for which to retrieve the JobStatus
	 * @param timeout for the asynchronous operation
	 * @return A future to the {@link JobStatus} of the given job
	 */
	default CompletableFuture<JobStatus> requestJobStatus(
			JobID jobId,
			@RpcTimeout Time timeout) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Requests the statistics on operator back pressure.
	 *
	 * @param jobId       Job for which the stats are requested.
	 * @param jobVertexId JobVertex for which the stats are requested.
	 * @return A Future to the {@link OperatorBackPressureStatsResponse} or {@code null} if the stats are
	 * not available (yet).
	 */
	default CompletableFuture<OperatorBackPressureStatsResponse> requestOperatorBackPressureStats(
			JobID jobId,
			JobVertexID jobVertexId) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Trigger rescaling of the given job.
	 *
	 * @param jobId specifying the job to rescale
	 * @param newParallelism new parallelism of the job
	 * @param rescalingBehaviour defining how strict the rescaling has to be executed
	 * @param timeout of this operation
	 * @return Future which is completed with {@link Acknowledge} once the rescaling was successful
	 */
	default CompletableFuture<Acknowledge> rescaleJob(
			JobID jobId,
			int newParallelism,
			RescalingBehaviour rescalingBehaviour,
			@RpcTimeout Time timeout) {
		throw new UnsupportedOperationException();
	}

	default CompletableFuture<Acknowledge> shutDownCluster() {
		throw new UnsupportedOperationException();
	}
}
