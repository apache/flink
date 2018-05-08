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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.FlinkException;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Mini Dispatcher which is instantiated as the dispatcher component by the {@link JobClusterEntrypoint}.
 *
 * <p>The mini dispatcher is initialized with a single {@link JobGraph} which it runs.
 *
 * <p>Depending on the {@link ClusterEntrypoint.ExecutionMode}, the mini dispatcher will directly
 * terminate after job completion if its execution mode is {@link ClusterEntrypoint.ExecutionMode#DETACHED}.
 */
public class MiniDispatcher extends Dispatcher {

	private final JobClusterEntrypoint.ExecutionMode executionMode;

	private final CompletableFuture<ApplicationStatus> jobTerminationFuture;

	public MiniDispatcher(
			RpcService rpcService,
			String endpointId,
			Configuration configuration,
			HighAvailabilityServices highAvailabilityServices,
			ResourceManagerGateway resourceManagerGateway,
			BlobServer blobServer,
			HeartbeatServices heartbeatServices,
			JobManagerMetricGroup jobManagerMetricGroup,
			@Nullable String metricQueryServicePath,
			ArchivedExecutionGraphStore archivedExecutionGraphStore,
			JobManagerRunnerFactory jobManagerRunnerFactory,
			FatalErrorHandler fatalErrorHandler,
			@Nullable String restAddress,
			JobGraph jobGraph,
			JobClusterEntrypoint.ExecutionMode executionMode) throws Exception {
		super(
			rpcService,
			endpointId,
			configuration,
			highAvailabilityServices,
			new SingleJobSubmittedJobGraphStore(jobGraph),
			resourceManagerGateway,
			blobServer,
			heartbeatServices,
			jobManagerMetricGroup,
			metricQueryServicePath,
			archivedExecutionGraphStore,
			jobManagerRunnerFactory,
			fatalErrorHandler,
			restAddress);

		this.executionMode = checkNotNull(executionMode);
		this.jobTerminationFuture = new CompletableFuture<>();
	}

	public CompletableFuture<ApplicationStatus> getJobTerminationFuture() {
		return jobTerminationFuture;
	}

	@Override
	public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
		final CompletableFuture<Acknowledge> acknowledgeCompletableFuture = super.submitJob(jobGraph, timeout);

		acknowledgeCompletableFuture.whenComplete(
			(Acknowledge ignored, Throwable throwable) -> {
				if (throwable != null) {
					onFatalError(new FlinkException(
						"Failed to submit job " + jobGraph.getJobID() + " in job mode.",
						throwable));
				}
			});

		return acknowledgeCompletableFuture;
	}

	@Override
	public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
		final CompletableFuture<JobResult> jobResultFuture = super.requestJobResult(jobId, timeout);

		if (executionMode == ClusterEntrypoint.ExecutionMode.NORMAL) {
			// terminate the MiniDispatcher once we served the first JobResult successfully
			jobResultFuture.thenAccept((JobResult result) -> {
				ApplicationStatus status = result.getSerializedThrowable().isPresent() ?
						ApplicationStatus.FAILED : ApplicationStatus.SUCCEEDED;

				jobTerminationFuture.complete(status);
			});
		}

		return jobResultFuture;
	}

	@Override
	protected void jobReachedGloballyTerminalState(ArchivedExecutionGraph archivedExecutionGraph) {
		super.jobReachedGloballyTerminalState(archivedExecutionGraph);

		if (executionMode == ClusterEntrypoint.ExecutionMode.DETACHED) {
			// shut down since we don't have to wait for the execution result retrieval
			jobTerminationFuture.complete(ApplicationStatus.fromJobStatus(archivedExecutionGraph.getState()));
		}
	}

	@Override
	protected void jobNotFinished(JobID jobId) {
		super.jobNotFinished(jobId);

		// shut down since we have done our job
		jobTerminationFuture.complete(ApplicationStatus.UNKNOWN);
	}
}
