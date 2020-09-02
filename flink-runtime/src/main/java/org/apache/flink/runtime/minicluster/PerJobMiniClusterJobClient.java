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
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestGateway;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.util.SerializedValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/**
 * A {@link JobClient} for a {@link MiniCluster}.
 */
public final class PerJobMiniClusterJobClient implements JobClient, CoordinationRequestGateway {

	private static final Logger LOG = LoggerFactory.getLogger(PerJobMiniClusterJobClient.class);

	private final JobID jobID;
	private final MiniCluster miniCluster;
	private final CompletableFuture<JobResult> jobResultFuture;
	private final ClassLoader classLoader;

	public PerJobMiniClusterJobClient(JobID jobID, MiniCluster miniCluster, ClassLoader classLoader) {
		this.jobID = jobID;
		this.miniCluster = miniCluster;
		this.jobResultFuture = miniCluster
				.requestJobResult(jobID)
				// Make sure to shutdown the cluster when the job completes.
				.whenComplete((result, throwable) -> shutDownCluster(miniCluster));
		this.classLoader = classLoader;
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
		return miniCluster.cancelJob(jobID).thenAccept(result -> {
		});
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(
			boolean advanceToEndOfEventTime,
			@Nullable String savepointDirectory) {
		return miniCluster.stopWithSavepoint(jobID, savepointDirectory, advanceToEndOfEventTime);
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(@Nullable String savepointDirectory) {
		return miniCluster.triggerSavepoint(jobID, savepointDirectory, false);
	}

	@Override
	public CompletableFuture<Map<String, Object>> getAccumulators() {
		return getJobExecutionResult().thenApply(JobExecutionResult::getAllAccumulatorResults);
	}

	@Override
	public CompletableFuture<JobExecutionResult> getJobExecutionResult() {
		return jobResultFuture.thenApply(result -> {
			try {
				return result.toJobExecutionResult(classLoader);
			} catch (Exception e) {
				throw new CompletionException(
						"Failed to convert JobResult to JobExecutionResult.",
						e);
			}
		});
	}

	@Override
	public CompletableFuture<CoordinationResponse> sendCoordinationRequest(
			OperatorID operatorId,
			CoordinationRequest request) {
		try {
			SerializedValue<CoordinationRequest> serializedRequest = new SerializedValue<>(request);
			return miniCluster.deliverCoordinationRequestToCoordinator(
					jobID,
					operatorId,
					serializedRequest);
		} catch (IOException e) {
			return FutureUtils.completedExceptionally(e);
		}
	}

	private static void shutDownCluster(MiniCluster miniCluster) {
		miniCluster.closeAsync()
				.whenComplete((ignored, throwable) -> {
					if (throwable != null) {
						LOG.warn("Shutdown of MiniCluster failed.", throwable);
					}
				});
	}
}
