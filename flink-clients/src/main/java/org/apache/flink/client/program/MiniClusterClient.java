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

package org.apache.flink.client.program;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Client to interact with a {@link MiniCluster}.
 */
public class MiniClusterClient extends ClusterClient<MiniClusterClient.MiniClusterId> implements NewClusterClient {

	private final MiniCluster miniCluster;

	public MiniClusterClient(@Nonnull Configuration configuration, @Nonnull MiniCluster miniCluster) {
		super(configuration, miniCluster.getHighAvailabilityServices(), true);

		this.miniCluster = miniCluster;
	}

	@Override
	public void shutdown() throws Exception {
		super.shutdown();
	}

	@Override
	public JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader) throws ProgramInvocationException {
		final CompletableFuture<JobSubmissionResult> jobSubmissionResultFuture = submitJob(jobGraph);

		if (isDetached()) {
			try {
				return jobSubmissionResultFuture.get();
			} catch (InterruptedException | ExecutionException e) {
				ExceptionUtils.checkInterrupted(e);

				throw new ProgramInvocationException("Could not run job in detached mode.", jobGraph.getJobID(), e);
			}
		} else {
			final CompletableFuture<JobResult> jobResultFuture = jobSubmissionResultFuture.thenCompose(
				(JobSubmissionResult ignored) -> requestJobResult(jobGraph.getJobID()));

			final JobResult jobResult;
			try {
				jobResult = jobResultFuture.get();
			} catch (InterruptedException | ExecutionException e) {
				ExceptionUtils.checkInterrupted(e);

				throw new ProgramInvocationException("Could not run job", jobGraph.getJobID(), e);
			}

			try {
				return jobResult.toJobExecutionResult(classLoader);
			} catch (JobExecutionException e) {
				throw new ProgramInvocationException("Job failed", jobGraph.getJobID(), e);
			} catch (IOException | ClassNotFoundException e) {
				throw new ProgramInvocationException("Job failed", jobGraph.getJobID(), e);
			}
		}
	}

	@Override
	public CompletableFuture<JobSubmissionResult> submitJob(@Nonnull JobGraph jobGraph) {
		return miniCluster.submitJob(jobGraph);
	}

	@Override
	public CompletableFuture<JobResult> requestJobResult(@Nonnull JobID jobId) {
		return miniCluster.requestJobResult(jobId);
	}

	@Override
	public void cancel(JobID jobId) throws Exception {
		miniCluster.cancelJob(jobId).get();
	}

	@Override
	public String cancelWithSavepoint(JobID jobId, @Nullable String savepointDirectory) throws Exception {
		return miniCluster.triggerSavepoint(jobId, savepointDirectory, true).get();
	}

	@Override
	public String stopWithSavepoint(JobID jobId, boolean advanceToEndOfEventTime, @Nullable String savepointDirector) throws Exception {
		return miniCluster.stopWithSavepoint(jobId, savepointDirector, advanceToEndOfEventTime).get();
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(JobID jobId, @Nullable String savepointDirectory) {
		return miniCluster.triggerSavepoint(jobId, savepointDirectory, false);
	}

	@Override
	public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath) {
		return miniCluster.disposeSavepoint(savepointPath);
	}

	@Override
	public CompletableFuture<Collection<JobStatusMessage>> listJobs() {
		return miniCluster.listJobs();
	}

	@Override
	public Map<String, OptionalFailure<Object>> getAccumulators(JobID jobID, ClassLoader loader) throws Exception {
		AccessExecutionGraph executionGraph = miniCluster.getExecutionGraph(jobID).get();
		Map<String, SerializedValue<OptionalFailure<Object>>> accumulatorsSerialized = executionGraph.getAccumulatorsSerialized();
		Map<String, OptionalFailure<Object>> result = new HashMap<>(accumulatorsSerialized.size());
		for (Map.Entry<String, SerializedValue<OptionalFailure<Object>>> acc : accumulatorsSerialized.entrySet()) {
			result.put(acc.getKey(), acc.getValue().deserializeValue(loader));
		}
		return result;
	}

	@Override
	public CompletableFuture<JobStatus> getJobStatus(JobID jobId) {
		return miniCluster.getJobStatus(jobId);
	}

	@Override
	public MiniClusterClient.MiniClusterId getClusterId() {
		return MiniClusterId.INSTANCE;
	}

	// ======================================
	// Legacy methods
	// ======================================

	@Override
	public String getWebInterfaceURL() {
		return miniCluster.getRestAddress().toString();
	}

	enum MiniClusterId {
		INSTANCE
	}
}
