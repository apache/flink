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
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Client to interact with a {@link MiniCluster}.
 */
public class MiniClusterClient extends ClusterClient<MiniClusterClient.MiniClusterId> {

	private final MiniCluster miniCluster;
	private final Configuration configuration;

	public MiniClusterClient(@Nonnull Configuration configuration, @Nonnull MiniCluster miniCluster) {
		this.configuration = configuration;
		this.miniCluster = miniCluster;
	}

	@Override
	public Configuration getFlinkConfiguration() {
		return new Configuration(configuration);
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

	@Override
	public String getWebInterfaceURL() {
		return miniCluster.getRestAddress().toString();
	}

	enum MiniClusterId {
		INSTANCE
	}
}
