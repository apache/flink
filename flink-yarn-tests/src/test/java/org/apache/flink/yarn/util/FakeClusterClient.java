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

package org.apache.flink.yarn.util;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.OptionalFailure;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Dummy {@link ClusterClient} for testing purposes (extend as needed).
 */
public class FakeClusterClient extends ClusterClient<ApplicationId> {

	public FakeClusterClient(Configuration flinkConfig) throws Exception {
		super(flinkConfig);
	}

	@Override
	public String getWebInterfaceURL() {
		return "";
	}

	@Override
	public ApplicationId getClusterId() {
		throw new UnsupportedOperationException("Not needed in test.");
	}

	@Override
	public JobSubmissionResult submitJob(JobGraph jobGraph, ClassLoader classLoader) {
		throw new UnsupportedOperationException("Not needed in test.");
	}

	public CompletableFuture<JobStatus> getJobStatus(JobID jobId) {
		throw new UnsupportedOperationException("Not needed in test.");
	}

	public void cancel(JobID jobId) {
		// no op
	}

	public String cancelWithSavepoint(JobID jobId, @Nullable String savepointDirectory) {
		throw new UnsupportedOperationException("Not needed in test.");
	}

	@Override
	public String stopWithSavepoint(JobID jobId, boolean advanceToEndOfEventTime, @Nullable String savepointDirectory) throws Exception {
		throw new UnsupportedOperationException("Not needed in test.");
	}

	public void stop(final JobID jobId) {
		// no op
	}

	public CompletableFuture<String> triggerSavepoint(JobID jobId, @Nullable String savepointDirectory) {
		throw new UnsupportedOperationException("Not needed in test.");
	}

	public CompletableFuture<Acknowledge> disposeSavepoint(String savepointPath) {
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	public CompletableFuture<Collection<JobStatusMessage>> listJobs() {
		return CompletableFuture.completedFuture(Collections.emptyList());
	}

	public Map<String, OptionalFailure<Object>> getAccumulators(JobID jobID, ClassLoader loader) {
		return Collections.emptyMap();
	}
}
