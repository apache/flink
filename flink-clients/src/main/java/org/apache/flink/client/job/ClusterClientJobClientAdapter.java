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

package org.apache.flink.client.job;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.OptionalFailure;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * ClusterClientJobClientAdapter.
 */
public class ClusterClientJobClientAdapter implements JobClient {

	private final JobID jobID;
	private final ClusterClient<?> client;
	private final boolean shared;

	public ClusterClientJobClientAdapter(JobID jobID, ClusterClient<?> client, boolean shared) {
		this.jobID = jobID;
		this.client = client;
		this.shared = shared;
	}

	@Override
	public JobID getJobID() {
		return jobID;
	}

	@Override
	public CompletableFuture<JobStatus> getJobStatus() {
		return client.getJobStatus(jobID);
	}

	@Override
	public CompletableFuture<JobResult> requestJobResult() {
		return client.requestJobResult(jobID);
	}

	@Override
	public CompletableFuture<Map<String, OptionalFailure<Object>>> getAccumulators(ClassLoader loader) {
		return client.getAccumulators(jobID, loader);
	}

	@Override
	public CompletableFuture<Acknowledge> cancel() {
		return client.cancel(jobID);
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(boolean advanceToEndOfEventTime, @Nullable String savepointDirectory) {
		return client.stopWithSavepoint(jobID, advanceToEndOfEventTime, savepointDirectory);
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(@Nullable String savepointDirectory) {
		return client.triggerSavepoint(jobID, savepointDirectory);
	}

	@Override
	public void close() throws Exception {
		if (!shared) {
			client.close();
		}
	}
}
