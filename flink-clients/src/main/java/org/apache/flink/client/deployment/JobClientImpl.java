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
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.DetachedJobExecutionResult;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link JobClient} interface.
 */
public class JobClientImpl<ClusterID> implements JobClient {

	private final ClusterClient<ClusterID> clusterClient;

	private final JobID jobID;

	public JobClientImpl(final ClusterClient<ClusterID> clusterClient, final JobID jobID) {
		this.jobID = checkNotNull(jobID);
		this.clusterClient = checkNotNull(clusterClient);
	}

	@Override
	public JobID getJobID() {
		return jobID;
	}

	@Override
	public CompletableFuture<JobExecutionResult> getJobSubmissionResult() {
		return CompletableFuture.completedFuture(new DetachedJobExecutionResult(jobID));
	}

	@Override
	public CompletableFuture<JobExecutionResult> getJobExecutionResult(@Nonnull final ClassLoader userClassloader) {
		final CompletableFuture<JobExecutionResult> res = new CompletableFuture<>();

		final CompletableFuture<JobResult> jobResultFuture = clusterClient.requestJobResult(jobID);
		jobResultFuture.whenComplete(((jobResult, throwable) -> {
			if (throwable != null) {
				ExceptionUtils.checkInterrupted(throwable);
				res.completeExceptionally(new ProgramInvocationException("Could not run job", jobID, throwable));
			} else {
				try {
					final JobExecutionResult result = jobResult.toJobExecutionResult(userClassloader);
					res.complete(result);
				} catch (JobExecutionException | IOException | ClassNotFoundException e) {
					res.completeExceptionally(new ProgramInvocationException("Job failed", jobID, e));
				}
			}
		}));
		return res;
	}

	@Override
	public void close() throws Exception {
		this.clusterClient.close();
	}
}
