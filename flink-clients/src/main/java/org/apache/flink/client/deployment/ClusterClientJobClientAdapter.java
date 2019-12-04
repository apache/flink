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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.FunctionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link JobClient} interface that uses a {@link ClusterClient} underneath..
 */
public class ClusterClientJobClientAdapter<ClusterID> implements JobClient {

	private final ClusterClient<ClusterID> clusterClient;

	private final JobID jobID;

	private final AtomicBoolean running = new AtomicBoolean(true);

	public ClusterClientJobClientAdapter(final ClusterClient<ClusterID> clusterClient, final JobID jobID) {
		this.jobID = checkNotNull(jobID);
		this.clusterClient = checkNotNull(clusterClient);
	}

	@Override
	public JobID getJobID() {
		return jobID;
	}

	@Override
	public JobClient duplicate() throws Exception {
		return new ClusterClientJobClientAdapter<>(clusterClient.duplicate(), jobID);
	}

	@Override
	public CompletableFuture<JobStatus> getJobStatus() {
		return clusterClient.getJobStatus(jobID);
	}

	@Override
	public CompletableFuture<Void> cancel() {
		return clusterClient.cancel(jobID).thenApply(FunctionUtils.nullFn());
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(boolean advanceToEndOfEventTime, @Nullable String savepointDirectory) {
		return clusterClient.stopWithSavepoint(jobID, advanceToEndOfEventTime, savepointDirectory);
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(@Nullable String savepointDirectory) {
		return clusterClient.triggerSavepoint(jobID, savepointDirectory);
	}

	@Override
	public CompletableFuture<Map<String, Object>> getAccumulators(ClassLoader classLoader) {
		return clusterClient.getAccumulators(jobID, classLoader);
	}

	@Override
	public CompletableFuture<JobExecutionResult> getJobExecutionResult(final ClassLoader userClassloader) {
		checkNotNull(userClassloader);

		final CompletableFuture<JobResult> jobResultFuture = clusterClient.requestJobResult(jobID);
		return jobResultFuture.handle((jobResult, throwable) -> {
			if (throwable != null) {
				ExceptionUtils.checkInterrupted(throwable);
				throw new CompletionException(new ProgramInvocationException("Could not run job", jobID, throwable));
			} else {
				try {
					return jobResult.toJobExecutionResult(userClassloader);
				} catch (JobExecutionException | IOException | ClassNotFoundException e) {
					throw new CompletionException(new ProgramInvocationException("Job failed", jobID, e));
				}
			}
		});
	}

	@Override
	public final void close() {
		if (running.compareAndSet(true, false)) {
			doClose();
		}
	}

	/**
	 * Method to be overridden by subclass which contains actual close actions.
	 *
	 * <p>We do close in this way to ensure multiple calls to {@link #close()}
	 * are executed at most once guarded by {@link #running} flag.
	 */
	protected void doClose() {
		clusterClient.close();
	}
}
