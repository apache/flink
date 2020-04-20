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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link JobClient} that wraps any other job client and transforms it into one that is not allowed
 * to wait for the job result.
 *
 * <p>This is used in web submission, where we do not want the Web UI to have jobs blocking threads while
 * waiting for their completion.
 */
@Internal
public class DetachedOnlyJobClientAdapter implements JobClient {

	private final JobClient jobClient;

	public DetachedOnlyJobClientAdapter(final JobClient jobClient) {
		this.jobClient = checkNotNull(jobClient);
	}

	@Override
	public JobID getJobID() {
		return jobClient.getJobID();
	}

	@Override
	public CompletableFuture<JobStatus> getJobStatus() {
		throw new FlinkRuntimeException("The Job Status cannot be requested when in Web Submission.");
	}

	@Override
	public CompletableFuture<Void> cancel() {
		throw new FlinkRuntimeException("Cancelling the job is not supported by the Job Client when in Web Submission.");
	}

	@Override
	public CompletableFuture<String> stopWithSavepoint(boolean advanceToEndOfEventTime, @Nullable String savepointDirectory) {
		throw new FlinkRuntimeException("Stop with Savepoint is not supported by the Job Client when in Web Submission.");
	}

	@Override
	public CompletableFuture<String> triggerSavepoint(@Nullable String savepointDirectory) {
		throw new FlinkRuntimeException("A savepoint cannot be taken through the Job Client when in Web Submission.");
	}

	@Override
	public CompletableFuture<Map<String, Object>> getAccumulators(ClassLoader classLoader) {
		throw new FlinkRuntimeException("The Accumulators cannot be fetched through the Job Client when in Web Submission.");
	}

	@Override
	public CompletableFuture<JobExecutionResult> getJobExecutionResult(ClassLoader userClassloader) {
		throw new FlinkRuntimeException("The Job Result cannot be fetched through the Job Client when in Web Submission.");
	}
}
