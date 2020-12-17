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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.CompletableFuture;

/**
 * Testing implementation of the {@link JobManagerRunner}.
 */
public class TestingJobManagerRunner implements JobManagerRunner {

	private final JobID jobId;

	private final boolean blockingTermination;

	private final CompletableFuture<Void> terminationFuture;

	private final CompletableFuture<JobMasterGateway> jobMasterGatewayFuture;

	private final CompletableFuture<JobManagerRunnerResult> resultFuture;

	private final OneShotLatch closeAsyncCalledLatch = new OneShotLatch();

	private TestingJobManagerRunner(JobID jobId,
			boolean blockingTermination,
			CompletableFuture<JobMasterGateway> jobMasterGatewayFuture,
			CompletableFuture<JobManagerRunnerResult> resultFuture) {
		this.jobId = jobId;
		this.blockingTermination = blockingTermination;
		this.jobMasterGatewayFuture = jobMasterGatewayFuture;
		this.resultFuture = resultFuture;
		this.terminationFuture = new CompletableFuture<>();

		terminationFuture.whenComplete((ignored, ignoredThrowable) -> resultFuture.completeExceptionally(new JobNotFinishedException(jobId)));
	}

	@Override
	public void start() throws Exception {}

	@Override
	public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
		return jobMasterGatewayFuture;
	}

	@Override
	public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
		return resultFuture;
	}

	@Override
	public JobID getJobID() {
		return jobId;
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		if (!blockingTermination) {
			terminationFuture.complete(null);
		}

		closeAsyncCalledLatch.trigger();
		return terminationFuture;
	}

	public OneShotLatch getCloseAsyncCalledLatch() {
		return closeAsyncCalledLatch;
	}

	public void completeResultFuture(ArchivedExecutionGraph archivedExecutionGraph) {
		resultFuture.complete(JobManagerRunnerResult.forSuccess(archivedExecutionGraph));
	}

	public void completeResultFutureExceptionally(Exception e) {
		resultFuture.completeExceptionally(e);
	}

	public void completeTerminationFuture() {
		terminationFuture.complete(null);
	}

	public CompletableFuture<Void> getTerminationFuture() {
		return terminationFuture;
	}

	public void completeJobMasterGatewayFuture(JobMasterGateway testingJobMasterGateway) {
		this.jobMasterGatewayFuture.complete(testingJobMasterGateway);
	}

	public static class Builder {
		private JobID jobId = null;
		private boolean blockingTermination = false;
		private CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = new CompletableFuture<>();
		private CompletableFuture<JobManagerRunnerResult> resultFuture = new CompletableFuture<>();

		public Builder setJobId(JobID jobId) {
			this.jobId = jobId;
			return this;
		}

		public Builder setBlockingTermination(boolean blockingTermination) {
			this.blockingTermination = blockingTermination;
			return this;
		}

		public Builder setJobMasterGatewayFuture(CompletableFuture<JobMasterGateway> jobMasterGatewayFuture) {
			Preconditions.checkNotNull(jobMasterGatewayFuture);
			this.jobMasterGatewayFuture = jobMasterGatewayFuture;
			return this;
		}

		public Builder setResultFuture(CompletableFuture<JobManagerRunnerResult> resultFuture) {
			Preconditions.checkNotNull(resultFuture);
			this.resultFuture = resultFuture;
			return this;
		}

		public TestingJobManagerRunner build() {
			Preconditions.checkNotNull(jobId);
			return new TestingJobManagerRunner(
				jobId,
				blockingTermination,
				jobMasterGatewayFuture,
				resultFuture);
		}
	}

}
