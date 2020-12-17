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
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerRunnerResult;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobNotFinishedException;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.CompletableFuture;

/**
 * Abstraction used by the {@link Dispatcher} to manage jobs.
 */
public final class DispatcherJob implements AutoCloseableAsync {

	private final Logger log = LoggerFactory.getLogger(DispatcherJob.class);

	private final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture;
	private final CompletableFuture<DispatcherJobResult> jobResultFuture;
	private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

	private final long initializationTimestamp;
	private final JobID jobId;
	private final String jobName;

	private final Object lock = new Object();

	// internal field to track job status during initialization. Is not updated anymore after
	// job is initialized, cancelled or failed.
	@GuardedBy("lock")
	private DispatcherJobStatus jobStatus = DispatcherJobStatus.INITIALIZING;

	private enum DispatcherJobStatus {
		// We are waiting for the JobManagerRunner to be initialized
		INITIALIZING(JobStatus.INITIALIZING),
		// JobManagerRunner is initialized
		JOB_MANAGER_RUNNER_INITIALIZED(null),
		// waiting for cancellation. We stay in this status until the job result future completed,
		// then we consider the JobManager to be initialized.
		CANCELLING(JobStatus.CANCELLING);

		@Nullable
		private final JobStatus jobStatus;

		DispatcherJobStatus(JobStatus jobStatus) {
			this.jobStatus = jobStatus;
		}

		public JobStatus asJobStatus() {
			if (jobStatus == null) {
				throw new IllegalStateException("This state is not defined as a 'JobStatus'");
			}
			return jobStatus;
		}
	}

	static DispatcherJob createFor(
			CompletableFuture<JobManagerRunner> jobManagerRunnerFuture,
			JobID jobId,
			String jobName,
			long initializationTimestamp) {
		return new DispatcherJob(jobManagerRunnerFuture, jobId, jobName, initializationTimestamp);
	}

	private DispatcherJob(
			CompletableFuture<JobManagerRunner> jobManagerRunnerFuture,
			JobID jobId,
			String jobName,
			long initializationTimestamp) {
		this.jobManagerRunnerFuture = jobManagerRunnerFuture;
		this.jobId = jobId;
		this.jobName = jobName;
		this.initializationTimestamp = initializationTimestamp;
		this.jobResultFuture = new CompletableFuture<>();

		FutureUtils.assertNoException(this.jobManagerRunnerFuture.handle((jobManagerRunner, throwable) -> {
			// JM has been initialized, or the initialization failed
			synchronized (lock) {
				jobStatus = DispatcherJobStatus.JOB_MANAGER_RUNNER_INITIALIZED;
				if (throwable == null) { // initialization succeeded
					// Forward result future
					jobManagerRunner.getResultFuture().whenComplete((jobManagerRunnerResult, resultThrowable) -> {
						if (jobManagerRunnerResult != null) {
							handleJobManagerRunnerResult(jobManagerRunnerResult);
						} else {
							jobResultFuture.completeExceptionally(ExceptionUtils.stripCompletionException(resultThrowable));
						}
					});
				} else { // failure during initialization
					handleInitializationFailure(ExceptionUtils.stripCompletionException(throwable));
				}
			}
			return null;
		}));
	}

	private void handleJobManagerRunnerResult(JobManagerRunnerResult jobManagerRunnerResult) {
		if (jobManagerRunnerResult.isSuccess()) {
			jobResultFuture.complete(DispatcherJobResult.forSuccess(jobManagerRunnerResult.getArchivedExecutionGraph()));
		} else if (jobManagerRunnerResult.isJobNotFinished()) {
			jobResultFuture.completeExceptionally(new JobNotFinishedException(jobId));
		} else if (jobManagerRunnerResult.isInitializationFailure()) {
			handleInitializationFailure(jobManagerRunnerResult.getInitializationFailure());
		}
	}

	private void handleInitializationFailure(Throwable initializationFailure) {
		ArchivedExecutionGraph archivedExecutionGraph = ArchivedExecutionGraph.createFromInitializingJob(
			jobId,
			jobName,
			JobStatus.FAILED,
			initializationFailure,
			initializationTimestamp);
		jobResultFuture.complete(DispatcherJobResult.forInitializationFailure(archivedExecutionGraph,
			initializationFailure));
	}

	public CompletableFuture<DispatcherJobResult> getResultFuture() {
		return jobResultFuture;
	}

	public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
		return requestJob(timeout).thenApply(executionGraph -> {
			synchronized (lock) {
				return JobDetails.createDetailsForJob(executionGraph);
			}
		});
	}

	/**
	 * Cancel job.
	 * A cancellation will be scheduled if the initialization is not completed.
	 * The returned future will complete exceptionally if the JobManagerRunner initialization failed.
	 */
	public CompletableFuture<Acknowledge> cancel(Time timeout) {
		synchronized (lock) {
			if (isInitialized()) {
				return getJobMasterGateway().thenCompose(jobMasterGateway -> jobMasterGateway.cancel(timeout));
			} else {
				log.info("Cancellation during initialization requested for job {}. Job will be cancelled once JobManager has been initialized.", jobId);

				// cancel job
				CompletableFuture<Acknowledge> cancelFuture = jobManagerRunnerFuture
					.thenCompose(JobManagerRunner::getJobMasterGateway)
					.thenCompose(jobMasterGateway -> jobMasterGateway.cancel(timeout));
				cancelFuture.whenComplete((ignored, cancelThrowable) -> {
					if (cancelThrowable != null) {
						log.warn("Cancellation of job {} failed", jobId, cancelThrowable);
					}
				});
				jobStatus = DispatcherJobStatus.CANCELLING;
				return cancelFuture;
			}
		}
	}

	public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
		return requestJob(timeout).thenApply(ArchivedExecutionGraph::getState);
	}

	/**
	 * Returns a future completing to the ArchivedExecutionGraph of the job.
	 */
	public CompletableFuture<ArchivedExecutionGraph> requestJob(Time timeout) {
		synchronized (lock) {
			if (isInitialized()) {
				if (jobResultFuture.isDone()) { // job is not running anymore
					return jobResultFuture.thenApply(DispatcherJobResult::getArchivedExecutionGraph);
				}
				// job is still running
				return getJobMasterGateway().thenCompose(jobMasterGateway -> jobMasterGateway.requestJob(
					timeout));
			} else {
				Preconditions.checkState(this.jobStatus == DispatcherJobStatus.INITIALIZING || jobStatus == DispatcherJobStatus.CANCELLING);
				return CompletableFuture.completedFuture(
					ArchivedExecutionGraph.createFromInitializingJob(
						jobId,
						jobName,
						jobStatus.asJobStatus(),
						null,
						initializationTimestamp));
			}
		}
	}

	/**
	 * The job is initialized once the JobManager runner has been initialized.
	 * It is also initialized if the runner initialization failed, or of it has been
	 * canceled (and the cancellation is complete).
	 */
	public boolean isInitialized() {
		synchronized (lock) {
			return jobStatus == DispatcherJobStatus.JOB_MANAGER_RUNNER_INITIALIZED;
		}
	}

	/**
	 * Returns the {@link JobMasterGateway} from the JobManagerRunner.
	 *
	 * @return the {@link JobMasterGateway}. The future will complete exceptionally if the JobManagerRunner initialization failed.
	 * @throws IllegalStateException is thrown if the job is not initialized
	 */
	public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
		Preconditions.checkState(
			isInitialized(),
			"JobMaster Gateway is not available during initialization");
		return jobManagerRunnerFuture.thenCompose(JobManagerRunner::getJobMasterGateway);
	}

	@Override
	public CompletableFuture<Void> closeAsync() {
		FutureUtils.assertNoException(jobManagerRunnerFuture.handle((runner, throwable) -> {
			if (throwable == null) {
				// init was successful: close jobManager runner.
				CompletableFuture<Void> jobManagerRunnerClose = jobManagerRunnerFuture.thenCompose(
					AutoCloseableAsync::closeAsync);
				FutureUtils.forward(jobManagerRunnerClose, terminationFuture);
			} else {
				// initialization has failed: complete termination.
				terminationFuture.complete(null);
			}
			return null;
		}));
		return terminationFuture;
	}
}
