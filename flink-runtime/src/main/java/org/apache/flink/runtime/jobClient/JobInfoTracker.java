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

package org.apache.flink.runtime.jobClient;

import akka.dispatch.ExecutionContexts$;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.SerializedJobExecutionResult;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkFuture;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.messages.ExecutionGraphMessages;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.util.SerializedThrowable;
import org.apache.flink.runtime.client.JobExecutionException;
import scala.concurrent.Promise;
import akka.dispatch.Mapper;

import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This base receives and prints job updates until job completion, receive job result when job completes.
 */
public class JobInfoTracker extends RpcEndpoint<JobInfoTrackerGateway> {

	private final LeaderRetrievalService leaderRetrievalService;

	/** identifier of a job */
	private final JobID jobID;

	/** true if status messages shall be printed to sysout */
	private final boolean sysoutUpdates;

	/** Promise about job result which will be completed when receive job result from job master */
	private final Promise<JobManagerMessages.JobResultMessage> jobResultMessagePromise;

	/** the leader session id of job master which is responsible for the given job */
	private UUID jobMasterLeaderSessionID = null;

	/**
	 * Constructs a new jobInfoTracker
	 *
	 * @param rpcService             rpc service
	 * @param leaderRetrievalService leader retrieval service of jobMaster
	 * @param jobID                  identifier of job
	 * @param sysoutUpdates          whether status messages shall be printed to sysout
	 */
	public JobInfoTracker(RpcService rpcService, LeaderRetrievalService leaderRetrievalService, JobID jobID,
		boolean sysoutUpdates)
	{
		super(rpcService);
		this.leaderRetrievalService = checkNotNull(leaderRetrievalService);
		this.jobID = checkNotNull(jobID);
		this.sysoutUpdates = sysoutUpdates;
		this.jobResultMessagePromise = new scala.concurrent.impl.Promise.DefaultPromise<>();
	}

	@Override
	public void start() {
		super.start();
		try {
			leaderRetrievalService.start(new JobMasterLeaderListener());
		} catch (Throwable e) {
			log.error("A fatal error happened when start a new jobMasterListener at leaderRetriever of job {}", jobID);
			throw new RuntimeException("A fatal error happened when get jobMaster leaderRetriever ", e);
		}
	}

	@Override
	public void shutDown() {
		super.shutDown();
		if (!jobResultMessagePromise.isCompleted()) {
			jobResultMessagePromise.tryFailure(new RuntimeException("JobInfoTracker service stopped before receive the job result"));
		}
		try {
			leaderRetrievalService.stop();
		} catch (Throwable e) {
			log.error("A fatal error happened when stop the jobMaster leaderRetriever service", e);
			throw new RuntimeException("A fatal error happened when stop the jobMaster leaderRetriever service", e);
		}
	}

	/**
	 * Receives notification about execution state changed event from {@link org.apache.flink.runtime.jobmaster.JobMaster}
	 *
	 * @param jobMasterLeaderSessionID leaderSessionID of jobMaster which send this notification
	 * @param executionStateChanged    the execution state change message
	 */
	@RpcMethod
	public void notifyJobExecutionStateChanged(UUID jobMasterLeaderSessionID,
		ExecutionGraphMessages.ExecutionStateChanged executionStateChanged)
	{
		if (jobMasterLeaderSessionID.equals(this.jobMasterLeaderSessionID)) {
			logAndPrintMessage(executionStateChanged.toString());
		}
	}

	/**
	 * Receives notification about job status changed event from {@link org.apache.flink.runtime.jobmaster.JobMaster}
	 *
	 * @param jobMasterLeaderSessionID leaderSessionID of jobMaster which send this notifcation
	 * @param jobStatusChanged         the job state change message
	 */
	@RpcMethod
	public void notifyJobStatusChanged(UUID jobMasterLeaderSessionID,
		ExecutionGraphMessages.JobStatusChanged jobStatusChanged)
	{
		if (jobMasterLeaderSessionID.equals(this.jobMasterLeaderSessionID)) {
			logAndPrintMessage(jobStatusChanged);
		}
	}

	/**
	 * Receives notification about job result from {@link org.apache.flink.runtime.jobmaster.JobMaster}
	 *
	 * @param jobMasterLeaderSessionID leaderSessionID of jobMaster which send this notifcation
	 * @param jobResultMessage         job result
	 */
	@RpcMethod
	public void notifyJobResult(UUID jobMasterLeaderSessionID, JobManagerMessages.JobResultMessage jobResultMessage) {
		if (jobMasterLeaderSessionID.equals(this.jobMasterLeaderSessionID)) {
			jobResultMessagePromise.success(jobResultMessage);
			shutDown();
		}
	}

	/**
	 * Gets jobID
	 *
	 * @return jobID
	 */
	public JobID getJobID() {
		return this.jobID;
	}

	/**
	 * Gets the final job execution result wrapped in Future
	 *
	 * @param classLoader the classloader to parse JobResultMessage
	 * @return job execution result wrapped in Future
	 */
	public Future<JobExecutionResult> getJobExecutionResult(final ClassLoader classLoader) {
		scala.concurrent.Future<JobExecutionResult> jobResultMessageFuture = jobResultMessagePromise.future().map(new Mapper<JobManagerMessages.JobResultMessage, JobExecutionResult>() {

			@Override
			public JobExecutionResult checkedApply(
				final JobManagerMessages.JobResultMessage jobResultMessage) throws JobExecutionException
			{
				if (jobResultMessage instanceof JobManagerMessages.JobResultSuccess) {
					log.info("Job execution complete");
					SerializedJobExecutionResult result = ((JobManagerMessages.JobResultSuccess) jobResultMessage).result();
					if (result != null) {
						try {
							return result.toJobExecutionResult(classLoader);
						} catch (Throwable t) {
							throw new JobExecutionException(jobID,
								"Job was successfully executed but JobExecutionResult could not be deserialized.");
						}
					} else {
						throw new JobExecutionException(jobID,
							"Job was successfully executed but result contained a null JobExecutionResult.");
					}
				} else if (jobResultMessage instanceof JobManagerMessages.JobResultFailure) {
					log.info("Job execution failed");
					SerializedThrowable serThrowable = ((JobManagerMessages.JobResultFailure) jobResultMessage).cause();
					if (serThrowable != null) {
						Throwable cause = serThrowable.deserializeError(classLoader);
						if (cause instanceof JobExecutionException) {
							throw (JobExecutionException) cause;
						} else {
							throw new JobExecutionException(jobID, "Job execution failed", cause);
						}
					} else {
						throw new JobExecutionException(jobID,
							"Job execution failed with null as failure cause.");
					}
				} else {
					throw new JobExecutionException(jobID,
						"Unknown answer from JobManager after submitting the job: " + jobResultMessage);
				}
			}
		}, ExecutionContexts$.MODULE$.fromExecutor(getRpcService().getExecutor()));

		return new FlinkFuture<>(jobResultMessageFuture);
	}

	private void handleFatalError(final Throwable e) {
		runAsync(new Runnable() {
			@Override
			public void run() {
				log.error("Error occurred.", e);
				if (!jobResultMessagePromise.isCompleted()) {
					jobResultMessagePromise.tryFailure(e);
				}
				shutDown();
			}
		});
	}

	private void logAndPrintMessage(String message) {
		log.info(message);
		if (sysoutUpdates) {
			System.out.println(message);
		}
	}

	private void logAndPrintMessage(ExecutionGraphMessages.JobStatusChanged message) {
		// by default, this only prints the status, and not any exception.
		// in state FAILING, we report the exception in addition
		if (message.newJobStatus() != JobStatus.FAILING || message.error() == null) {
			logAndPrintMessage(message.toString());
		} else {
			log.info(message.toString(), message.error());
			if (sysoutUpdates) {
				System.out.println(message.toString());
				message.error().printStackTrace(System.out);
			}
		}
	}

	private class JobMasterLeaderListener implements LeaderRetrievalListener {

		@Override
		public void notifyLeaderAddress(final String leaderAddress, final UUID leaderSessionID) {
			jobMasterLeaderSessionID = leaderSessionID;
		}

		@Override
		public void handleError(final Exception exception) {
			log.error("Error occurred in the LeaderRetrievalService.", exception);
			handleFatalError(exception);
		}
	}
}
