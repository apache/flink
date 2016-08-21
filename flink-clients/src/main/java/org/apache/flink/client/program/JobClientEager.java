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

import org.apache.flink.api.common.JobClient;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.client.JobClientActorUtils;
import org.apache.flink.runtime.client.JobClientActor;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobListeningContext;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.accumulators.AccumulatorResultsErroneous;
import org.apache.flink.runtime.messages.accumulators.AccumulatorResultsFound;
import org.apache.flink.runtime.messages.accumulators.RequestAccumulatorResults;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * A client to interact with a running Flink job.
 */
public class JobClientEager implements JobClient {

	private final Logger LOG = LoggerFactory.getLogger(getClass());

	/** The Job's listening context for monitoring and job interaction */
	private final JobListeningContext jobListeningContext;

	/** Finalization code to run upon shutting down the JobClient */
	private final List<Runnable> finalizers;

	public JobClientEager(JobListeningContext jobListeningContext) {
		this.jobListeningContext = jobListeningContext;
		this.finalizers = new LinkedList<>();
	}

	/**
	 * Blocks until the job finishes and returns the {@link JobExecutionResult}
	 * @return the result of the job execution
	 */
	@Override
	public JobExecutionResult waitForResult() throws JobExecutionException {
		LOG.info("Waiting for results of Job {}", jobListeningContext.getJobID());
		JobExecutionResult result = JobClientActorUtils.awaitJobResult(jobListeningContext);
		shutdown();
		return result;
	}

	/**
	 * Gets the job id that this client is bound to
	 * @return The JobID of this JobClient
	 */
	public JobID getJobID() {
		return jobListeningContext.getJobID();
	}

	@Override
	public boolean hasFinished() {
		return jobListeningContext.getJobResultFuture().isCompleted();
	}

	/**
	 * Cancels a job identified by the job id.
	 * @throws Exception In case an error occurred.
	 */
	@Override
	public void cancel() throws Exception {
		final ActorGateway jobClient = jobListeningContext.getJobClientGateway();

		final Future<Object> response;
		try {
			response = jobClient.ask(
				new JobClientActor.ClientMessage(
					new JobManagerMessages.CancelJob(getJobID())),
				AkkaUtils.getDefaultTimeoutAsFiniteDuration());
		} catch (final Exception e) {
			throw new ProgramInvocationException("Failed to query the job manager gateway.", e);
		}

		final Object result = Await.result(response, AkkaUtils.getDefaultTimeoutAsFiniteDuration());

		if (result instanceof JobManagerMessages.CancellationSuccess) {
			LOG.info("Job cancellation with ID " + getJobID() + " succeeded.");
		} else if (result instanceof JobManagerMessages.CancellationFailure) {
			final Throwable t = ((JobManagerMessages.CancellationFailure) result).cause();
			LOG.info("Job cancellation with ID " + getJobID() + " failed.", t);
			throw new Exception("Failed to cancel the job because of \n" + t.getMessage());
		} else {
			throw new Exception("Unknown message received while cancelling: " + result);
		}
	}

	/**
	 * Stops a program on Flink cluster whose job-manager is configured in this client's configuration.
	 * Stopping works only for streaming programs. Be aware, that the program might continue to run for
	 * a while after sending the stop command, because after sources stopped to emit data all operators
	 * need to finish processing.
	 *
	 * @throws Exception
	 *             If the job ID is invalid (ie, is unknown or refers to a batch job) or if sending the stop signal
	 *             failed. That might be due to an I/O problem, ie, the job-manager is unreachable.
	 */
	@Override
	public void stop() throws Exception {
		final ActorGateway jobManagerGateway = jobListeningContext.getJobManager();

		final Future<Object> response;
		try {
			response = jobManagerGateway.ask(
				new JobClientActor.ClientMessage(
					new JobManagerMessages.StopJob(getJobID())),
				AkkaUtils.getDefaultTimeoutAsFiniteDuration());
		} catch (final Exception e) {
			throw new ProgramInvocationException("Failed to query the job manager gateway.", e);
		}

		final Object result = Await.result(response, AkkaUtils.getDefaultTimeoutAsFiniteDuration());

		if (result instanceof JobManagerMessages.StoppingSuccess) {
			LOG.info("Job stopping with ID " + getJobID() + " succeeded.");
		} else if (result instanceof JobManagerMessages.StoppingFailure) {
			final Throwable t = ((JobManagerMessages.StoppingFailure) result).cause();
			LOG.info("Job stopping with ID " + getJobID() + " failed.", t);
			throw new Exception("Failed to stop the job because of \n" + t.getMessage());
		} else {
			throw new Exception("Unknown message received while stopping: " + result);
		}
	}


	/**
	 * Requests and returns the accumulators for the given job identifier. Accumulators can be
	 * requested while a job is running or after it has finished.
	 * @return A Map containing the accumulator's name and its value.
	 */
	@Override
	public Map<String, Object> getAccumulators() throws Exception {
		ActorGateway jobManagerGateway = jobListeningContext.getJobManager();

		Future<Object> response;
		try {
			response = jobManagerGateway.ask(
				new JobClientActor.ClientMessage(
					new RequestAccumulatorResults(getJobID())),
				AkkaUtils.getDefaultTimeoutAsFiniteDuration());
		} catch (Exception e) {
			throw new Exception("Failed to query the job manager gateway for accumulators.", e);
		}

		Object result = Await.result(response, AkkaUtils.getDefaultTimeoutAsFiniteDuration());

		if (result instanceof AccumulatorResultsFound) {
			Map<String, SerializedValue<Object>> serializedAccumulators =
				((AccumulatorResultsFound) result).result();

			return AccumulatorHelper.deserializeAccumulators(
				serializedAccumulators, jobListeningContext.getClassLoader());

		} else if (result instanceof AccumulatorResultsErroneous) {
			throw ((AccumulatorResultsErroneous) result).cause();
		} else {
			throw new Exception("Failed to fetch accumulators for the job " + getJobID());
		}
	}


	@Override
	public void addFinalizer(Runnable finalizer) {
		finalizers.add(finalizer);
	}

	/**
	 * Shuts down the JobClient and executes any finalizers.
	 */
	@Override
	public void shutdown() {
		synchronized (this) {
			for (Runnable finalizer : finalizers) {
				finalizer.run();
			}
			finalizers.clear();
		}
	}

	/**
	 * Perform cleanup on garbage collection.
	 */
	@Override
	protected void finalize() throws Throwable {
		shutdown();
		super.finalize();
	}
}
