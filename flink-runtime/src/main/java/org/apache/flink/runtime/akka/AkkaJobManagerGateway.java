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

package org.apache.flink.runtime.akka;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FlinkFutureException;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.webmonitor.JobsWithIDsOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.messages.webmonitor.RequestJobDetails;
import org.apache.flink.runtime.messages.webmonitor.RequestJobsWithIDsOverview;
import org.apache.flink.runtime.messages.webmonitor.RequestStatusOverview;
import org.apache.flink.runtime.messages.webmonitor.StatusOverview;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import scala.Option;
import scala.reflect.ClassTag$;

/**
 * Implementation of the {@link JobManagerGateway} for old JobManager code based
 * on Akka actors and the {@link ActorGateway}.
 */
public class AkkaJobManagerGateway implements JobManagerGateway {

	private final ActorGateway jobManagerGateway;
	private final String hostname;

	public AkkaJobManagerGateway(ActorGateway jobManagerGateway) {
		this.jobManagerGateway = Preconditions.checkNotNull(jobManagerGateway);

		final Option<String> optHostname = jobManagerGateway.actor().path().address().host();
		hostname = optHostname.isDefined() ? optHostname.get() : "localhost";
	}

	@Override
	public String getAddress() {
		return jobManagerGateway.path();
	}

	@Override
	public String getHostname() {
		return hostname;
	}

	@Override
	public CompletableFuture<Integer> requestBlobServerPort(Time timeout) {
		return FutureUtils.toJava(
			jobManagerGateway
				.ask(JobManagerMessages.getRequestBlobManagerPort(), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.apply(Integer.class)));
	}

	@Override
	public CompletableFuture<Integer> requestWebPort(Time timeout) {
		CompletableFuture<JobManagerMessages.ResponseWebMonitorPort> portResponseFuture = FutureUtils.toJava(
			jobManagerGateway
				.ask(JobManagerMessages.getRequestWebMonitorPort(), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.apply(JobManagerMessages.ResponseWebMonitorPort.class)));

		return portResponseFuture.thenApply(
			JobManagerMessages.ResponseWebMonitorPort::port);
	}

	//--------------------------------------------------------------------------------
	// Job control
	//--------------------------------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, ListeningBehaviour listeningBehaviour, Time timeout) {
		return FutureUtils
			.toJava(
				jobManagerGateway.ask(
					new JobManagerMessages.SubmitJob(
						jobGraph,
						listeningBehaviour),
					FutureUtils.toFiniteDuration(timeout)))
			.thenApply(
				(Object response) -> {
					if (response instanceof JobManagerMessages.JobSubmitSuccess) {
						JobManagerMessages.JobSubmitSuccess success = ((JobManagerMessages.JobSubmitSuccess) response);

						if (Objects.equals(success.jobId(), jobGraph.getJobID())) {
							return Acknowledge.get();
						} else {
							throw new FlinkFutureException("JobManager responded for wrong Job. This Job: " +
								jobGraph.getJobID() + ", response: " + success.jobId());
						}
					} else if (response instanceof JobManagerMessages.JobResultFailure) {
						JobManagerMessages.JobResultFailure failure = ((JobManagerMessages.JobResultFailure) response);

						throw new FlinkFutureException("Job submission failed.", failure.cause());
					} else {
						throw new FlinkFutureException("Unknown response to SubmitJob message: " + response + '.');
					}
				}
			);
	}

	@Override
	public CompletableFuture<String> cancelJobWithSavepoint(JobID jobId, String savepointPath, Time timeout) {
		CompletableFuture<JobManagerMessages.CancellationResponse> cancellationFuture = FutureUtils.toJava(
			jobManagerGateway
				.ask(new JobManagerMessages.CancelJobWithSavepoint(jobId, savepointPath), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.apply(JobManagerMessages.CancellationResponse.class)));

		return cancellationFuture.thenApply(
			(JobManagerMessages.CancellationResponse response) -> {
				if (response instanceof JobManagerMessages.CancellationSuccess) {
					return ((JobManagerMessages.CancellationSuccess) response).savepointPath();
				} else {
					throw new FlinkFutureException("Cancel with savepoint failed.", ((JobManagerMessages.CancellationFailure) response).cause());
				}
			});
	}

	@Override
	public CompletableFuture<Acknowledge> cancelJob(JobID jobId, Time timeout) {
		CompletableFuture<JobManagerMessages.CancellationResponse> responseFuture = FutureUtils.toJava(
			jobManagerGateway
				.ask(new JobManagerMessages.CancelJob(jobId), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.apply(JobManagerMessages.CancellationResponse.class)));

		return responseFuture.thenApply(
			(JobManagerMessages.CancellationResponse response) -> {
				if (response instanceof JobManagerMessages.CancellationSuccess) {
					return Acknowledge.get();
				} else {
					throw new FlinkFutureException("Cancel job failed " + jobId + '.', ((JobManagerMessages.CancellationFailure) response).cause());
				}
			});
	}

	@Override
	public CompletableFuture<Acknowledge> stopJob(JobID jobId, Time timeout) {
		CompletableFuture<JobManagerMessages.StoppingResponse> responseFuture = FutureUtils.toJava(
			jobManagerGateway
				.ask(new JobManagerMessages.StopJob(jobId), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.apply(JobManagerMessages.StoppingResponse.class)));

		return responseFuture.thenApply(
			(JobManagerMessages.StoppingResponse response) -> {
				if (response instanceof JobManagerMessages.StoppingSuccess) {
					return Acknowledge.get();
				} else {
					throw new FlinkFutureException("Stop job failed " + jobId + '.', ((JobManagerMessages.StoppingFailure) response).cause());
				}
			});
	}

	//--------------------------------------------------------------------------------
	// JobManager information
	//--------------------------------------------------------------------------------

	@Override
	public CompletableFuture<Optional<Instance>> requestTaskManagerInstance(InstanceID instanceId, Time timeout) {
		return FutureUtils.toJava(
			jobManagerGateway
				.ask(new JobManagerMessages.RequestTaskManagerInstance(instanceId), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.<JobManagerMessages.TaskManagerInstance>apply(JobManagerMessages.TaskManagerInstance.class)))
			.thenApply(
				(JobManagerMessages.TaskManagerInstance taskManagerResponse) -> {
					if (taskManagerResponse.instance().isDefined()) {
						return Optional.of(taskManagerResponse.instance().get());
					} else {
						return Optional.empty();
					}
				});
	}

	@Override
	public CompletableFuture<Collection<Instance>> requestTaskManagerInstances(Time timeout) {
		CompletableFuture<JobManagerMessages.RegisteredTaskManagers> taskManagersFuture = FutureUtils.toJava(
			jobManagerGateway
				.ask(JobManagerMessages.getRequestRegisteredTaskManagers(), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.apply(JobManagerMessages.RegisteredTaskManagers.class)));

		return taskManagersFuture.thenApply(
			JobManagerMessages.RegisteredTaskManagers::asJavaCollection);
	}

	@Override
	public CompletableFuture<Optional<JobManagerMessages.ClassloadingProps>> requestClassloadingProps(JobID jobId, Time timeout) {
		return FutureUtils
			.toJava(jobManagerGateway
				.ask(
					new JobManagerMessages.RequestClassloadingProps(jobId),
					FutureUtils.toFiniteDuration(timeout)))
			.thenApply(
				(Object response) -> {
					if (response instanceof JobManagerMessages.ClassloadingProps) {
						return Optional.of(((JobManagerMessages.ClassloadingProps) response));
					} else if (response instanceof JobManagerMessages.JobNotFound) {
						return Optional.empty();
					} else {
						throw new FlinkFutureException("Unknown response: " + response + '.');
					}
				});
	}

	@Override
	public CompletableFuture<MultipleJobsDetails> requestJobDetails(boolean includeRunning, boolean includeFinished, Time timeout) {
		return FutureUtils.toJava(
			jobManagerGateway
				.ask(new RequestJobDetails(true, true), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.apply(MultipleJobsDetails.class)));
	}

	@Override
	public CompletableFuture<Optional<AccessExecutionGraph>> requestJob(JobID jobId, Time timeout) {
		CompletableFuture<JobManagerMessages.JobResponse> jobResponseFuture = FutureUtils.toJava(
			jobManagerGateway
				.ask(new JobManagerMessages.RequestJob(jobId), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.apply(JobManagerMessages.JobResponse.class)));

		return jobResponseFuture.thenApply(
			(JobManagerMessages.JobResponse jobResponse) -> {
				if (jobResponse instanceof JobManagerMessages.JobFound) {
					return Optional.of(((JobManagerMessages.JobFound) jobResponse).executionGraph());
				} else {
					return Optional.empty();
				}
			});
	}

	@Override
	public CompletableFuture<StatusOverview> requestStatusOverview(Time timeout) {
		return FutureUtils.toJava(
			jobManagerGateway
				.ask(RequestStatusOverview.getInstance(), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.apply(StatusOverview.class)));
	}

	@Override
	public CompletableFuture<JobsWithIDsOverview> requestJobsOverview(Time timeout) {
		return FutureUtils.toJava(
			jobManagerGateway
				.ask(RequestJobsWithIDsOverview.getInstance(), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.apply(JobsWithIDsOverview.class)));
	}
}
