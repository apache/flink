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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.runtime.messages.webmonitor.ClusterOverview;
import org.apache.flink.runtime.messages.webmonitor.JobIdsWithStatusOverview;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.messages.webmonitor.RequestJobDetails;
import org.apache.flink.runtime.messages.webmonitor.RequestJobsWithIDsOverview;
import org.apache.flink.runtime.messages.webmonitor.RequestStatusOverview;
import org.apache.flink.runtime.metrics.dump.MetricQueryService;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

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
							throw new CompletionException(new FlinkException("JobManager responded for wrong Job. This Job: " +
								jobGraph.getJobID() + ", response: " + success.jobId()));
						}
					} else if (response instanceof JobManagerMessages.JobResultFailure) {
						JobManagerMessages.JobResultFailure failure = ((JobManagerMessages.JobResultFailure) response);

						throw new CompletionException(new FlinkException("Job submission failed.", failure.cause()));
					} else {
						throw new CompletionException(new FlinkException("Unknown response to SubmitJob message: " + response + '.'));
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
					throw new CompletionException(new FlinkException("Cancel with savepoint failed.", ((JobManagerMessages.CancellationFailure) response).cause()));
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
					throw new CompletionException(new FlinkException("Cancel job failed " + jobId + '.', ((JobManagerMessages.CancellationFailure) response).cause()));
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
					throw new CompletionException(new FlinkException("Stop job failed " + jobId + '.', ((JobManagerMessages.StoppingFailure) response).cause()));
				}
			});
	}

	//--------------------------------------------------------------------------------
	// JobManager information
	//--------------------------------------------------------------------------------

	@Override
	public CompletableFuture<Optional<Instance>> requestTaskManagerInstance(ResourceID resourceId, Time timeout) {
		return FutureUtils.toJava(
			jobManagerGateway
				.ask(new JobManagerMessages.RequestTaskManagerInstance(resourceId), FutureUtils.toFiniteDuration(timeout))
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
						throw new CompletionException(new FlinkException("Unknown response: " + response + '.'));
					}
				});
	}

	@Override
	public CompletableFuture<MultipleJobsDetails> requestMultipleJobDetails(Time timeout) {
		return FutureUtils.toJava(
			jobManagerGateway
				.ask(new RequestJobDetails(true, true), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.apply(MultipleJobsDetails.class)));
	}

	@Override
	public CompletableFuture<AccessExecutionGraph> requestJob(JobID jobId, Time timeout) {
		CompletableFuture<JobManagerMessages.JobResponse> jobResponseFuture = FutureUtils.toJava(
			jobManagerGateway
				.ask(new JobManagerMessages.RequestJob(jobId), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.apply(JobManagerMessages.JobResponse.class)));

		return jobResponseFuture.thenApply(
			(JobManagerMessages.JobResponse jobResponse) -> {
				if (jobResponse instanceof JobManagerMessages.JobFound) {
					return ((JobManagerMessages.JobFound) jobResponse).executionGraph();
				} else {
					throw new CompletionException(new FlinkJobNotFoundException(jobId));
				}
			});
	}

	@Override
	public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
		return requestJob(jobId, timeout).thenApply(JobResult::createFrom);
	}

	@Override
	public CompletableFuture<ClusterOverview> requestClusterOverview(Time timeout) {
		return FutureUtils.toJava(
			jobManagerGateway
				.ask(RequestStatusOverview.getInstance(), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.apply(ClusterOverview.class)));
	}

	@Override
	public CompletableFuture<Collection<String>> requestMetricQueryServicePaths(Time timeout) {
		final String jobManagerPath = getAddress();
		final String jobManagerMetricQueryServicePath = jobManagerPath.substring(0, jobManagerPath.lastIndexOf('/') + 1) + MetricQueryService.METRIC_QUERY_SERVICE_NAME;

		return CompletableFuture.completedFuture(
			Collections.singleton(jobManagerMetricQueryServicePath));
	}

	@Override
	public CompletableFuture<Collection<Tuple2<ResourceID, String>>> requestTaskManagerMetricQueryServicePaths(Time timeout) {
		return requestTaskManagerInstances(timeout)
			.thenApply(
				(Collection<Instance> instances) ->
					instances
						.stream()
						.map(
							(Instance instance) -> {
								final String taskManagerAddress = instance.getTaskManagerGateway().getAddress();
								final String taskManagerMetricQuerServicePath = taskManagerAddress.substring(0, taskManagerAddress.lastIndexOf('/') + 1) +
									MetricQueryService.METRIC_QUERY_SERVICE_NAME + '_' + instance.getTaskManagerID().getResourceIdString();

								return Tuple2.of(instance.getTaskManagerID(), taskManagerMetricQuerServicePath);
							})
						.collect(Collectors.toList()));
	}

	@Override
	public CompletableFuture<JobIdsWithStatusOverview> requestJobsOverview(Time timeout) {
		return FutureUtils.toJava(
			jobManagerGateway
				.ask(RequestJobsWithIDsOverview.getInstance(), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.apply(JobIdsWithStatusOverview.class)));
	}

	@Override
	public CompletableFuture<String> requestRestAddress(Time timeout) {
		return FutureUtils.toJava(
			jobManagerGateway
				.ask(JobManagerMessages.getRequestRestAddress(), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.apply(String.class)));
	}
}
