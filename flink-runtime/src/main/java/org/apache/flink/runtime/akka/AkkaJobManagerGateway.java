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
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.JobManagerMessages;
import org.apache.flink.util.Preconditions;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import scala.Option;
import scala.reflect.ClassTag$;

/**
 * Implementation of the {@link JobManagerGateway} for the {@link ActorGateway}.
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
	public CompletableFuture<Integer> requestBlobServerPort(Time timeout) {
		return FutureUtils.toJava(
			jobManagerGateway
				.ask(JobManagerMessages.getRequestBlobManagerPort(), FutureUtils.toFiniteDuration(timeout))
				.mapTo(ClassTag$.MODULE$.apply(Integer.class)));
	}

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
}
