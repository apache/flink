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

package org.apache.flink.client.deployment.application.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.deployment.application.EmbeddedJobClient;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.client.ClientUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.FunctionWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link PipelineExecutor} that invokes directly methods of the
 * {@link org.apache.flink.runtime.dispatcher.DispatcherGateway Dispatcher} and does
 * not go through the REST API.
 */
@Internal
public class EmbeddedExecutor implements PipelineExecutor {

	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedExecutor.class);

	public static final String NAME = "embedded";

	private final Collection<JobID> submittedJobIds;

	private final DispatcherGateway dispatcherGateway;

	/**
	 * Creates an {@link EmbeddedExecutor}.
	 * @param submittedJobIds a list that is going to be filled with the job ids of the
	 *                        new jobs that will be submitted. This is essentially used to return the submitted job ids
	 *                        to the caller.
	 * @param dispatcherGateway the dispatcher of the cluster which is going to be used to submit jobs.
	 */
	public EmbeddedExecutor(
			final Collection<JobID> submittedJobIds,
			final DispatcherGateway dispatcherGateway) {
		this.submittedJobIds = checkNotNull(submittedJobIds);
		this.dispatcherGateway = checkNotNull(dispatcherGateway);
	}

	@Override
	public CompletableFuture<JobClient> execute(final Pipeline pipeline, final Configuration configuration) {
		checkNotNull(pipeline);
		checkNotNull(configuration);

		final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);
		final JobID actualJobId = jobGraph.getJobID();

		this.submittedJobIds.add(actualJobId);
		LOG.info("Job {} is submitted.", actualJobId);

		final Time timeout = Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT));

		final CompletableFuture<JobID> jobSubmissionFuture = submitJob(
				dispatcherGateway,
				blobServerAddress -> new BlobClient(blobServerAddress, configuration),
				jobGraph,
				timeout);

		final EmbeddedJobClient embeddedClient = new EmbeddedJobClient(
				actualJobId,
				dispatcherGateway,
				timeout);

		return jobSubmissionFuture
				.thenApplyAsync(jobID -> embeddedClient);
	}

	private static CompletableFuture<JobID> submitJob(
			final DispatcherGateway dispatcherGateway,
			final FunctionWithException<InetSocketAddress, BlobClient, IOException> blobClientCreator,
			final JobGraph jobGraph,
			final Time rpcTimeout) {
		checkNotNull(blobClientCreator);
		checkNotNull(jobGraph);

		LOG.info("Submitting Job with JobId={}.", jobGraph.getJobID());

		return dispatcherGateway
				.getBlobServerPort(rpcTimeout)
				.thenApply(blobServerPort -> new InetSocketAddress(dispatcherGateway.getHostname(), blobServerPort))
				.thenCompose(blobServerAddress -> {

					try {
						ClientUtils.extractAndUploadJobGraphFiles(jobGraph, () -> blobClientCreator.apply(blobServerAddress));
					} catch (FlinkException e) {
						throw new CompletionException(e);
					}

					return dispatcherGateway.submitJob(jobGraph, rpcTimeout);
				}).thenApply(ack -> jobGraph.getJobID());
	}
}
