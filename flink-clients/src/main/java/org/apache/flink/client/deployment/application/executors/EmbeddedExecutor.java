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
import org.apache.flink.client.deployment.executors.ExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link PipelineExecutor} that invokes directly methods of the
 * {@link org.apache.flink.runtime.dispatcher.DispatcherGateway Dispatcher} does
 * not go through the REST API.
 */
@Internal
public class EmbeddedExecutor implements PipelineExecutor {

	private static final Logger LOG = LoggerFactory.getLogger(EmbeddedExecutor.class);

	public static final String NAME = "embedded";

	private final Collection<JobID> applicationJobIds;

	private final DispatcherGateway dispatcherGateway;

	/**
	 * Creates an {@link EmbeddedExecutor}.
	 * @param submittedJobIds a list that going to be filled with the job ids of the
	 *                        new jobs that will be submitted. This is essentially used to return the submitted job ids
	 *                        to the caller.
	 * @param dispatcherGateway the dispatcher of the cluster which is going to be used to submit jobs.
	 */
	public EmbeddedExecutor(
			final Collection<JobID> submittedJobIds,
			final DispatcherGateway dispatcherGateway) {
		this.applicationJobIds = checkNotNull(submittedJobIds);
		this.dispatcherGateway = checkNotNull(dispatcherGateway);
	}

	@Override
	public CompletableFuture<JobClient> execute(final Pipeline pipeline, final Configuration configuration) {
		checkNotNull(pipeline);
		checkNotNull(configuration);

		final JobGraph jobGraph = ExecutorUtils.getJobGraph(pipeline, configuration);
		final JobID actualJobId = jobGraph.getJobID();

		this.applicationJobIds.add(actualJobId);
		LOG.info("Job {} is submitted.", actualJobId);

		final EmbeddedJobClient embeddedClient = new EmbeddedJobClient(
				actualJobId,
				dispatcherGateway,
				Time.milliseconds(configuration.getLong(WebOptions.TIMEOUT)));

		return embeddedClient
				.submitJob(blobServerAddress -> new BlobClient(blobServerAddress, configuration), jobGraph)
				.thenApplyAsync(jobID -> embeddedClient);
	}
}
