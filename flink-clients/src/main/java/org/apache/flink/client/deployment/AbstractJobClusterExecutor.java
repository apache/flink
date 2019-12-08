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
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.Executor;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract {@link Executor} used to execute {@link Pipeline pipelines} on dedicated (per-job) clusters.
 *
 * @param <ClusterID> the type of the id of the cluster.
 * @param <ClientFactory> the type of the {@link ClusterClientFactory} used to create/retrieve a client to the target cluster.
 */
@Internal
public class AbstractJobClusterExecutor<ClusterID, ClientFactory extends ClusterClientFactory<ClusterID>> implements Executor {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractJobClusterExecutor.class);

	private final ClientFactory clusterClientFactory;

	public AbstractJobClusterExecutor(@Nonnull final ClientFactory clusterClientFactory) {
		this.clusterClientFactory = checkNotNull(clusterClientFactory);
	}

	@Override
	public CompletableFuture<JobClient> execute(@Nonnull final Pipeline pipeline, @Nonnull final Configuration configuration) throws Exception {
		final JobGraph jobGraph = ExecutorUtils.getJobGraph(pipeline, configuration);

		try (final ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(configuration)) {
			final ExecutionConfigAccessor configAccessor = ExecutionConfigAccessor.fromConfiguration(configuration);

			final ClusterSpecification clusterSpecification = clusterClientFactory.getClusterSpecification(configuration);

			final ClusterClientProvider<ClusterID> clusterClientProvider = clusterDescriptor
					.deployJobCluster(clusterSpecification, jobGraph, configAccessor.getDetachedMode());
			LOG.info("Job has been submitted with JobID " + jobGraph.getJobID());

			final boolean withShutdownHook = !configAccessor.getDetachedMode() && configAccessor.isShutdownOnAttachedExit();

			if (withShutdownHook) {
				ShutdownHookUtil.addShutdownHook(
						() -> {
							try (ClusterClient<ClusterID> client = clusterClientProvider.getClusterClient()) {
								client.shutDownCluster();
							}
						},
						"Cluster shutdown hook for attached Job execution",
						LOG);

				return CompletableFuture.completedFuture(
						new ClusterClientJobClientAdapter<ClusterID>(clusterClientProvider, jobGraph.getJobID()) {
				});
			} else {
				return CompletableFuture.completedFuture(
						new ClusterClientJobClientAdapter<>(clusterClientProvider, jobGraph.getJobID()));
			}
		}
	}
}
