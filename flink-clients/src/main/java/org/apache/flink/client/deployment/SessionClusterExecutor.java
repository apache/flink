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
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.Executor;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.JobGraph;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An abstract {@link Executor} used to execute {@link Pipeline pipelines} on an existing (session) cluster.
 *
 * @param <ClusterID> the type of the id of the cluster.
 */
@Internal
public final class SessionClusterExecutor<ClusterID> implements Executor {

	private final ClusterClientProvider<ClusterID> clusterClientProvider;

	public SessionClusterExecutor(final ClusterClientProvider<ClusterID> clusterClientProvider) {
		this.clusterClientProvider = checkNotNull(clusterClientProvider);
	}

	@Override
	public CompletableFuture<JobClient> execute(final Pipeline pipeline, final Configuration configuration) throws Exception {
		final JobGraph jobGraph = ExecutorUtils.getJobGraph(pipeline, configuration);

		final ClusterClient<ClusterID> clusterClient = clusterClientProvider.getClusterClient();

		return clusterClient
				.submitJob(jobGraph)
				.thenApplyAsync(jobID -> (JobClient) new ClusterClientJobClientAdapter<>(
						clusterClientProvider,
						jobID))
				.whenComplete((ignored1, ignored2) -> clusterClient.close());
	}
}
