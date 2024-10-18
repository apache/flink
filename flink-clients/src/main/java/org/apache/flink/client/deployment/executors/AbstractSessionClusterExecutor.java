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

package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.CacheSupportedPipelineExecutor;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerUtils;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An abstract {@link PipelineExecutor} used to execute {@link Pipeline pipelines} on an existing
 * (session) cluster.
 *
 * @param <ClusterID> the type of the id of the cluster.
 * @param <ClientFactory> the type of the {@link ClusterClientFactory} used to create/retrieve a
 *     client to the target cluster.
 */
@Internal
public class AbstractSessionClusterExecutor<
                ClusterID, ClientFactory extends ClusterClientFactory<ClusterID>>
        implements CacheSupportedPipelineExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractSessionClusterExecutor.class);
    private final ExecutorService executorService =
            Executors.newFixedThreadPool(
                    1, new ExecutorThreadFactory("Flink-SessionClusterExecutor-IO"));

    private final ClientFactory clusterClientFactory;
    private final Configuration configuration;
    private final List<JobStatusChangedListener> jobStatusChangedListeners;

    public AbstractSessionClusterExecutor(
            @Nonnull final ClientFactory clusterClientFactory, Configuration configuration) {
        this.clusterClientFactory = checkNotNull(clusterClientFactory);
        this.configuration = configuration;
        this.jobStatusChangedListeners =
                JobStatusChangedListenerUtils.createJobStatusChangedListeners(
                        Thread.currentThread().getContextClassLoader(),
                        configuration,
                        executorService);
    }

    @Override
    public CompletableFuture<JobClient> execute(
            @Nonnull final Pipeline pipeline,
            @Nonnull final Configuration configuration,
            @Nonnull final ClassLoader userCodeClassloader)
            throws Exception {
        StreamGraph streamGraph = PipelineExecutorUtils.getStreamGraph(pipeline, configuration);

        try (final ClusterDescriptor<ClusterID> clusterDescriptor =
                clusterClientFactory.createClusterDescriptor(configuration)) {
            final ClusterID clusterID = clusterClientFactory.getClusterId(configuration);
            checkState(clusterID != null);

            final ClusterClientProvider<ClusterID> clusterClientProvider =
                    clusterDescriptor.retrieve(clusterID);
            ClusterClient<ClusterID> clusterClient = clusterClientProvider.getClusterClient();

            streamGraph.serializeUserDefinedInstances();
            return clusterClient
                    .submitJob(streamGraph)
                    .thenApplyAsync(
                            FunctionUtils.uncheckedFunction(
                                    jobId -> {
                                        ClientUtils.waitUntilJobInitializationFinished(
                                                () -> clusterClient.getJobStatus(jobId).get(),
                                                () -> clusterClient.requestJobResult(jobId).get(),
                                                userCodeClassloader);
                                        return jobId;
                                    }))
                    .thenApplyAsync(
                            jobID ->
                                    (JobClient)
                                            new ClusterClientJobClientAdapter<>(
                                                    clusterClientProvider,
                                                    jobID,
                                                    userCodeClassloader))
                    .whenCompleteAsync(
                            (jobClient, throwable) -> {
                                if (throwable == null) {
                                    PipelineExecutorUtils.notifyJobStatusListeners(
                                            pipeline, streamGraph, jobStatusChangedListeners);
                                } else {
                                    LOG.error(
                                            "Failed to submit job graph to remote session cluster.",
                                            throwable);
                                }
                                clusterClient.close();
                            });
        }
    }

    @Override
    public CompletableFuture<Set<AbstractID>> listCompletedClusterDatasetIds(
            Configuration configuration, ClassLoader userCodeClassloader) throws Exception {

        try (final ClusterDescriptor<ClusterID> clusterDescriptor =
                clusterClientFactory.createClusterDescriptor(configuration)) {
            final ClusterID clusterID = clusterClientFactory.getClusterId(configuration);
            checkState(clusterID != null);

            final ClusterClientProvider<ClusterID> clusterClientProvider =
                    clusterDescriptor.retrieve(clusterID);

            final ClusterClient<ClusterID> clusterClient = clusterClientProvider.getClusterClient();
            return clusterClient.listCompletedClusterDatasetIds();
        }
    }

    @Override
    public CompletableFuture<Void> invalidateClusterDataset(
            AbstractID clusterDatasetId,
            Configuration configuration,
            ClassLoader userCodeClassloader)
            throws Exception {
        try (final ClusterDescriptor<ClusterID> clusterDescriptor =
                clusterClientFactory.createClusterDescriptor(configuration)) {
            final ClusterID clusterID = clusterClientFactory.getClusterId(configuration);
            checkState(clusterID != null);

            final ClusterClientProvider<ClusterID> clusterClientProvider =
                    clusterDescriptor.retrieve(clusterID);

            final ClusterClient<ClusterID> clusterClient = clusterClientProvider.getClusterClient();
            return clusterClient
                    .invalidateClusterDataset(new IntermediateDataSetID(clusterDatasetId))
                    .thenApply(acknowledge -> null);
        }
    }
}
