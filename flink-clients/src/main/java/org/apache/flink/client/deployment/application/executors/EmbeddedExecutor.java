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
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerUtils;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.client.ClientUtils;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A base class for {@link PipelineExecutor executors} that invoke directly methods of the {@link
 * org.apache.flink.runtime.dispatcher.DispatcherGateway Dispatcher} and do not go through the REST
 * API.
 */
@Internal
public class EmbeddedExecutor implements PipelineExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedExecutor.class);

    private final ExecutorService executorService =
            Executors.newFixedThreadPool(
                    1, new ExecutorThreadFactory("Flink-EmbeddedClusterExecutor-IO"));

    public static final String NAME = "embedded";

    private final Collection<JobID> submittedJobIds;

    private final DispatcherGateway dispatcherGateway;

    private final EmbeddedJobClientCreator jobClientCreator;

    private final List<JobStatusChangedListener> jobStatusChangedListeners;

    /**
     * Creates a {@link EmbeddedExecutor}.
     *
     * @param submittedJobIds a list that is going to be filled with the job ids of the new jobs
     *     that will be submitted. This is essentially used to return the submitted job ids to the
     *     caller.
     * @param dispatcherGateway the dispatcher of the cluster which is going to be used to submit
     *     jobs.
     * @param configuration the flink application configuration
     * @param jobClientCreator the job client creator
     */
    public EmbeddedExecutor(
            final Collection<JobID> submittedJobIds,
            final DispatcherGateway dispatcherGateway,
            final Configuration configuration,
            final EmbeddedJobClientCreator jobClientCreator) {
        this.submittedJobIds = checkNotNull(submittedJobIds);
        this.dispatcherGateway = checkNotNull(dispatcherGateway);
        this.jobClientCreator = checkNotNull(jobClientCreator);
        this.jobStatusChangedListeners =
                JobStatusChangedListenerUtils.createJobStatusChangedListeners(
                        Thread.currentThread().getContextClassLoader(),
                        configuration,
                        executorService);
    }

    @Override
    public CompletableFuture<JobClient> execute(
            final Pipeline pipeline,
            final Configuration configuration,
            ClassLoader userCodeClassloader)
            throws Exception {
        checkNotNull(pipeline);
        checkNotNull(configuration);

        final Optional<JobID> optJobId =
                configuration
                        .getOptional(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID)
                        .map(JobID::fromHexString);

        if (optJobId.isPresent() && submittedJobIds.contains(optJobId.get())) {
            return getJobClientFuture(optJobId.get(), userCodeClassloader);
        }

        return submitAndGetJobClientFuture(pipeline, configuration, userCodeClassloader);
    }

    private CompletableFuture<JobClient> getJobClientFuture(
            final JobID jobId, final ClassLoader userCodeClassloader) {
        LOG.info("Job {} was recovered successfully.", jobId);
        return CompletableFuture.completedFuture(
                jobClientCreator.getJobClient(jobId, userCodeClassloader));
    }

    private CompletableFuture<JobClient> submitAndGetJobClientFuture(
            final Pipeline pipeline,
            final Configuration configuration,
            final ClassLoader userCodeClassloader)
            throws Exception {
        final Duration timeout = configuration.get(ClientOptions.CLIENT_TIMEOUT);

        final StreamGraph streamGraph =
                PipelineExecutorUtils.getStreamGraph(pipeline, configuration);
        final JobID actualJobId = streamGraph.getJobID();

        this.submittedJobIds.add(actualJobId);
        LOG.info("Job {} is submitted.", actualJobId);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Effective Configuration: {}", configuration);
        }

        final CompletableFuture<JobID> jobSubmissionFuture =
                submitJob(configuration, dispatcherGateway, streamGraph, timeout);

        return jobSubmissionFuture
                .thenApplyAsync(
                        FunctionUtils.uncheckedFunction(
                                jobId -> {
                                    org.apache.flink.client.ClientUtils
                                            .waitUntilJobInitializationFinished(
                                                    () ->
                                                            dispatcherGateway
                                                                    .requestJobStatus(
                                                                            jobId, timeout)
                                                                    .get(),
                                                    () ->
                                                            dispatcherGateway
                                                                    .requestJobResult(
                                                                            jobId, timeout)
                                                                    .get(),
                                                    userCodeClassloader);
                                    return jobId;
                                }))
                .thenApplyAsync(
                        jobID -> jobClientCreator.getJobClient(actualJobId, userCodeClassloader))
                .whenCompleteAsync(
                        (jobClient, throwable) -> {
                            if (throwable == null) {
                                PipelineExecutorUtils.notifyJobStatusListeners(
                                        pipeline, streamGraph, jobStatusChangedListeners);
                            } else {
                                LOG.error(
                                        "Failed to submit job graph to application cluster",
                                        throwable);
                            }
                        });
    }

    private static CompletableFuture<JobID> submitJob(
            final Configuration configuration,
            final DispatcherGateway dispatcherGateway,
            final StreamGraph streamGraph,
            final Duration rpcTimeout) {
        checkNotNull(streamGraph);

        LOG.info("Submitting Job with JobId={}.", streamGraph.getJobID());

        return dispatcherGateway
                .getBlobServerPort(rpcTimeout)
                .thenApply(
                        blobServerPort ->
                                new InetSocketAddress(
                                        dispatcherGateway.getHostname(), blobServerPort))
                .thenCompose(
                        blobServerAddress -> {
                            try {
                                ClientUtils.extractAndUploadExecutionPlanFiles(
                                        streamGraph,
                                        () -> new BlobClient(blobServerAddress, configuration));
                                streamGraph.serializeUserDefinedInstances();
                            } catch (Exception e) {
                                throw new CompletionException(e);
                            }

                            return dispatcherGateway.submitJob(streamGraph, rpcTimeout);
                        })
                .thenApply(ack -> streamGraph.getJobID());
    }
}
