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
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.FunctionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

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

    private final Collection<JobID> applicationJobIds;

    private final Collection<JobID> suspendedJobIds;

    private final Collection<JobID> terminalJobIds;

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
        this(
                submittedJobIds,
                Collections.emptyList(),
                Collections.emptyList(),
                dispatcherGateway,
                configuration,
                jobClientCreator);
    }

    /**
     * Creates a {@link EmbeddedExecutor}.
     *
     * @param applicationJobIds a list that is going to be filled by the {@link EmbeddedExecutor}
     *     with job ids of all jobs that are part of the current application execution. This is
     *     essentially used to return the job ids to the caller.
     * @param suspendedJobIds ids of jobs that are suspended from a previous application execution,
     *     which are not supposed to be modified by the {@link EmbeddedExecutor}.
     * @param terminalJobIds ids of jobs that are already in a terminal state in a previous
     *     application execution, which are not supposed to be modified by the {@link
     *     EmbeddedExecutor}.
     * @param dispatcherGateway the dispatcher of the cluster which is going to be used to submit
     *     jobs.
     * @param configuration the flink application configuration
     * @param jobClientCreator the job client creator
     */
    public EmbeddedExecutor(
            final Collection<JobID> applicationJobIds,
            final Collection<JobID> suspendedJobIds,
            final Collection<JobID> terminalJobIds,
            final DispatcherGateway dispatcherGateway,
            final Configuration configuration,
            final EmbeddedJobClientCreator jobClientCreator) {
        this.applicationJobIds = checkNotNull(applicationJobIds);
        this.suspendedJobIds = checkNotNull(suspendedJobIds);
        this.terminalJobIds = checkNotNull(terminalJobIds);
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
        checkState(pipeline instanceof StreamGraph);

        StreamGraph streamGraph = (StreamGraph) pipeline;

        final Optional<JobID> optJobId =
                configuration
                        .getOptional(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID)
                        .map(JobID::fromHexString);

        // Skip resubmission if the job is recovered via HA.
        // When optJobId is present, the streamGraph's ID is deterministically derived from it. In
        // this case, if the streamGraph's ID is in terminalJobIds or submittedJobIds, it means the
        // job was submitted in a previous run and should not be resubmitted.
        if (optJobId.isPresent()) {
            final JobID actualJobId = streamGraph.getJobID();
            if (terminalJobIds.contains(actualJobId)) {
                LOG.info("Job {} reached a terminal state in a previous execution.", actualJobId);
                return addJobAndGetJobClientFuture(actualJobId, userCodeClassloader);
            }

            if (suspendedJobIds.contains(actualJobId)) {
                final Duration timeout = configuration.get(ClientOptions.CLIENT_TIMEOUT);
                return dispatcherGateway
                        .recoverJob(actualJobId, timeout)
                        .thenCompose(
                                ack -> {
                                    LOG.info("Job {} is recovered successfully.", actualJobId);
                                    return addJobAndGetJobClientFuture(
                                            actualJobId, userCodeClassloader);
                                });
            }
        }

        return submitAndGetJobClientFuture(pipeline, configuration, userCodeClassloader);
    }

    private CompletableFuture<JobClient> addJobAndGetJobClientFuture(
            final JobID jobId, final ClassLoader userCodeClassloader) {
        applicationJobIds.add(jobId);
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

        this.applicationJobIds.add(actualJobId);
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

    private CompletableFuture<JobID> submitJob(
            final Configuration configuration,
            final DispatcherGateway dispatcherGateway,
            final StreamGraph streamGraph,
            final Duration rpcTimeout) {
        checkNotNull(streamGraph);

        LOG.info("Submitting Job with JobId={}.", streamGraph.getJobID());

        return dispatcherGateway
                .getBlobServerPort(rpcTimeout)
                .thenCombine(
                        dispatcherGateway.getBlobServerAddress(rpcTimeout),
                        (blobServerPort, blobServerAddress) ->
                                new InetSocketAddress(
                                        blobServerAddress.getHostName(), blobServerPort))
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

                            return internalSubmit(dispatcherGateway, streamGraph, rpcTimeout);
                        })
                .thenApply(ack -> streamGraph.getJobID());
    }

    CompletableFuture<Acknowledge> internalSubmit(
            final DispatcherGateway dispatcherGateway,
            final StreamGraph streamGraph,
            final Duration rpcTimeout) {
        return dispatcherGateway.submitJob(streamGraph, rpcTimeout);
    }
}
