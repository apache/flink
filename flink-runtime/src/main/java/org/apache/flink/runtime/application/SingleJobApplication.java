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

package org.apache.flink.runtime.application;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** An implementation of {@link AbstractApplication} designed for executing a single job. */
public class SingleJobApplication extends AbstractApplication implements JobStatusListener {

    private static final Logger LOG = LoggerFactory.getLogger(SingleJobApplication.class);

    private final ExecutionPlan executionPlan;

    private final Duration rpcTimeout;

    private final boolean isRecovered;

    private transient DispatcherGateway dispatcherGateway;

    private transient Executor mainThreadExecutor;

    private transient FatalErrorHandler errorHandler;

    public SingleJobApplication(ExecutionPlan executionPlan) {
        this(executionPlan, false);
    }

    public SingleJobApplication(ExecutionPlan executionPlan, boolean isRecovered) {
        super(ApplicationID.fromHexString(executionPlan.getJobID().toHexString()));
        executionPlan.setApplicationId(getApplicationId());
        this.executionPlan = executionPlan;
        this.isRecovered = isRecovered;
        this.rpcTimeout = executionPlan.getJobConfiguration().get(RpcOptions.ASK_TIMEOUT_DURATION);
    }

    @Override
    public CompletableFuture<Acknowledge> execute(
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final Executor mainThreadExecutor,
            final FatalErrorHandler errorHandler) {
        transitionToRunning();

        this.dispatcherGateway = dispatcherGateway;
        this.mainThreadExecutor = mainThreadExecutor;
        this.errorHandler = errorHandler;

        if (isRecovered) {
            LOG.info("Recovering application {} ({})", getName(), getApplicationId());
            // skip job submission during application recovery as the Dispatcher will handle job
            // recovery
            return CompletableFuture.completedFuture(Acknowledge.get());
        }

        return dispatcherGateway
                .submitJob(executionPlan, rpcTimeout)
                .handle(
                        (ack, t) -> {
                            if (t != null) {
                                LOG.error("Job submission failed.", t);
                                transitionToFailing();
                                transitionToFailed();
                                throw new CompletionException(t);
                            }
                            return ack;
                        });
    }

    @Override
    public void cancel() {
        ApplicationState currentState = getApplicationStatus();
        if (currentState == ApplicationState.CREATED) {
            // nothing to cancel
            transitionToCanceling();
            transitionToCanceled();
        } else if (currentState == ApplicationState.RUNNING) {
            transitionToCanceling();
            cancelJob();
        }
    }

    @Override
    public void dispose() {}

    @Override
    public String getName() {
        return "SingleJobApplication(" + executionPlan.getName() + ")";
    }

    @Override
    public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp) {
        checkState(jobId.equals(executionPlan.getJobID()), "Job ID mismatch");
        if (newJobStatus.isGloballyTerminalState()) {
            checkNotNull(mainThreadExecutor);

            mainThreadExecutor.execute(
                    () -> {
                        LOG.info("Application completed with job status {}", newJobStatus);
                        if (newJobStatus == JobStatus.FINISHED) {
                            transitionToFinished();
                        } else if (newJobStatus == JobStatus.CANCELED) {
                            if (getApplicationStatus() != ApplicationState.CANCELING) {
                                transitionToCanceling();
                            }
                            transitionToCanceled();
                        } else {
                            if (getApplicationStatus() == ApplicationState.RUNNING) {
                                transitionToFailing();
                                transitionToFailed();
                            }
                        }
                    });
        }
    }

    @VisibleForTesting
    public ExecutionPlan getExecutionPlan() {
        return executionPlan;
    }

    private void cancelJob() {
        checkNotNull(dispatcherGateway);
        checkNotNull(errorHandler);

        final JobID jobId = executionPlan.getJobID();
        LOG.info("Canceling job {} for application {}", jobId, getName());
        dispatcherGateway
                .cancelJob(jobId, rpcTimeout)
                .exceptionally(
                        t -> {
                            LOG.warn("Failed to cancel job.", t);
                            errorHandler.onFatalError(t);
                            return null;
                        });
    }
}
