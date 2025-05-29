/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Abstract state class which contains its {@link Context} and {@link #logger} to execute common
 * operations.
 */
abstract class StateWithoutExecutionGraph implements State {

    private final Context context;

    private final Logger logger;

    StateWithoutExecutionGraph(Context context, Logger logger) {
        this.context = context;
        this.logger = logger;
    }

    @Override
    public void cancel() {
        context.goToFinished(context.getArchivedExecutionGraph(JobStatus.CANCELED, null));
    }

    @Override
    public void suspend(Throwable cause) {
        context.goToFinished(context.getArchivedExecutionGraph(JobStatus.SUSPENDED, cause));
    }

    @Override
    public JobID getJobId() {
        return context.getJobId();
    }

    @Override
    public ArchivedExecutionGraph getJob() {
        return context.getArchivedExecutionGraph(getJobStatus(), null);
    }

    @Override
    public void handleGlobalFailure(
            Throwable cause, CompletableFuture<Map<String, String>> failureLabels) {
        context.goToFinished(context.getArchivedExecutionGraph(JobStatus.FAILED, cause));
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    /** Context of the {@link StateWithoutExecutionGraph} state. */
    interface Context extends StateTransitions.ToFinished {

        /**
         * Gets the {@link JobID} of the job.
         *
         * @return the {@link JobID} of the job
         */
        JobID getJobId();

        /**
         * Creates the {@link ArchivedExecutionGraph} for the given job status and cause. Cause can
         * be null if there is no failure.
         *
         * @param jobStatus jobStatus to initialize the {@link ArchivedExecutionGraph} with
         * @param cause cause describing a failure cause; {@code null} if there is none
         * @return the created {@link ArchivedExecutionGraph}
         */
        ArchivedExecutionGraph getArchivedExecutionGraph(
                JobStatus jobStatus, @Nullable Throwable cause);
    }
}
