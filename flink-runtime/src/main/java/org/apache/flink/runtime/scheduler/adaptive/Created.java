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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;

import org.slf4j.Logger;

import javax.annotation.Nullable;

/** Initial state of the {@link AdaptiveScheduler}. */
class Created implements State {

    private final Context context;

    private final Logger logger;

    Created(Context context, Logger logger) {
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
    public JobStatus getJobStatus() {
        return JobStatus.INITIALIZING;
    }

    @Override
    public ArchivedExecutionGraph getJob() {
        return context.getArchivedExecutionGraph(getJobStatus(), null);
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        context.goToFinished(context.getArchivedExecutionGraph(JobStatus.FAILED, cause));
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    /** Starts the scheduling by going into the {@link WaitingForResources} state. */
    void startScheduling() {
        context.goToWaitingForResources();
    }

    /** Context of the {@link Created} state. */
    interface Context {

        /**
         * Transitions into the {@link Finished} state.
         *
         * @param archivedExecutionGraph archivedExecutionGraph is passed to the {@link Finished}
         *     state
         */
        void goToFinished(ArchivedExecutionGraph archivedExecutionGraph);

        /**
         * Creates an {@link ArchivedExecutionGraph} for the given jobStatus and failure cause.
         *
         * @param jobStatus jobStatus to create the {@link ArchivedExecutionGraph} with
         * @param cause cause represents the failure cause for the {@link ArchivedExecutionGraph};
         *     {@code null} if there is no failure cause
         * @return the created {@link ArchivedExecutionGraph}
         */
        ArchivedExecutionGraph getArchivedExecutionGraph(
                JobStatus jobStatus, @Nullable Throwable cause);

        /** Transitions into the {@link WaitingForResources} state. */
        void goToWaitingForResources();
    }

    static class Factory implements StateFactory<Created> {

        private final Context context;
        private final Logger log;

        public Factory(Context context, Logger log) {
            this.context = context;
            this.log = log;
        }

        public Class<Created> getStateClass() {
            return Created.class;
        }

        public Created getState() {
            return new Created(this.context, this.log);
        }
    }
}
