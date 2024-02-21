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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/** State which describes a finished job execution. */
class Finished implements State {

    private final ArchivedExecutionGraph archivedExecutionGraph;

    private final Logger logger;

    Finished(Context context, ArchivedExecutionGraph archivedExecutionGraph, Logger logger) {
        this.archivedExecutionGraph = archivedExecutionGraph;
        this.logger = logger;

        context.onFinished(archivedExecutionGraph);
    }

    @Override
    public void cancel() {}

    @Override
    public void suspend(Throwable cause) {}

    @Override
    public JobStatus getJobStatus() {
        return archivedExecutionGraph.getState();
    }

    @Override
    public ArchivedExecutionGraph getJob() {
        return archivedExecutionGraph;
    }

    @Override
    public void handleGlobalFailure(
            Throwable cause, CompletableFuture<Map<String, String>> failureLabels) {
        logger.debug(
                "Ignore global failure because we already finished the job {}.",
                archivedExecutionGraph.getJobID(),
                cause);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    /** Context of the {@link Finished} state. */
    interface Context {

        /**
         * Callback which is called when the execution reaches the {@link Finished} state.
         *
         * @param archivedExecutionGraph archivedExecutionGraph represents the final state of the
         *     job execution
         */
        void onFinished(ArchivedExecutionGraph archivedExecutionGraph);
    }

    static class Factory implements StateFactory<Finished> {

        private final Context context;
        private final Logger log;
        private final ArchivedExecutionGraph archivedExecutionGraph;

        public Factory(Context context, ArchivedExecutionGraph archivedExecutionGraph, Logger log) {
            this.context = context;
            this.log = log;
            this.archivedExecutionGraph = archivedExecutionGraph;
        }

        public Class<Finished> getStateClass() {
            return Finished.class;
        }

        public Finished getState() {
            return new Finished(context, archivedExecutionGraph, log);
        }
    }
}
