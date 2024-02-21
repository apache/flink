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

import org.slf4j.Logger;

/** Initial state of the {@link AdaptiveScheduler}. */
class Created extends StateWithoutExecutionGraph {

    private final Context context;

    Created(Context context, Logger logger) {
        super(context, logger);
        this.context = context;
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.INITIALIZING;
    }

    /** Starts the scheduling by going into the {@link WaitingForResources} state. */
    void startScheduling() {
        context.goToWaitingForResources(null);
    }

    /** Context of the {@link Created} state. */
    interface Context
            extends StateWithoutExecutionGraph.Context, StateTransitions.ToWaitingForResources {}

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
