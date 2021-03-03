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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;

import org.slf4j.Logger;

/** State which describes a job which is currently being canceled. */
class Canceling extends StateWithExecutionGraph {

    private final Context context;

    Canceling(
            Context context,
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            Logger logger) {
        super(context, executionGraph, executionGraphHandler, operatorCoordinatorHandler, logger);
        this.context = context;

        getExecutionGraph().cancel();
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.CANCELLING;
    }

    @Override
    public void cancel() {
        // we are already in the state canceling
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        // ignore global failures
    }

    @Override
    boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionStateTransition) {
        return getExecutionGraph().updateState(taskExecutionStateTransition);
    }

    @Override
    void onGloballyTerminalState(JobStatus globallyTerminalState) {
        context.goToFinished(ArchivedExecutionGraph.createFrom(getExecutionGraph()));
    }

    static class Factory implements StateFactory<Canceling> {

        private final Context context;
        private final Logger log;
        private final ExecutionGraph executionGraph;
        private final ExecutionGraphHandler executionGraphHandler;
        private final OperatorCoordinatorHandler operatorCoordinatorHandler;

        public Factory(
                Context context,
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Logger log) {
            this.context = context;
            this.log = log;
            this.executionGraph = executionGraph;
            this.executionGraphHandler = executionGraphHandler;
            this.operatorCoordinatorHandler = operatorCoordinatorHandler;
        }

        public Class<Canceling> getStateClass() {
            return Canceling.class;
        }

        public Canceling getState() {
            return new Canceling(
                    context,
                    executionGraph,
                    executionGraphHandler,
                    operatorCoordinatorHandler,
                    log);
        }
    }
}
