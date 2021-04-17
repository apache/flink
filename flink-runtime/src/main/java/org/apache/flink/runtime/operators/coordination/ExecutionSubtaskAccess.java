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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.util.IncompleteFuturesTracker;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link SubtaskAccess} interface that uses the ExecutionGraph's classes,
 * specifically {@link Execution} and {@link ExecutionJobVertex} to access tasks.
 */
final class ExecutionSubtaskAccess implements SubtaskAccess {

    private final Execution taskExecution;
    private final OperatorID operator;
    private final IncompleteFuturesTracker futuresTracker;

    ExecutionSubtaskAccess(Execution taskExecution, OperatorID operator) {
        this.taskExecution = taskExecution;
        this.operator = operator;
        this.futuresTracker = new IncompleteFuturesTracker();

        // this is a safeguard to speed up things: as soon as the task is in a terminal state, all
        // the pending futures from events sent to that task should fail immediately
        // without this, the futures would only fail after the RPC system hits the ask-timeout.
        taskExecution
                .getTerminalStateFuture()
                .thenAccept(
                        (state) ->
                                futuresTracker.failAllFutures(
                                        new FlinkException("Task is no longer running")));
    }

    @Override
    public Callable<CompletableFuture<Acknowledge>> createEventSendAction(
            SerializedValue<OperatorEvent> event) {
        return () -> {
            final CompletableFuture<Acknowledge> result =
                    taskExecution.sendOperatorEvent(operator, event);
            futuresTracker.trackFutureWhileIncomplete(result);
            return result;
        };
    }

    @Override
    public int getSubtaskIndex() {
        return taskExecution.getParallelSubtaskIndex();
    }

    @Override
    public ExecutionAttemptID currentAttempt() {
        return taskExecution.getAttemptId();
    }

    @Override
    public String subtaskName() {
        return taskExecution.getVertexWithAttempt();
    }

    @Override
    public CompletableFuture<?> hasSwitchedToRunning() {
        return taskExecution.getInitializingOrRunningFuture();
    }

    @Override
    public boolean isStillRunning() {
        return taskExecution.getState() == ExecutionState.RUNNING
                || taskExecution.getState() == ExecutionState.INITIALIZING;
    }

    @Override
    public void triggerTaskFailover(Throwable cause) {
        taskExecution.fail(cause);
    }

    // ------------------------------------------------------------------------

    static final class ExecutionJobVertexSubtaskAccess implements SubtaskAccessFactory {

        private final ExecutionJobVertex ejv;
        private final OperatorID operator;

        ExecutionJobVertexSubtaskAccess(ExecutionJobVertex ejv, OperatorID operator) {
            this.ejv = checkNotNull(ejv);
            this.operator = checkNotNull(operator);
        }

        @Override
        public SubtaskAccess getAccessForSubtask(int subtask) {
            if (subtask < 0 || subtask >= ejv.getParallelism()) {
                throw new IllegalArgumentException(
                        "Subtask index out of bounds [0, " + ejv.getParallelism() + ')');
            }

            final Execution taskExecution =
                    ejv.getTaskVertices()[subtask].getCurrentExecutionAttempt();
            return new ExecutionSubtaskAccess(taskExecution, operator);
        }
    }
}
