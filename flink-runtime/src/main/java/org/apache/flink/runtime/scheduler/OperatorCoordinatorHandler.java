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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.TaskNotRunningException;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/** Handler for the {@link OperatorCoordinator OperatorCoordinators}. */
public class OperatorCoordinatorHandler {
    private final ExecutionGraph executionGraph;

    private final Map<OperatorID, OperatorCoordinatorHolder> coordinatorMap;

    private final Consumer<Throwable> globalFailureHandler;

    public OperatorCoordinatorHandler(
            ExecutionGraph executionGraph, Consumer<Throwable> globalFailureHandler) {
        this.executionGraph = executionGraph;

        this.coordinatorMap = createCoordinatorMap(executionGraph);
        this.globalFailureHandler = globalFailureHandler;
    }

    private Map<OperatorID, OperatorCoordinatorHolder> createCoordinatorMap(
            ExecutionGraph executionGraph) {
        Map<OperatorID, OperatorCoordinatorHolder> coordinatorMap = new HashMap<>();
        for (ExecutionJobVertex vertex : executionGraph.getAllVertices().values()) {
            for (OperatorCoordinatorHolder holder : vertex.getOperatorCoordinators()) {
                coordinatorMap.put(holder.operatorId(), holder);
            }
        }
        return coordinatorMap;
    }

    public void initializeOperatorCoordinators(ComponentMainThreadExecutor mainThreadExecutor) {
        for (OperatorCoordinatorHolder coordinatorHolder : coordinatorMap.values()) {
            coordinatorHolder.lazyInitialize(globalFailureHandler, mainThreadExecutor);
        }
    }

    public void startAllOperatorCoordinators() {
        final Collection<OperatorCoordinatorHolder> coordinators = coordinatorMap.values();
        try {
            for (OperatorCoordinatorHolder coordinator : coordinators) {
                coordinator.start();
            }
        } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
            coordinators.forEach(IOUtils::closeQuietly);
            throw new FlinkRuntimeException("Failed to start the operator coordinators", t);
        }
    }

    public void disposeAllOperatorCoordinators() {
        coordinatorMap.values().forEach(IOUtils::closeQuietly);
    }

    public void deliverOperatorEventToCoordinator(
            final ExecutionAttemptID taskExecutionId,
            final OperatorID operatorId,
            final OperatorEvent evt)
            throws FlinkException {

        // Failure semantics (as per the javadocs of the method):
        // If the task manager sends an event for a non-running task or an non-existing operator
        // coordinator, then respond with an exception to the call. If task and coordinator exist,
        // then we assume that the call from the TaskManager was valid, and any bubbling exception
        // needs to cause a job failure.

        final Execution exec = executionGraph.getRegisteredExecutions().get(taskExecutionId);
        if (exec == null || exec.getState() != ExecutionState.RUNNING) {
            // This situation is common when cancellation happens, or when the task failed while the
            // event was just being dispatched asynchronously on the TM side.
            // It should be fine in those expected situations to just ignore this event, but, to be
            // on the safe, we notify the TM that the event could not be delivered.
            throw new TaskNotRunningException(
                    "Task is not known or in state running on the JobManager.");
        }

        final OperatorCoordinatorHolder coordinator = coordinatorMap.get(operatorId);
        if (coordinator == null) {
            throw new FlinkException("No coordinator registered for operator " + operatorId);
        }

        try {
            coordinator.handleEventFromOperator(exec.getParallelSubtaskIndex(), evt);
        } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
            globalFailureHandler.accept(t);
        }
    }

    public CompletableFuture<CoordinationResponse> deliverCoordinationRequestToCoordinator(
            OperatorID operator, CoordinationRequest request) throws FlinkException {

        final OperatorCoordinatorHolder coordinatorHolder = coordinatorMap.get(operator);
        if (coordinatorHolder == null) {
            throw new FlinkException("Coordinator of operator " + operator + " does not exist");
        }

        final OperatorCoordinator coordinator = coordinatorHolder.coordinator();
        if (coordinator instanceof CoordinationRequestHandler) {
            return ((CoordinationRequestHandler) coordinator).handleCoordinationRequest(request);
        } else {
            throw new FlinkException(
                    "Coordinator of operator " + operator + " cannot handle client event");
        }
    }
}
