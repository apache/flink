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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.types.Either;
import org.apache.flink.util.SerializedValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Customized {@link TaskManagerActions} that wait for ExecutionState changes. */
public class TestTaskManagerActions implements TaskManagerActions {

    private final JobMasterGateway jobMasterGateway;
    private final TaskSlotTable<Task> taskSlotTable;
    private final TaskManagerActionListeners taskManagerActionListeners =
            new TaskManagerActionListeners();

    private final ScheduledExecutorService scheduledExecutor = Executors.newScheduledThreadPool(2);
    private boolean hasPendingSync = false;
    private final List<TaskExecutionState> pendingStates = new ArrayList<>();

    public TestTaskManagerActions(
            TaskSlotTable<Task> taskSlotTable, JobMasterGateway jobMasterGateway) {
        this.taskSlotTable = taskSlotTable;
        this.jobMasterGateway = jobMasterGateway;
    }

    public void addListener(
            ExecutionAttemptID eid, ExecutionState executionState, CompletableFuture<Void> future) {
        taskManagerActionListeners.addListener(eid, executionState, future);
    }

    @Override
    public void notifyFatalError(String message, Throwable cause) {}

    @Override
    public void failTask(ExecutionAttemptID executionAttemptID, Throwable cause) {
        if (taskSlotTable != null) {
            taskSlotTable.getTask(executionAttemptID).failExternally(cause);
        }
    }

    @Override
    public synchronized void updateTaskExecutionState(TaskExecutionState taskExecutionState) {
        Optional<CompletableFuture<Void>> listenerFuture =
                taskManagerActionListeners.getListenerFuture(
                        taskExecutionState.getID(), taskExecutionState.getExecutionState());
        if (listenerFuture.isPresent()) {
            listenerFuture.get().complete(null);
        }

        if (!hasPendingSync) {
            scheduledExecutor.schedule(this::syncMessagesWithJobMaster, 20, TimeUnit.MILLISECONDS);
            hasPendingSync = true;
        }
        pendingStates.add(taskExecutionState);
    }

    private synchronized void syncMessagesWithJobMaster() {
        List<TaskExecutionState> currentStates;
        currentStates = new ArrayList<>(pendingStates);
        pendingStates.clear();

        final List<ExecutionAttemptID> executionAttemptIDs =
                currentStates.stream().map(TaskExecutionState::getID).collect(Collectors.toList());

        if (jobMasterGateway != null) {
            CompletableFuture<List<Either<Acknowledge, Throwable>>> futureAcknowledge =
                    jobMasterGateway.updateTaskExecutionStates(currentStates);

            futureAcknowledge.whenCompleteAsync(
                    (acks, throwable) -> {
                        if (throwable != null) {
                            for (ExecutionAttemptID executionAttemptID : executionAttemptIDs) {
                                failTask(executionAttemptID, throwable);
                            }
                        } else {
                            for (int i = 0; i < executionAttemptIDs.size(); ++i) {
                                final ExecutionAttemptID executionAttemptID =
                                        executionAttemptIDs.get(i);
                                if (acks.get(i).isRight()) {
                                    failTask(executionAttemptID, acks.get(i).right());
                                }
                            }
                        }
                    });
        }

        hasPendingSync = false;
    }

    @Override
    public CompletableFuture<Acknowledge> sendOperatorEventToCoordinator(
            ExecutionAttemptID task, OperatorID operatorID, SerializedValue<OperatorEvent> event) {
        throw new UnsupportedOperationException();
    }

    @Override
    public CompletableFuture<CoordinationResponse> sendRequestToCoordinator(
            OperatorID operatorID, SerializedValue<CoordinationRequest> request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void notifyEndOfData(ExecutionAttemptID executionAttemptID) {}

    private static class TaskManagerActionListeners {
        private final Map<ExecutionAttemptID, List<Tuple2<ExecutionState, CompletableFuture<Void>>>>
                expectExecutionStateAndFutures;

        private TaskManagerActionListeners() {
            this.expectExecutionStateAndFutures = new HashMap<>();
        }

        private void addListener(
                ExecutionAttemptID eid,
                ExecutionState executionState,
                CompletableFuture<Void> future) {
            final List<Tuple2<ExecutionState, CompletableFuture<Void>>>
                    expectExecutionStateAndFutureList =
                            expectExecutionStateAndFutures.getOrDefault(eid, new ArrayList<>());
            expectExecutionStateAndFutureList.add(Tuple2.of(executionState, future));
            expectExecutionStateAndFutures.put(eid, expectExecutionStateAndFutureList);
        }

        private Optional<CompletableFuture<Void>> getListenerFuture(
                ExecutionAttemptID eid, ExecutionState executionState) {
            List<Tuple2<ExecutionState, CompletableFuture<Void>>> expectStateAndFutureList =
                    expectExecutionStateAndFutures.get(eid);
            if (expectExecutionStateAndFutures == null) {
                return Optional.empty();
            }
            for (Tuple2<ExecutionState, CompletableFuture<Void>> expectStateAndFuture :
                    expectStateAndFutureList) {
                if (expectStateAndFuture.f0 == executionState) {
                    return Optional.of(expectStateAndFuture.f1);
                }
            }
            return Optional.empty();
        }
    }
}
